package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/driver/node/mounter"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/driver/node/mounter/mountertest"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/mountpoint/mountoptions"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/util/testutil/assert"
)

// --- Fake ProcessRunner ---

type fakeProcessHandle struct {
	pid      int
	cmd      *exec.Cmd
	extraFds []uintptr // captured at Start time before close
	exitCode int
	stderr   []byte
	done     chan struct{}
	sigCh    chan os.Signal
}

func (h *fakeProcessHandle) Pid() int { return h.pid }

func (h *fakeProcessHandle) Wait() (int, []byte) {
	<-h.done
	return h.exitCode, h.stderr
}

func (h *fakeProcessHandle) Signal(sig os.Signal) error {
	h.sigCh <- sig
	return nil
}

// Exit makes Wait() return with the given code and stderr.
func (h *fakeProcessHandle) Exit(code int, stderr string) {
	h.exitCode = code
	h.stderr = []byte(stderr)
	close(h.done)
}

type fakeProcessRunner struct {
	mu       sync.Mutex
	nextPid  int
	handles  []*fakeProcessHandle
	startErr error // when set, Start fails instead of spawning
}

func (r *fakeProcessRunner) Start(cmd *exec.Cmd) (ProcessHandle, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.startErr != nil {
		return nil, r.startErr
	}
	r.nextPid++
	var extraFds []uintptr
	for _, f := range cmd.ExtraFiles {
		extraFds = append(extraFds, f.Fd())
	}
	h := &fakeProcessHandle{
		pid:      r.nextPid,
		cmd:      cmd,
		extraFds: extraFds,
		done:     make(chan struct{}),
		sigCh:    make(chan os.Signal, 1),
	}
	r.handles = append(r.handles, h)
	return h, nil
}

// lchownRecorder records the ownership the manager hands out, since tests are not root and cannot chown.
type lchownRecorder struct {
	mu    sync.Mutex
	calls []lchownCall
	err   error // when set, lchown fails instead of recording
}

type lchownCall struct {
	Path     string
	UID, GID int
}

func (r *lchownRecorder) lchown(path string, uid, gid int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.calls = append(r.calls, lchownCall{path, uid, gid})
	return nil
}

func (r *lchownRecorder) recorded() []lchownCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// newCachingProcessManager returns a manager whose container has a cache volume, and that volume.
func newCachingProcessManager(t *testing.T, fr *fakeProcessRunner, cache cacheLimit) (*ProcessManager, string, *lchownRecorder) {
	t.Helper()
	cacheDir := t.TempDir()
	rec := &lchownRecorder{}
	pm := NewProcessManager(t.TempDir(), cacheDir, fr, memoryLimit{strategy: memoryLimitNone}, cache)
	pm.lchown = rec.lchown
	return pm, cacheDir, rec
}

// --- Tests ---

func TestHandleConnection_PropagatesOptionsToRunner(t *testing.T) {
	commDir := t.TempDir()
	fr := &fakeProcessRunner{}
	pm := NewProcessManager(commDir, "", fr, memoryLimit{strategy: memoryLimitNone}, cacheLimit{strategy: cacheLimitNone})

	sockPath := filepath.Join(commDir, "test.sock")
	listener, err := net.Listen("unix", sockPath)
	assert.NoError(t, err)
	defer listener.Close()

	dev := mountertest.OpenDevNull(t)

	// Send options via UDS in background
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		mountoptions.Send(ctx, sockPath, mountoptions.Options{
			Fd:         int(dev.Fd()),
			BucketName: "test-bucket",
			Args:       []string{"--region", "us-west-2"},
			Env:        []string{"AWS_REGION=us-west-2"},
			VolumeId:   "pod123-vol456",
			Uid:        65536,
			Gid:        65536,
		})
	}()

	conn, err := listener.Accept()
	assert.NoError(t, err)

	handleConnection(conn.(*net.UnixConn), "/opt/mount-s3", pm, 5*time.Second)

	// Verify runner received the options
	fr.mu.Lock()
	assert.Equals(t, 1, len(fr.handles))
	fr.mu.Unlock()

	cmd := fr.handles[0].cmd
	assert.Equals(t, "/opt/mount-s3", cmd.Path)
	assert.Equals(t, "test-bucket", cmd.Args[1])
	assert.Equals(t, []string{"AWS_REGION=us-west-2"}, cmd.Env)
	assert.Equals(t, 1, len(fr.handles[0].extraFds)) // FD was passed

	// The credentials must reach the child, or it would run as the mounter's root with no isolation.
	cred := cmd.SysProcAttr.Credential
	assert.Equals(t, uint32(65536), cred.Uid)
	assert.Equals(t, uint32(65536), cred.Gid)
	assert.Equals(t, 0, len(cred.Groups)) // supplementary groups cleared

	// Cleanup
	fr.handles[0].Exit(0, "")
	pm.Shutdown()
}

func TestProcessManager_SecureCacheRoot(t *testing.T) {
	t.Run("locks the cache volume to root, reachable but unlistable", func(t *testing.T) {
		pm, cacheDir, rec := newCachingProcessManager(t, &fakeProcessRunner{}, cacheLimit{strategy: cacheLimitNone})
		// As the kubelet creates an emptyDir.
		assert.NoError(t, os.Chmod(cacheDir, 0777))

		assert.NoError(t, pm.secureCacheRoot())

		assert.Equals(t, []lchownCall{{cacheDir, 0, 0}}, rec.recorded())
		fi, err := os.Stat(cacheDir)
		assert.NoError(t, err)
		assert.Equals(t, fs.FileMode(0711), fi.Mode().Perm())
	})

	t.Run("does nothing when the container has no cache volume", func(t *testing.T) {
		rec := &lchownRecorder{}
		pm := NewProcessManager(t.TempDir(), "", &fakeProcessRunner{}, memoryLimit{strategy: memoryLimitNone}, cacheLimit{strategy: cacheLimitNone})
		pm.lchown = rec.lchown

		assert.NoError(t, pm.secureCacheRoot())
		assert.Equals(t, 0, len(rec.recorded()))
	})

	t.Run("fails when the cache volume cannot be chowned to root", func(t *testing.T) {
		pm, _, rec := newCachingProcessManager(t, &fakeProcessRunner{}, cacheLimit{strategy: cacheLimitNone})
		rec.err = syscall.EPERM

		if err := pm.secureCacheRoot(); err == nil {
			t.Fatal("expected secureCacheRoot to fail when the cache volume cannot be chowned")
		}
	})

	t.Run("fails when the cache volume cannot be chmodded", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "cache")
		pm := NewProcessManager(t.TempDir(), missing, &fakeProcessRunner{}, memoryLimit{strategy: memoryLimitNone}, cacheLimit{strategy: cacheLimitNone})
		pm.lchown = (&lchownRecorder{}).lchown

		if err := pm.secureCacheRoot(); err == nil {
			t.Fatal("expected secureCacheRoot to fail when the cache volume cannot be chmodded")
		}
	})
}

func TestProcessManager_Launch_HappyPath(t *testing.T) {
	commDir := t.TempDir()
	fr := &fakeProcessRunner{}
	pm := NewProcessManager(commDir, "", fr, memoryLimit{strategy: memoryLimitNone}, cacheLimit{strategy: cacheLimitNone})
	dev := mountertest.OpenDevNull(t)

	err := pm.Launch("mount-123", "/usr/bin/mount-s3", mountoptions.Options{
		Uid:        65536,
		Gid:        65536,
		Fd:         int(dev.Fd()),
		BucketName: "my-bucket",
		Env:        []string{"AWS_REGION=us-east-1"},
	})
	assert.NoError(t, err)

	// Verify cmd args
	cmd := fr.handles[0].cmd
	assert.Equals(t, []string{"/usr/bin/mount-s3", "my-bucket", "/dev/fd/3", "--foreground"}, cmd.Args)
	assert.Equals(t, []uintptr{dev.Fd()}, fr.handles[0].extraFds)
	assert.Equals(t, []string{"AWS_REGION=us-east-1"}, cmd.Env)

	// Verify tracked
	pm.mu.Lock()
	assert.Equals(t, 1, len(pm.processes))
	assert.Equals(t, fr.handles[0].Pid(), pm.processes["mount-123"].Pid())
	pm.mu.Unlock()

	// Clean exit
	fr.handles[0].Exit(0, "")
	pm.Shutdown()

	// Verify untracked, no error file
	pm.mu.Lock()
	assert.Equals(t, 0, len(pm.processes))
	pm.mu.Unlock()

	_, err = os.ReadFile(filepath.Join(commDir, "mount-123.error"))
	assert.Equals(t, true, os.IsNotExist(err))
}

func TestProcessManager_Launch_MemoryTarget(t *testing.T) {
	testCases := []struct {
		name     string
		limit    memoryLimit
		args     []string
		wantArgs []string
	}{
		{
			name:     "equalSplit injects the node's share",
			limit:    mustMemoryLimit(t, memoryLimitEqualSplit, 4*gib, 4), // (4096 - 64) / 4 = 1008
			wantArgs: []string{"--foreground", "--memory-target=1008"},
		},
		{
			name:     "equalSplit overrides --memory-target from PV mountOptions",
			limit:    mustMemoryLimit(t, memoryLimitEqualSplit, 4*gib, 4),
			args:     []string{"--memory-target=2048"},
			wantArgs: []string{"--foreground", "--memory-target=1008"},
		},
		{
			name:     "equalSplit leaves other mount options alone",
			limit:    mustMemoryLimit(t, memoryLimitEqualSplit, 4*gib, 4),
			args:     []string{"--prefix=data/"},
			wantArgs: []string{"--foreground", "--memory-target=1008", "--prefix=data/"},
		},
		{
			name:     "none leaves --memory-target unset",
			limit:    mustMemoryLimit(t, memoryLimitNone, 4*gib, 4),
			wantArgs: []string{"--foreground"},
		},
		{
			name:     "none passes --memory-target from PV mountOptions through",
			limit:    mustMemoryLimit(t, memoryLimitNone, 4*gib, 4),
			args:     []string{"--memory-target=2048"},
			wantArgs: []string{"--foreground", "--memory-target=2048"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fr := &fakeProcessRunner{}
			pm := NewProcessManager(t.TempDir(), "", fr, tc.limit, cacheLimit{strategy: cacheLimitNone})
			dev := mountertest.OpenDevNull(t)

			options := mountoptions.Options{
				Fd:         int(dev.Fd()),
				BucketName: "my-bucket",
				Args:       tc.args,
				Uid:        mounter.UIDRangeStart,
				Gid:        mounter.UIDRangeStart,
			}
			assert.NoError(t, pm.Launch("mount-123", "/usr/bin/mount-s3", options))

			// cmd.Args is [binary, bucket, /dev/fd/3, ...sorted args].
			assert.Equals(t, tc.wantArgs, fr.handles[0].cmd.Args[3:])

			// The wire slice is the driver's mount-sharing key and is persisted to meta, so a target
			// leaking into it would make running mounts look incompatible with themselves after a resize.
			assert.Equals(t, tc.args, options.Args)

			fr.handles[0].Exit(0, "")
			pm.Shutdown()
		})
	}
}

func TestProcessManager_Launch_TracksNothingWhenStartFails(t *testing.T) {
	fr := &fakeProcessRunner{startErr: errors.New("fork/exec: no such file")}
	pm := NewProcessManager(t.TempDir(), "", fr, mustMemoryLimit(t, memoryLimitEqualSplit, 4*gib, 4), cacheLimit{strategy: cacheLimitNone})
	dev := mountertest.OpenDevNull(t)

	err := pm.Launch("mount-doomed", "/usr/bin/mount-s3", mountoptions.Options{
		Fd:         int(dev.Fd()),
		BucketName: "bucket",
	})
	if err == nil {
		t.Fatal("expected Launch to fail when the process cannot start")
	}

	pm.mu.Lock()
	assert.Equals(t, 0, len(pm.processes))
	pm.mu.Unlock()
}

func TestProcessManager_Launch_MaxCacheSize(t *testing.T) {
	// What the driver sends; the mounter replaces it with its own directory for the mount.
	const cacheArg = "--cache=/cache/mount-123"

	testCases := []struct {
		name     string
		cache    cacheLimit
		args     []string
		wantArgs []string
	}{
		{
			name:     "equalSplit injects this node's share",
			cache:    mustCacheLimit(t, cacheLimitEqualSplit, 4*gib, 4),
			args:     []string{cacheArg},
			wantArgs: []string{"--foreground", "--max-cache-size=972"},
		},
		{
			name:     "equalSplit with no cache volume size injects 0, which serves the mount uncached",
			cache:    mustCacheLimit(t, cacheLimitEqualSplit, 0, 4),
			args:     []string{cacheArg},
			wantArgs: []string{"--foreground", "--max-cache-size=0"},
		},
		{
			name:     "none leaves the PV's size alone",
			cache:    cacheLimit{strategy: cacheLimitNone},
			args:     []string{cacheArg, "--max-cache-size=3000"},
			wantArgs: []string{"--foreground", "--max-cache-size=3000"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fr := &fakeProcessRunner{}
			pm, cacheDir, _ := newCachingProcessManager(t, fr, testCase.cache)
			dev := mountertest.OpenDevNull(t)

			options := mountoptions.Options{Uid: 65536, Gid: 65536, Fd: int(dev.Fd()), BucketName: "my-bucket", Args: testCase.args}
			assert.NoError(t, pm.Launch("mount-123", "/usr/bin/mount-s3", options))

			// cmd.Args is [binary, bucket, /dev/fd/3, ...sorted args].
			wantArgs := append([]string{"--cache=" + filepath.Join(cacheDir, "mount-123")}, testCase.wantArgs...)
			assert.Equals(t, wantArgs, fr.handles[0].cmd.Args[3:])
			// The wire slice is the driver's mount-sharing key and is persisted to its meta file, so a
			// share leaking into it would make a running mount look incompatible with itself on restart.
			assert.Equals(t, testCase.args, options.Args)

			fr.handles[0].Exit(0, "")
			pm.Shutdown()
		})
	}
}

func TestProcessManager_Launch_CacheDir(t *testing.T) {
	const mountId = "mount-123"

	cachedOptions := func(t *testing.T, args ...string) mountoptions.Options {
		dev := mountertest.OpenDevNull(t)
		return mountoptions.Options{Uid: 65536, Gid: 65536, Fd: int(dev.Fd()), BucketName: "my-bucket",
			Args: append([]string{"--cache=/cache/" + mountId}, args...)}
	}
	assertNotExist := func(t *testing.T, path string) {
		t.Helper()
		_, err := os.Lstat(path)
		assert.Equals(t, true, errors.Is(err, fs.ErrNotExist))
	}

	t.Run("creates the mount's directory, owned by its UID", func(t *testing.T) {
		fr := &fakeProcessRunner{}
		pm, cacheDir, rec := newCachingProcessManager(t, fr, cacheLimit{strategy: cacheLimitNone})
		mountCacheDir := filepath.Join(cacheDir, mountId)

		assert.NoError(t, pm.Launch(mountId, "/usr/bin/mount-s3", cachedOptions(t)))

		fi, err := os.Lstat(mountCacheDir)
		assert.NoError(t, err)
		assert.Equals(t, true, fi.IsDir())
		assert.Equals(t, fs.FileMode(0700), fi.Mode().Perm())
		assert.Equals(t, []lchownCall{{mountCacheDir, 65536, 65536}}, rec.recorded())

		fr.handles[0].Exit(0, "")
		pm.Shutdown()
	})

	t.Run("points Mountpoint at the mount's directory, whatever --cache the request carried", func(t *testing.T) {
		fr := &fakeProcessRunner{}
		pm, cacheDir, _ := newCachingProcessManager(t, fr, cacheLimit{strategy: cacheLimitNone})
		// Repeated `cache` mount options on a PV reach the mounter; three in all, as Args.Set alone drops a second.
		options := cachedOptions(t, "--cache=/elsewhere", "--cache=/other")

		assert.NoError(t, pm.Launch(mountId, "/usr/bin/mount-s3", options))

		assert.Equals(t, []string{"--cache=" + filepath.Join(cacheDir, mountId), "--foreground"}, fr.handles[0].cmd.Args[3:])
		assert.Equals(t, []string{"--cache=/cache/" + mountId, "--cache=/elsewhere", "--cache=/other"}, options.Args)

		fr.handles[0].Exit(0, "")
		pm.Shutdown()
	})

	t.Run("replaces a leftover directory instead of re-owning it", func(t *testing.T) {
		fr := &fakeProcessRunner{}
		pm, cacheDir, _ := newCachingProcessManager(t, fr, cacheLimit{strategy: cacheLimitNone})
		// As a killed Mountpoint leaves it.
		leftoverBlock := filepath.Join(cacheDir, mountId, "mountpoint-cache", "block")
		assert.NoError(t, os.MkdirAll(filepath.Dir(leftoverBlock), 0700))
		assert.NoError(t, os.WriteFile(leftoverBlock, []byte("x"), 0600))

		assert.NoError(t, pm.Launch(mountId, "/usr/bin/mount-s3", cachedOptions(t)))

		assertNotExist(t, leftoverBlock)
		fi, err := os.Lstat(filepath.Join(cacheDir, mountId))
		assert.NoError(t, err)
		assert.Equals(t, true, fi.IsDir())

		fr.handles[0].Exit(0, "")
		pm.Shutdown()
	})

	t.Run("replaces a symlink left at the mount's path without touching its target", func(t *testing.T) {
		fr := &fakeProcessRunner{}
		pm, cacheDir, _ := newCachingProcessManager(t, fr, cacheLimit{strategy: cacheLimitNone})
		victim := t.TempDir()
		victimFile := filepath.Join(victim, "secret")
		assert.NoError(t, os.WriteFile(victimFile, []byte("x"), 0600))
		assert.NoError(t, os.Symlink(victim, filepath.Join(cacheDir, mountId)))

		assert.NoError(t, pm.Launch(mountId, "/usr/bin/mount-s3", cachedOptions(t)))

		_, err := os.Stat(victimFile)
		assert.NoError(t, err)
		fi, err := os.Lstat(filepath.Join(cacheDir, mountId))
		assert.NoError(t, err)
		assert.Equals(t, true, fi.IsDir())

		fr.handles[0].Exit(0, "")
		pm.Shutdown()
	})

	t.Run("gives an uncached mount no directory", func(t *testing.T) {
		fr := &fakeProcessRunner{}
		pm, cacheDir, rec := newCachingProcessManager(t, fr, cacheLimit{strategy: cacheLimitNone})
		dev := mountertest.OpenDevNull(t)

		assert.NoError(t, pm.Launch(mountId, "/usr/bin/mount-s3", mountoptions.Options{Uid: 65536, Gid: 65536, Fd: int(dev.Fd()), BucketName: "my-bucket"}))

		entries, err := os.ReadDir(cacheDir)
		assert.NoError(t, err)
		assert.Equals(t, 0, len(entries))
		assert.Equals(t, 0, len(rec.recorded()))
		assert.Equals(t, []string{"--foreground"}, fr.handles[0].cmd.Args[3:])

		fr.handles[0].Exit(0, "")
		pm.Shutdown()
	})

	t.Run("leaves a running mount's directory alone when a duplicate arrives", func(t *testing.T) {
		fr := &fakeProcessRunner{}
		pm, cacheDir, _ := newCachingProcessManager(t, fr, cacheLimit{strategy: cacheLimitNone})
		assert.NoError(t, pm.Launch(mountId, "/usr/bin/mount-s3", cachedOptions(t)))
		cachedBlock := filepath.Join(cacheDir, mountId, "block")
		assert.NoError(t, os.WriteFile(cachedBlock, []byte("x"), 0600))

		if err := pm.Launch(mountId, "/usr/bin/mount-s3", cachedOptions(t)); err == nil {
			t.Fatal("expected the duplicate launch to be rejected")
		}

		_, err := os.Stat(cachedBlock)
		assert.NoError(t, err)

		fr.handles[0].Exit(0, "")
		pm.Shutdown()
	})

	t.Run("refuses a cached mount when the container has no cache volume", func(t *testing.T) {
		fr := &fakeProcessRunner{}
		pm := NewProcessManager(t.TempDir(), "", fr, memoryLimit{strategy: memoryLimitNone}, cacheLimit{strategy: cacheLimitNone})

		err := pm.Launch(mountId, "/usr/bin/mount-s3", cachedOptions(t))
		if err == nil {
			t.Fatal("expected Launch to refuse a cache request with no cache volume")
		}
		// Note: removeCacheDir would also refuse the empty volume, so the message is what shows this check ran.
		assert.Contains(t, err.Error(), "has no cache volume")
		assert.Equals(t, 0, len(fr.handles))
	})

	t.Run("refuses any mount, uncached too, while the cache volume cannot be read", func(t *testing.T) {
		fr := &fakeProcessRunner{}
		pm := NewProcessManager(t.TempDir(), filepath.Join(t.TempDir(), "missing"), fr, memoryLimit{strategy: memoryLimitNone}, cacheLimit{strategy: cacheLimitNone})
		dev := mountertest.OpenDevNull(t)

		err := pm.Launch(mountId, "/usr/bin/mount-s3", mountoptions.Options{Uid: 65536, Gid: 65536, Fd: int(dev.Fd()), BucketName: "my-bucket"})
		if err == nil {
			t.Fatal("expected Launch to refuse an uncached mount while the cache volume cannot be read")
		}
		assert.Equals(t, 0, len(fr.handles))
	})

	t.Run("fails without starting Mountpoint when its directory cannot be handed to the UID, and releases the mount", func(t *testing.T) {
		fr := &fakeProcessRunner{}
		pm, _, rec := newCachingProcessManager(t, fr, cacheLimit{strategy: cacheLimitNone})
		rec.err = syscall.EPERM

		if err := pm.Launch(mountId, "/usr/bin/mount-s3", cachedOptions(t)); err == nil {
			t.Fatal("expected Launch to fail when the cache directory cannot be chowned")
		}
		assert.Equals(t, 0, len(fr.handles))

		// A retry must get the lock and find nothing tracked; 2s only bounds the failing case.
		rec.err = nil
		options := cachedOptions(t)
		retried := make(chan error, 1)
		go func() { retried <- pm.Launch(mountId, "/usr/bin/mount-s3", options) }()
		select {
		case err := <-retried:
			assert.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("retried Launch did not return: the failed launch kept the lock")
		}

		fr.handles[0].Exit(0, "")
		pm.Shutdown()
	})

	t.Run("fails without starting Mountpoint when the cache volume cannot be written to", func(t *testing.T) {
		// Assert, not skip: CI is unprivileged, so a root run must fail loudly rather than lose this case. (Considered skipping if root run)
		assert.Equals(t, false, os.Geteuid() == 0)
		fr := &fakeProcessRunner{}
		pm, cacheDir, _ := newCachingProcessManager(t, fr, cacheLimit{strategy: cacheLimitNone})
		// A read-only cache volume: the kubelet has mounted it, but nothing can be created inside.
		// e.g. HugePages emptyDir medium which we should reject.
		assert.NoError(t, os.Chmod(cacheDir, 0500))
		t.Cleanup(func() { os.Chmod(cacheDir, 0700) })

		err := pm.Launch(mountId, "/usr/bin/mount-s3", cachedOptions(t))
		if err == nil {
			t.Fatal("expected Launch to fail on a read-only cache volume")
		}
		assert.Contains(t, err.Error(), "failed to create cache directory")
		assert.Equals(t, 0, len(fr.handles))
	})

	t.Run("removes the directory when Mountpoint fails to start", func(t *testing.T) {
		fr := &fakeProcessRunner{startErr: errors.New("fork/exec: no such file")}
		pm, cacheDir, _ := newCachingProcessManager(t, fr, cacheLimit{strategy: cacheLimitNone})

		if err := pm.Launch(mountId, "/usr/bin/mount-s3", cachedOptions(t)); err == nil {
			t.Fatal("expected Launch to fail when the process cannot start")
		}
		assertNotExist(t, filepath.Join(cacheDir, mountId))
	})

	t.Run("removes the directory once Mountpoint exits", func(t *testing.T) {
		for _, testCase := range []struct {
			name     string
			exitCode int
		}{
			{"after a clean exit", 0},
			{"after a failure", 1},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				fr := &fakeProcessRunner{}
				pm, cacheDir, _ := newCachingProcessManager(t, fr, cacheLimit{strategy: cacheLimitNone})
				assert.NoError(t, pm.Launch(mountId, "/usr/bin/mount-s3", cachedOptions(t)))

				fr.handles[0].Exit(testCase.exitCode, "")
				pm.Shutdown()

				assertNotExist(t, filepath.Join(cacheDir, mountId))
			})
		}
	})

	t.Run("keeps the mount claimed until its directory is removed", func(t *testing.T) {
		fr := &fakeProcessRunner{}
		pm, cacheDir, _ := newCachingProcessManager(t, fr, cacheLimit{strategy: cacheLimitNone})
		mountCacheDir := filepath.Join(cacheDir, mountId)
		assert.NoError(t, pm.Launch(mountId, "/usr/bin/mount-s3", cachedOptions(t)))

		// Holding the lock parks the waiter at its delete; the directory must already be gone by then.
		// 2s only bounds the failing case.
		pm.mu.Lock()
		fr.handles[0].Exit(1, "boom")
		dirGone := false
		for deadline := time.Now().Add(2 * time.Second); !dirGone && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			_, err := os.Lstat(mountCacheDir)
			dirGone = errors.Is(err, fs.ErrNotExist)
		}
		pm.mu.Unlock()
		assert.Equals(t, true, dirGone)

		pm.Shutdown()
	})
}

func TestProcessManager_Launch_MultipleProcesses(t *testing.T) {
	commDir := t.TempDir()
	fr := &fakeProcessRunner{}
	pm := NewProcessManager(commDir, "", fr, memoryLimit{strategy: memoryLimitNone}, cacheLimit{strategy: cacheLimitNone})

	for i, id := range []string{"mount-a", "mount-b", "mount-c"} {
		dev := mountertest.OpenDevNull(t)
		err := pm.Launch(id, "/usr/bin/mount-s3", mountoptions.Options{
			Uid:        65536,
			Gid:        65536,
			Fd:         int(dev.Fd()),
			BucketName: fmt.Sprintf("bucket-%d", i),
		})
		assert.NoError(t, err)
	}

	// All 3 tracked
	pm.mu.Lock()
	assert.Equals(t, 3, len(pm.processes))
	pm.mu.Unlock()

	// Verify each got correct bucket and received exactly one FD
	assert.Equals(t, "bucket-0", fr.handles[0].cmd.Args[1])
	assert.Equals(t, "bucket-1", fr.handles[1].cmd.Args[1])
	assert.Equals(t, "bucket-2", fr.handles[2].cmd.Args[1])

	// One exits with error, one clean, one still running
	fr.handles[0].Exit(1, "oom killed")
	fr.handles[1].Exit(0, "")

	// Give goroutines time to process
	time.Sleep(50 * time.Millisecond)

	// Only mount-c still tracked
	pm.mu.Lock()
	assert.Equals(t, 1, len(pm.processes))
	_, hasMountC := pm.processes["mount-c"]
	assert.Equals(t, true, hasMountC)
	pm.mu.Unlock()

	// Error file written for mount-a, not for mount-b
	errBytes, err := os.ReadFile(filepath.Join(commDir, "mount-a.error"))
	assert.NoError(t, err)
	assert.Equals(t, "oom killed", string(errBytes))

	_, err = os.ReadFile(filepath.Join(commDir, "mount-b.error"))
	assert.Equals(t, true, os.IsNotExist(err))

	// Shutdown signals remaining and waits
	fr.handles[2].Exit(0, "")
	pm.Shutdown()

	pm.mu.Lock()
	assert.Equals(t, 0, len(pm.processes))
	pm.mu.Unlock()
}

func TestProcessManager_Launch_DuplicateMountId_Rejected(t *testing.T) {
	commDir := t.TempDir()
	fr := &fakeProcessRunner{}
	pm := NewProcessManager(commDir, "", fr, memoryLimit{strategy: memoryLimitNone}, cacheLimit{strategy: cacheLimitNone})

	dev1 := mountertest.OpenDevNull(t)
	err := pm.Launch("same-mount", "/usr/bin/mount-s3", mountoptions.Options{
		Uid:        65536,
		Gid:        65536,
		Fd:         int(dev1.Fd()),
		BucketName: "bucket",
	})
	assert.NoError(t, err)

	// Second launch with same mountId should fail
	dev2 := mountertest.OpenDevNull(t)
	err = pm.Launch("same-mount", "/usr/bin/mount-s3", mountoptions.Options{
		Uid:        65536,
		Gid:        65536,
		Fd:         int(dev2.Fd()),
		BucketName: "bucket",
	})
	if err == nil {
		t.Fatal("Expected error for duplicate mountId, got nil")
	}

	// Only one process tracked
	pm.mu.Lock()
	assert.Equals(t, 1, len(pm.processes))
	pm.mu.Unlock()

	// After first exits, same mountId can be reused
	fr.handles[0].Exit(0, "")
	time.Sleep(10 * time.Millisecond)

	dev3 := mountertest.OpenDevNull(t)
	err = pm.Launch("same-mount", "/usr/bin/mount-s3", mountoptions.Options{
		Uid:        65536,
		Gid:        65536,
		Fd:         int(dev3.Fd()),
		BucketName: "bucket",
	})
	assert.NoError(t, err)

	fr.handles[1].Exit(0, "")
	pm.Shutdown()
}

func TestCheckCacheEntriesNotOwnedBy(t *testing.T) {
	me := uint32(os.Getuid())
	newVolume := func(t *testing.T, entries ...string) string {
		dir := t.TempDir()
		for _, e := range entries {
			assert.NoError(t, os.Mkdir(filepath.Join(dir, e), 0700))
		}
		return dir
	}

	t.Run("passes a UID that owns nothing in the cache volume", func(t *testing.T) {
		assert.NoError(t, checkCacheEntriesNotOwnedBy(newVolume(t, "other-pv"), "this-pv", false, me+1))
	})
	t.Run("passes a cached launch over its own leftover, which it replaces", func(t *testing.T) {
		assert.NoError(t, checkCacheEntriesNotOwnedBy(newVolume(t, "this-pv"), "this-pv", true, me))
	})
	t.Run("refuses a UID that still owns another mount's directory, without naming it", func(t *testing.T) {
		err := checkCacheEntriesNotOwnedBy(newVolume(t, "other-pv"), "this-pv", true, me)
		if err == nil {
			t.Fatal("expected a UID owning another mount's cache directory to be refused")
		}
		assert.Equals(t, fmt.Sprintf("UID %d still owns a cache directory", me), err.Error())
	})
	t.Run("refuses an uncached launch over its own leftover, which nothing replaces", func(t *testing.T) {
		if err := checkCacheEntriesNotOwnedBy(newVolume(t, "this-pv"), "this-pv", false, me); err == nil {
			t.Fatal("expected an uncached launch to be refused over a leftover its UID owns")
		}
	})
	t.Run("fails when the cache volume cannot be read", func(t *testing.T) {
		if err := checkCacheEntriesNotOwnedBy(filepath.Join(t.TempDir(), "missing"), "this-pv", false, me); err == nil {
			t.Fatal("expected a missing cache volume to fail the check")
		}
	})
}

func TestRemoveCacheDir(t *testing.T) {
	t.Run("removes the mount's directory and its contents", func(t *testing.T) {
		cacheDir := t.TempDir()
		assert.NoError(t, os.Mkdir(filepath.Join(cacheDir, "s3-pv"), 0700))
		assert.NoError(t, os.WriteFile(filepath.Join(cacheDir, "s3-pv", "block"), []byte("x"), 0600))

		assert.NoError(t, removeCacheDir(cacheDir, "s3-pv"))
		_, err := os.Stat(filepath.Join(cacheDir, "s3-pv"))
		assert.Equals(t, true, os.IsNotExist(err))
	})

	t.Run("is idempotent, so a retried cleanup does not fail", func(t *testing.T) {
		cacheDir := t.TempDir()
		assert.NoError(t, os.Mkdir(filepath.Join(cacheDir, "s3-pv"), 0700))

		assert.NoError(t, removeCacheDir(cacheDir, "s3-pv"))
		assert.NoError(t, removeCacheDir(cacheDir, "s3-pv"))
	})

	t.Run("fails when the mounter has no cache volume", func(t *testing.T) {
		if err := removeCacheDir("", "s3-pv"); err == nil {
			t.Fatal("expected removeCacheDir to refuse an empty cache volume")
		}
	})

	t.Run("fails when there is a problem with the os.RemoveAll path", func(t *testing.T) {
		testCases := []struct {
			name    string
			mountId string
		}{
			{name: "an unnamed mount", mountId: ""},
			{name: "the volume root itself", mountId: "."},
			{name: "the parent of the volume root", mountId: ".."},
			{name: "a path escaping the volume root", mountId: "../sibling"},
			{name: "a subdirectory of a mount's cache", mountId: "s3-pv/mountpoint-cache/V2"},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				cacheDir := t.TempDir()
				sibling := filepath.Join(filepath.Dir(cacheDir), "sibling")
				assert.NoError(t, os.Mkdir(sibling, 0770))
				cachedBlocks := filepath.Join(cacheDir, "s3-pv", "mountpoint-cache", "V2")
				assert.NoError(t, os.MkdirAll(cachedBlocks, 0770))

				if err := removeCacheDir(cacheDir, testCase.mountId); err == nil {
					t.Fatal("expected removeCacheDir to refuse a mountId that is not a plain directory name")
				}

				// Check that it did not run os.RemoveAll after returning error, and that
				// the cache volume root and its sibling are still there.
				_, err := os.Stat(cacheDir)
				assert.NoError(t, err)
				_, err = os.Stat(filepath.Dir(cacheDir))
				assert.NoError(t, err)
				_, err = os.Stat(sibling)
				assert.NoError(t, err)
				_, err = os.Stat(cachedBlocks)
				assert.NoError(t, err)
			})
		}
	})
}

func TestProcessManager_Shutdown_SendsSIGTERM(t *testing.T) {
	commDir := t.TempDir()
	fr := &fakeProcessRunner{}
	pm := NewProcessManager(commDir, "", fr, memoryLimit{strategy: memoryLimitNone}, cacheLimit{strategy: cacheLimitNone})

	dev := mountertest.OpenDevNull(t)
	err := pm.Launch("m1", "/usr/bin/mount-s3", mountoptions.Options{
		Uid:        65536,
		Gid:        65536,
		Fd:         int(dev.Fd()),
		BucketName: "b",
	})
	assert.NoError(t, err)

	go func() {
		select {
		case sig := <-fr.handles[0].sigCh:
			assert.Equals(t, os.Signal(syscall.SIGTERM), sig)
			fr.handles[0].Exit(0, "")
		case <-time.After(5 * time.Second):
			t.Error("Timed out waiting for SIGTERM")
			fr.handles[0].Exit(1, "")
		}
	}()

	pm.Shutdown()
}

// TestHandleConnection_NoFdLeak verifies that handleConnection does not leak file descriptors
// across multiple iterations, including the error path (empty VolumeId).
// NOTE: Do not use t.Parallel() here — fd counting via /proc/self/fd is process-global
// and would be unreliable if other tests open/close fds concurrently.
func TestHandleConnection_NoFdLeak(t *testing.T) {
	commDir := t.TempDir()
	fr := &fakeProcessRunner{}
	pm := NewProcessManager(commDir, "", fr, memoryLimit{strategy: memoryLimitNone}, cacheLimit{strategy: cacheLimitNone})

	sockPath := filepath.Join(commDir, "test.sock")
	listener, err := net.Listen("unix", sockPath)
	assert.NoError(t, err)
	defer listener.Close()

	fdsBefore := countOpenFds(t)

	const iterations = 5
	for i := range iterations {
		dev := mountertest.OpenDevNull(t)
		sendDone := make(chan struct{})
		volumeId := fmt.Sprintf("vol-%d", i)
		if i == iterations-1 {
			volumeId = "" // no VolumeId — handleConnection should close fd without launching
		}
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			mountoptions.Send(ctx, sockPath, mountoptions.Options{
				Uid:        65536,
				Gid:        65536,
				Fd:         int(dev.Fd()),
				BucketName: "bucket",
				VolumeId:   volumeId,
			})
			dev.Close()
			close(sendDone)
		}()

		conn, err := listener.Accept()
		assert.NoError(t, err)
		handleConnection(conn.(*net.UnixConn), "/opt/mount-s3", pm, 5*time.Second)
		<-sendDone
		if i < iterations-1 {
			fr.handles[i].Exit(0, "")
		}
	}

	time.Sleep(50 * time.Millisecond)
	pm.Shutdown()

	fdsAfter := countOpenFds(t)
	if fdsAfter > fdsBefore {
		t.Errorf("fd leak: %d before, %d after", fdsBefore, fdsAfter)
	}
}

func countOpenFds(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	assert.NoError(t, err)
	return len(entries)
}

func TestHandleConnection_MountIdValidation(t *testing.T) {
	validIds := []string{
		"abcdef12-3456-7890-abcd-ef1234567890-vol456",
		"pvc-abcdef12-3456-7890-abcd-ef1234567890",
		"s3-pv",
	}
	invalidIds := []string{
		"../../etc/passwd",
		"../parent",
		"slash/inside",
		"/absolute",
		"-starts-with-dash",
		".starts-with-dot",
		"has spaces",
		"has\nnewline",
	}

	commDir := t.TempDir()
	fr := &fakeProcessRunner{}
	pm := NewProcessManager(commDir, "", fr, memoryLimit{strategy: memoryLimitNone}, cacheLimit{strategy: cacheLimitNone})

	sockPath := filepath.Join(commDir, "test.sock")
	listener, err := net.Listen("unix", sockPath)
	assert.NoError(t, err)
	defer listener.Close()

	dev := mountertest.OpenDevNull(t)
	defer dev.Close()

	sendMount := func(id string) {
		sendDone := make(chan struct{})
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			mountoptions.Send(ctx, sockPath, mountoptions.Options{
				Uid:        65536,
				Gid:        65536,
				Fd:         int(dev.Fd()),
				BucketName: "bucket",
				VolumeId:   id,
			})
			close(sendDone)
		}()

		conn, err := listener.Accept()
		assert.NoError(t, err)
		handleConnection(conn.(*net.UnixConn), "/opt/mount-s3", pm, 5*time.Second)
		<-sendDone
	}

	for _, id := range invalidIds {
		sendMount(id)
	}

	fr.mu.Lock()
	assert.Equals(t, 0, len(fr.handles))
	fr.mu.Unlock()

	for _, id := range validIds {
		sendMount(id)
	}

	fr.mu.Lock()
	assert.Equals(t, len(validIds), len(fr.handles))
	fr.mu.Unlock()

	for _, h := range fr.handles {
		h.Exit(0, "")
	}
	pm.Shutdown()
}

func TestHandleConnection_RefusedLaunchWritesErrorFile(t *testing.T) {
	commDir := t.TempDir()
	fr := &fakeProcessRunner{}
	pm := NewProcessManager(commDir, "", fr, memoryLimit{strategy: memoryLimitNone}, cacheLimit{strategy: cacheLimitNone})

	sockPath := filepath.Join(commDir, "test.sock")
	listener, err := net.Listen("unix", sockPath)
	assert.NoError(t, err)
	defer listener.Close()

	dev := mountertest.OpenDevNull(t)
	// Waited on below, so the sender is done with dev before its cleanup closes it.
	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		mountoptions.Send(ctx, sockPath, mountoptions.Options{Fd: int(dev.Fd()), BucketName: "bucket", VolumeId: "s3-pv"})
	}()

	conn, err := listener.Accept()
	assert.NoError(t, err)
	handleConnection(conn.(*net.UnixConn), "/opt/mount-s3", pm, 5*time.Second)
	<-sendDone

	errBytes, err := os.ReadFile(filepath.Join(commDir, "s3-pv.error"))
	assert.NoError(t, err)
	assert.Contains(t, string(errBytes), "out-of-range UID 0")
	assert.Equals(t, 0, len(fr.handles))
}

func TestProcessManager_Launch_ErrorExit_WritesErrorFile(t *testing.T) {
	commDir := t.TempDir()
	fr := &fakeProcessRunner{}
	pm := NewProcessManager(commDir, "", fr, memoryLimit{strategy: memoryLimitNone}, cacheLimit{strategy: cacheLimitNone})

	dev := mountertest.OpenDevNull(t)
	err := pm.Launch("mount-abc", "/usr/bin/mount-s3", mountoptions.Options{
		Uid:        65536,
		Gid:        65536,
		Fd:         int(dev.Fd()),
		BucketName: "bucket",
	})
	assert.NoError(t, err)

	fr.handles[0].Exit(1, "credential error")
	pm.Shutdown()

	errBytes, err := os.ReadFile(filepath.Join(commDir, "mount-abc.error"))
	assert.NoError(t, err)
	assert.Equals(t, "credential error", string(errBytes))
}

func TestProcessManager_Launch_StartsWithNoEarlierErrorFile(t *testing.T) {
	commDir := t.TempDir()
	fr := &fakeProcessRunner{}
	pm := NewProcessManager(commDir, "", fr, memoryLimit{strategy: memoryLimitNone}, cacheLimit{strategy: cacheLimitNone})
	launch := func() error {
		dev := mountertest.OpenDevNull(t)
		return pm.Launch("mount-abc", "/usr/bin/mount-s3", mountoptions.Options{Uid: 65536, Gid: 65536, Fd: int(dev.Fd()), BucketName: "bucket"})
	}
	assert.NoError(t, launch())

	// A FIFO holds the waiter's error-file write open until it is read, so whatever it guards stays visible.
	errFile := filepath.Join(commDir, "mount-abc.error")
	assert.NoError(t, syscall.Mkfifo(errFile, 0600))
	fr.handles[0].Exit(1, "credential error")

	relaunched := make(chan error, 1)
	for {
		go func() { relaunched <- launch() }()
		select {
		case err := <-relaunched:
			if err == nil {
				t.Fatal("relaunch accepted before the earlier exit's error file was written")
			}
			continue // still claimed; the waiter has not reached its write yet
		case <-time.After(200 * time.Millisecond):
		}
		break
	}

	got, err := os.ReadFile(errFile)
	assert.NoError(t, err)
	assert.Equals(t, "credential error", string(got))
	assert.NoError(t, <-relaunched)
	_, err = os.Lstat(errFile)
	assert.Equals(t, true, errors.Is(err, fs.ErrNotExist))

	fr.mu.Lock()
	h := fr.handles[len(fr.handles)-1]
	fr.mu.Unlock()
	h.Exit(0, "")
	pm.Shutdown()
}

func TestProcessManager_Launch_RejectsCredentialsOutsideTheAllocatorRange(t *testing.T) {
	for _, tc := range []struct {
		name     string
		uid, gid uint32
	}{
		{"zero, the unset value", 0, 0},
		{"below the range", mounter.UIDRangeStart - 1, mounter.UIDRangeStart - 1},
		{"above the range", mounter.UIDRangeEnd + 1, mounter.UIDRangeEnd + 1},
		{"GID not matching UID", mounter.UIDRangeStart, mounter.UIDRangeStart + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fr := &fakeProcessRunner{}
			pm := NewProcessManager(t.TempDir(), "", fr, memoryLimit{strategy: memoryLimitNone}, cacheLimit{strategy: cacheLimitNone})
			dev := mountertest.OpenDevNull(t)

			err := pm.Launch("vol-bad-creds", "/usr/bin/mount-s3", mountoptions.Options{
				Uid:        tc.uid,
				Gid:        tc.gid,
				Fd:         int(dev.Fd()),
				BucketName: "bucket",
			})
			if err == nil {
				t.Fatalf("expected Launch to refuse uid %d gid %d", tc.uid, tc.gid)
			}

			fr.mu.Lock()
			defer fr.mu.Unlock()
			assert.Equals(t, 0, len(fr.handles))
		})
	}
}
