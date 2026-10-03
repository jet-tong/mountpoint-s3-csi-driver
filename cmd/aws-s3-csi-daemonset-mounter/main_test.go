package main

import (
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/util/testutil/assert"
)

func TestServe_EmptiesTheCacheVolumeBeforeListeningAndAfterStopping(t *testing.T) {
	pm, cacheDir := newProcessManagerWithCache(t, &fakeProcessRunner{}, cacheLimit{strategy: cacheLimitNone})
	assert.NoError(t, os.Mkdir(filepath.Join(cacheDir, "pv-old"), 0700))
	sock := filepath.Join(t.TempDir(), mountSockName)
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- serve(pm, sock, "/usr/bin/mount-s3", stop) }()

	// The socket exists only once the startup cleanup has run.
	waitAndAssertListening(t, sock)
	assertNotExist(t, filepath.Join(cacheDir, "pv-old"))

	assert.NoError(t, os.Mkdir(filepath.Join(cacheDir, "pv-new"), 0700))
	close(stop)
	assert.NoError(t, <-done)
	assertNotExist(t, filepath.Join(cacheDir, "pv-new"))
}

func TestServe_StartsRemovingUntrackedCacheDirsOnlyAfterStartupCleanup(t *testing.T) {
	interval := untrackedCacheDirsInterval
	untrackedCacheDirsInterval = 10 * time.Millisecond
	t.Cleanup(func() { untrackedCacheDirsInterval = interval })
	pm, cacheDir := newProcessManagerWithCache(t, &fakeProcessRunner{}, cacheLimit{strategy: cacheLimitNone})
	runner := &gatedRunner{gate: make(chan struct{})}
	pm.runner = runner
	// Not empty, so startup cleanup waits at the gate for its removal helper.
	blockDir := filepath.Join(cacheDir, "uid-65537", "mountpoint-cache")
	assert.NoError(t, os.MkdirAll(blockDir, 0700))
	assert.NoError(t, os.WriteFile(filepath.Join(blockDir, "block"), []byte("x"), 0600))
	sock := filepath.Join(t.TempDir(), mountSockName)
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- serve(pm, sock, "/usr/bin/mount-s3", stop) }()

	// 100ms is ten intervals, ample for a pass started too early to reach its own helper.
	time.Sleep(100 * time.Millisecond)
	assert.Equals(t, int32(1), runner.starts.Load())

	close(runner.gate)
	waitAndAssertListening(t, sock)
	close(stop)
	assert.NoError(t, <-done)
}

func TestServe_RemovesUntrackedCacheDirsWhileServing(t *testing.T) {
	interval := untrackedCacheDirsInterval
	untrackedCacheDirsInterval = 10 * time.Millisecond
	t.Cleanup(func() { untrackedCacheDirsInterval = interval })
	pm, cacheDir := newProcessManagerWithCache(t, &fakeProcessRunner{}, cacheLimit{strategy: cacheLimitNone})
	sock := filepath.Join(t.TempDir(), mountSockName)
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- serve(pm, sock, "/usr/bin/mount-s3", stop) }()
	waitAndAssertListening(t, sock)

	// Created after startup cleanup, so only the periodic pass can remove it before stop.
	leftover := filepath.Join(cacheDir, "uid-65537")
	assert.NoError(t, os.Mkdir(leftover, 0700))
	removed := false
	// 5s only bounds the failing case.
	for deadline := time.Now().Add(5 * time.Second); !removed && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		_, err := os.Lstat(leftover)
		removed = errors.Is(err, fs.ErrNotExist)
	}
	assert.Equals(t, true, removed)

	close(stop)
	assert.NoError(t, <-done)
}

func TestServe_ExitsWithoutListeningWhenTheCacheVolumeCannotBeLocked(t *testing.T) {
	pm, _ := newProcessManagerWithCache(t, &fakeProcessRunner{}, cacheLimit{strategy: cacheLimitNone})
	pm.chown = func(string, int, int) error { return syscall.EPERM }
	sock := filepath.Join(t.TempDir(), mountSockName)
	// Already closed, so a serve that wrongly gets past the lock returns instead of serving forever.
	stop := make(chan struct{})
	close(stop)

	err := serve(pm, sock, "/usr/bin/mount-s3", stop)
	if err == nil {
		t.Fatal("expected serve to fail when the cache volume cannot be locked")
	}
	assert.Contains(t, err.Error(), "cannot chown")
	assertNotExist(t, sock)
}

// waitAndAssertListening waits until serve listens on sock.
func waitAndAssertListening(t *testing.T, sock string) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if _, err := os.Lstat(sock); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("serve did not listen on %s", sock)
		}
	}
}

// gatedRunner holds every start until gate closes, and counts them.
type gatedRunner struct {
	fakeProcessRunner
	gate   chan struct{}
	starts atomic.Int32
}

func (r *gatedRunner) Start(cmd *exec.Cmd) (ProcessHandle, error) {
	r.starts.Add(1)
	<-r.gate
	return r.fakeProcessRunner.Start(cmd)
}
