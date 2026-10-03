package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/shirou/gopsutil/v4/process"
	"k8s.io/klog/v2"

	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/driver/node/mounter"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/mountpoint"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/mountpoint/mountoptions"
)

const errorFilePerm = fs.FileMode(0600)
const errorFileExt = ".error"

// cacheVolumePerm lets a Mountpoint reach its own cache directory but not create or list entries beside it.
const cacheVolumePerm = fs.FileMode(0711)

// mountCacheDirPerm closes a mount's cache directory to every UID but the one its Mountpoint runs as.
const mountCacheDirPerm = fs.FileMode(0700)

// emptyDirArg makes this binary empty a cache directory as its owner, since the mounter cannot read inside one.
const emptyDirArg = "empty-cache-dir"

// killProcessesArg makes this binary kill every process of the UID it runs as, so the mounter never signals a PID read from /proc.
const killProcessesArg = "kill-processes"

const (
	// killHelperTimeout bounds a kill helper, which a leftover process of the same UID is allowed to stop.
	killHelperTimeout = 5 * time.Second
	// SIGKILL takes effect when each process next runs, and a fork loop can outrun one kill.
	stopRounds           = 3
	stopRoundPause       = 100 * time.Millisecond
	stopRetryInterval    = time.Second
	maxStopRetryInterval = time.Minute
	// childWaitDelay lets Wait return when a process left behind still holds a child's stdout or stderr.
	childWaitDelay = 5 * time.Second
)

// uidMarkerPrefix and uidMarkerPerm describe the root-only file in the comm directory that tells csi-node a UID is still in use here.
const uidMarkerPrefix = ".uid-"
const uidMarkerPerm = fs.FileMode(0600)

// ProcessManager tracks and manages Mountpoint child processes.
type ProcessManager struct {
	commDir  string
	cacheDir string        // the cache volume's mount path, or "" when this container has none
	runner   ProcessRunner // interface for spawning processes; substituted in tests
	memory   memoryLimit
	cache    cacheLimit
	chown    func(path string, uid, gid int) error // nil = chown through a no-follow handle; tests are not root, so they record instead

	mu        sync.Mutex
	processes map[uint32]mountpointProcess // the UID a Mountpoint runs as -> that Mountpoint; one per UID
	wg        sync.WaitGroup               // tracks waiter goroutines

	shutdown chan struct{} // closed by Shutdown, so a waiter stops retrying
}

// mountpointProcess is a running Mountpoint and the mount it serves.
type mountpointProcess struct {
	mountId string
	handle  ProcessHandle
}

func NewProcessManager(commDir, cacheDir string, runner ProcessRunner, memory memoryLimit, cache cacheLimit) *ProcessManager {
	return &ProcessManager{
		commDir:   commDir,
		cacheDir:  cacheDir,
		runner:    runner,
		memory:    memory,
		cache:     cache,
		processes: make(map[uint32]mountpointProcess),
		shutdown:  make(chan struct{}),
	}
}

// secureCacheVolume gives the cache volume to root, so a Mountpoint can reach its own directory but not list or create beside it.
func (pm *ProcessManager) secureCacheVolume() error {
	if pm.cacheDir == "" {
		return nil
	}
	if err := pm.chownWithDefault(pm.cacheDir, 0, 0); err != nil {
		return fmt.Errorf("cannot chown %s: the cache volume must allow root to change ownership, e.g. not NFS with root_squash or an EFS access point: %w", pm.cacheDir, err)
	}
	if err := os.Chmod(pm.cacheDir, cacheVolumePerm); err != nil {
		return fmt.Errorf("cannot chmod %s to %o: %w", pm.cacheDir, cacheVolumePerm, err)
	}
	return nil
}

// emptyCacheVolume empties the cache volume, and returns an error for anything it could not remove.
func (pm *ProcessManager) emptyCacheVolume() error {
	if pm.cacheDir == "" {
		return nil
	}
	entries, err := os.ReadDir(pm.cacheDir)
	if err != nil {
		return fmt.Errorf("failed to list cache volume %q: %w", pm.cacheDir, err)
	}
	var errs []error
	leftovers := make(map[string]bool, len(entries))
	for _, entry := range entries {
		leftovers[entry.Name()] = true
		if err := pm.removeCacheVolumeEntry(entry.Name()); err != nil {
			errs = append(errs, err)
		}
	}

	// List again rather than trust the removals: anything left, or created meanwhile, is reported.
	remaining, err := os.ReadDir(pm.cacheDir)
	if err != nil {
		return fmt.Errorf("failed to list cache volume %q: %w", pm.cacheDir, err)
	}
	var notRemoved, appeared []string
	for _, entry := range remaining {
		if leftovers[entry.Name()] {
			notRemoved = append(notRemoved, entry.Name())
		} else {
			appeared = append(appeared, entry.Name())
		}
	}
	if len(notRemoved) > 0 {
		errs = append(errs, fmt.Errorf("cache volume %q still holds %v after cleanup", pm.cacheDir, notRemoved))
	}
	if len(appeared) > 0 {
		errs = append(errs, fmt.Errorf("cache volume %q gained %v during cleanup", pm.cacheDir, appeared))
	}
	return errors.Join(errs...)
}

// removeUIDMarkers removes every UID marker in the comm directory, which a previous mounter in this pod may have left.
func (pm *ProcessManager) removeUIDMarkers() error {
	entries, err := os.ReadDir(pm.commDir)
	if err != nil {
		return fmt.Errorf("failed to list comm directory %q: %w", pm.commDir, err)
	}
	var errs []error
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), uidMarkerPrefix) {
			if err := os.Remove(filepath.Join(pm.commDir, entry.Name())); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// Launch spawns a Mountpoint process for the given mount and waits for it asynchronously.
// Takes ownership of options.Fd, caller must not close it after calling this function.
// Returns an error if a process with the same mountId or UID is already running.
func (pm *ProcessManager) Launch(mountId string, mountpointPath string, options mountoptions.Options) error {
	fuseDev := os.NewFile(uintptr(options.Fd), "/dev/fuse")
	if fuseDev == nil {
		return fmt.Errorf("invalid FUSE file descriptor %d", options.Fd)
	}

	if options.Uid < mounter.UIDRangeStart || options.Uid > mounter.UIDRangeEnd {
		fuseDev.Close()
		return fmt.Errorf("refusing to launch mount %s with out-of-range UID %d", mountId, options.Uid)
	}
	if options.Gid != options.Uid {
		fuseDev.Close()
		return fmt.Errorf("refusing to launch mount %s with GID %d not matching UID %d", mountId, options.Gid, options.Uid)
	}

	args := mountpoint.ParseArgs(options.Args)
	args.Set(mountpoint.ArgForeground, mountpoint.ArgNoValue)

	// We point --cache at a directory derived here, so the one created and removed is the one Mountpoint uses.
	cached := args.Has(mountpoint.ArgCache)
	for args.Has(mountpoint.ArgCache) {
		args.Remove(mountpoint.ArgCache)
	}
	dirName := mountCacheDirName(options.Uid)
	if cached {
		if pm.cacheDir == "" {
			fuseDev.Close()
			return fmt.Errorf("refusing to launch mount %s: it requests a cache, but this container has no cache volume", mountId)
		}
		args.Set(mountpoint.ArgCache, filepath.Join(pm.cacheDir, dirName))
	}

	if targetMiB := pm.memory.targetFor(mountId, args); targetMiB > 0 {
		args.Set(mountpoint.ArgMemoryTarget, strconv.FormatInt(targetMiB, 10))
	}
	if sizeMiB, ok := pm.cache.maxCacheSizeFor(mountId, args); ok {
		args.Set(mountpoint.ArgMaxCacheSize, strconv.FormatInt(sizeMiB, 10))
	}

	cmdArgs := append([]string{
		options.BucketName,
		"/dev/fd/3", // ExtraFiles[0] becomes fd 3
	}, args.SortedList()...)

	cmd := exec.Command(mountpointPath, cmdArgs...)
	cmd.ExtraFiles = []*os.File{fuseDev}

	cmd.Env = options.Env
	cmd.Stdout = newPrefixWriter(os.Stdout, mountId)
	cmd.Stderr = newPrefixWriter(os.Stderr, mountId)
	// Without it, a process Mountpoint left behind holding these pipes would keep Wait, and so the stop below, from running.
	cmd.WaitDelay = childWaitDelay

	// Give the child the per-mount credentials csi-node determined, so the kernel isolates it from
	// every other Mountpoint on this node. Supplementary groups are cleared: the mounter runs as
	// root, and inheriting its groups would hand the child access to every other mount's files.
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{
			Uid:    options.Uid,
			Gid:    options.Gid,
			Groups: []uint32{},
		},
	}

	// Hold lock across duplicate check and process start to prevent races.
	pm.mu.Lock()
	// A scan, as processes is keyed by UID; it holds at most max-volumes-per-node entries.
	for _, p := range pm.processes {
		if p.mountId == mountId {
			pm.mu.Unlock()
			fuseDev.Close()
			return fmt.Errorf("mount %s already has a running process", mountId)
		}
	}
	// csi-node allocates a unique UID per mount, so a UID already serving another mount means a bug
	// or a stale/duplicate request. Reject it rather than run two Mountpoints under one UID, which
	// would defeat the per-mount kernel isolation.
	if other, taken := pm.processes[options.Uid]; taken {
		pm.mu.Unlock()
		fuseDev.Close()
		// The error file reaches this mount's pod events, and the other mount is another PV's.
		klog.Errorf("Refusing to launch mount %s with UID %d already in use by mount %s", mountId, options.Uid, other.mountId)
		// We rely on csi-node's UID allocator moving its cursor on, so the NodePublishVolume retry gets another UID.
		return fmt.Errorf("refusing to launch mount %s: UID %d is already in use by another mount", mountId, options.Uid)
	}
	// A process left behind as this UID could read this mount's credentials and cache. The waiter stops a UID's processes
	// before freeing it, so this refuses only after a bug, or for a process an admin started as this UID.
	if uids, err := pm.runner.ProcessUIDs(); err != nil || uids[options.Uid] {
		pm.mu.Unlock()
		fuseDev.Close()
		if err != nil {
			klog.Errorf("Refusing to launch mount %s: cannot list the processes of UID %d: %v", mountId, options.Uid, err)
		} else {
			klog.Errorf("Refusing to launch mount %s: UID %d still has processes here", mountId, options.Uid)
		}
		return fmt.Errorf("refusing to launch mount %s: UID %d is already in use", mountId, options.Uid)
	}
	// Delete any error files that earlier Mountpoint of this PV wrote after node deletes error files.
	// TODO if we add process to ensure Mountpoint exited, this should not be needed.
	os.Remove(filepath.Join(pm.commDir, mountId+errorFileExt))

	// No running Mountpoint has this UID, so its directory is a leftover; uncached too, as a Mountpoint owns and can enter it.
	if pm.cacheDir != "" {
		if err := pm.removeCacheVolumeEntry(dirName); err != nil {
			pm.mu.Unlock()
			fuseDev.Close()
			klog.Errorf("Failed to remove leftover cache directory %s: %v", dirName, err)
			return fmt.Errorf("failed to remove the leftover cache directory of UID %d, see the mounter log", options.Uid)
		}
	}
	if cached {
		if err := pm.createMountCacheDir(dirName, options.Uid); err != nil {
			pm.mu.Unlock()
			fuseDev.Close()
			return err
		}
	}

	// csi-node writes a mount's credentials, owned by its UID, before we can refuse a busy UID; so it skips a UID while this exists.
	marker := filepath.Join(pm.commDir, uidMarkerName(options.Uid))
	err := os.WriteFile(marker, nil, uidMarkerPerm)
	var handle ProcessHandle
	if err == nil {
		handle, err = pm.runner.Start(cmd)
	}
	if err != nil {
		os.Remove(marker)
		if cached {
			if rmErr := pm.removeCacheVolumeEntry(dirName); rmErr != nil {
				klog.Errorf("Failed to remove cache directory of mount %s after a failed start: %v", mountId, rmErr)
			}
		}
		pm.mu.Unlock()
		fuseDev.Close()
		return fmt.Errorf("failed to start Mountpoint: %w", err)
	}

	// Child has its own copy of the FD (kernel dup'd it during fork/exec).
	fuseDev.Close()

	pm.processes[options.Uid] = mountpointProcess{mountId: mountId, handle: handle}
	pm.mu.Unlock()

	klog.Infof("Launched Mountpoint for mount %s (pid %d)", mountId, handle.Pid())

	pm.wg.Add(1)
	go func() {
		defer pm.wg.Done()
		exitCode, stderr := handle.Wait()

		// For every exit, before the UID is freed: a process this Mountpoint left behind could read the next mount's credentials and cache.
		if !pm.stopProcessesOfWithRetry(options.Uid) {
			// Keep the UID and its marker: its processes die only with this mounter, and the next one removes the marker.
			klog.Errorf("Mountpoint for mount %s exited with code %d; keeping UID %d, whose processes could not be stopped before shutdown", mountId, exitCode, options.Uid)
			return
		}

		// Before freeing mountId and the UID, so a relaunch cannot create the directory this then removes.
		if cached {
			if err := pm.removeCacheVolumeEntry(dirName); err != nil {
				klog.Errorf("Failed to remove cache directory of mount %s: %v", mountId, err)
			}
		}

		pm.mu.Lock()
		// Before freeing mountId, so a relaunch's removal of a stale error file always comes after this write.
		if exitCode != 0 {
			pm.writeErrorFile(mountId, stderr)
		}
		delete(pm.processes, options.Uid)
		if err := os.Remove(marker); err != nil {
			klog.Errorf("Failed to remove %s, so csi-node will not hand out UID %d until the mounter restarts: %v", marker, options.Uid, err)
		}
		pm.mu.Unlock()

		if exitCode != 0 {
			klog.Errorf("Mountpoint for mount %s exited with code %d", mountId, exitCode)
		} else {
			klog.Infof("Mountpoint for mount %s exited cleanly", mountId)
		}
	}()

	return nil
}

// mountCacheDirName names the cache directory of the Mountpoint running as uid.
func mountCacheDirName(uid uint32) string {
	return fmt.Sprintf("uid-%d", uid)
}

// removeCacheVolumeEntry removes an entry of the cache volume, whoever owns it.
// Note Mountpoint also removes its own cache when it exits cleanly.
func (pm *ProcessManager) removeCacheVolumeEntry(name string) error {
	// We guard escapes and partial cache delete attempts by checking the cacheDir and name, and returning error if something is wrong.
	// Names come from mountCacheDirName or from listing the cache volume; so only a new caller could lead to these problems.
	if pm.cacheDir == "" || name == "" || name == "." || name == ".." || strings.ContainsRune(name, filepath.Separator) {
		return fmt.Errorf("refusing to remove cache directory %q in %q: not a cache volume and a plain directory name", name, pm.cacheDir)
	}
	path := filepath.Join(pm.cacheDir, name)
	// Root owns the cache volume, so it can remove an empty directory or a file, but cannot read inside another UID's directory.
	err := os.Remove(path)
	if errors.Is(err, syscall.ENOTEMPTY) {
		info, statErr := os.Lstat(path)
		if statErr != nil {
			return fmt.Errorf("failed to find the owner of cache directory %q: %w", path, statErr)
		}
		owner := info.Sys().(*syscall.Stat_t).Uid
		// Note: an empty entry was removed above without this check, as only emptying needs its owner.
		if uidStr, ok := strings.CutPrefix(name, "uid-"); ok {
			if named, parseErr := strconv.ParseUint(uidStr, 10, 32); parseErr == nil && uint32(named) != owner {
				klog.Errorf("Cache directory %q is owned by UID %d, not the UID %d its name gives; removing it anyway", path, owner, named)
			}
		}
		if err := pm.emptyDirAs(path, owner); err != nil {
			return err
		}
		err = os.Remove(path)
	}
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("failed to remove cache directory %q: %w", path, err)
	}
	return nil
}

// emptyDirAs empties dir by running this binary as uid with no groups; any uid but root also drops every capability.
func (pm *ProcessManager) emptyDirAs(dir string, uid uint32) error {
	klog.Infof("Emptying cache directory %s as UID %d", dir, uid)
	if err := pm.runHelper(context.Background(), uid, emptyDirArg, dir); err != nil {
		return fmt.Errorf("failed to empty cache directory %q as UID %d: %w", dir, uid, err)
	}
	return nil
}

// runHelper runs this binary with args as uid, with no groups and no environment, and fails unless it exits cleanly before ctx ends.
func (pm *ProcessManager) runHelper(ctx context.Context, uid uint32, args ...string) error {
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to find this binary: %w", err)
	}
	cmd := exec.CommandContext(ctx, self, args...)
	// The context kills the helper, but Wait still waits for its stderr, which a process of the same UID can open from /proc.
	cmd.WaitDelay = childWaitDelay
	// No environment: a process of the same UID could read the helper's from /proc.
	cmd.Env = []string{}
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uid, Gid: uid, Groups: []uint32{}}}
	handle, err := pm.runner.Start(cmd)
	if err != nil {
		return err
	}
	if exitCode, stderr := handle.Wait(); exitCode != 0 {
		return fmt.Errorf("exit status %d: %s", exitCode, bytes.TrimSpace(stderr))
	}
	return nil
}

// runAsRemovalHelper empties a directory and exits, when emptyDirAs started this binary as the removal helper.
func runAsRemovalHelper(args []string) {
	if len(args) != 3 || args[1] != emptyDirArg {
		return
	}
	if err := emptyDirAsOwner(args[2]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

// emptyDirAsOwner removes everything inside dir, as its owner, but not dir: only root can write the cache volume.
func emptyDirAsOwner(dir string) error {
	// A Mountpoint can make its own subdirectories unreadable, which stops the removal below.
	filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		// Directories only: Chmod follows symlinks, and WalkDir reports a symlink as not a directory.
		if err == nil && d.IsDir() {
			os.Chmod(path, mountCacheDirPerm)
		}
		return nil
	})
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(dir, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

// createMountCacheDir creates a mount's cache directory, owned by the UID its Mountpoint runs as. Mountpoint writes ./mountpoint-cache inside it.
func (pm *ProcessManager) createMountCacheDir(name string, uid uint32) error {
	mountCacheDir := filepath.Join(pm.cacheDir, name)
	if err := os.Mkdir(mountCacheDir, mountCacheDirPerm); err != nil {
		return fmt.Errorf("failed to create cache directory %q: %w", mountCacheDir, err)
	}
	if err := pm.chownWithDefault(mountCacheDir, int(uid), int(uid)); err != nil {
		return fmt.Errorf("failed to hand cache directory %q to UID %d: %w", mountCacheDir, uid, err)
	}

	klog.V(4).Infof("Created cache directory %s", mountCacheDir)
	return nil
}

func (pm *ProcessManager) chownWithDefault(path string, uid, gid int) error {
	if pm.chown != nil {
		return pm.chown(path, uid, gid)
	}
	// Chown through a handle opened without following symlinks, so a link planted at path cannot redirect it.
	dir, err := os.OpenFile(path, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Chown(uid, gid)
}

// uidMarkerName names the marker that exists while this mounter tracks a Mountpoint as uid.
func uidMarkerName(uid uint32) string {
	return uidMarkerPrefix + strconv.FormatUint(uint64(uid), 10)
}

// stopProcessesOfWithRetry retries stopProcessesOf with backoff until it succeeds, or until Shutdown starts.
func (pm *ProcessManager) stopProcessesOfWithRetry(uid uint32) (stopped bool) {
	for wait := stopRetryInterval; ; wait = min(2*wait, maxStopRetryInterval) {
		err := pm.stopProcessesOf(uid)
		if err == nil {
			return true
		}
		// The caller still holds the UID, and with it its mount ID, so that PV cannot remount on this node meanwhile.
		klog.Errorf("Failed to stop the processes of UID %d, retrying in %s: %v", uid, wait, err)
		select {
		case <-pm.shutdown:
			return false
		case <-time.After(wait):
		}
	}
}

// stopProcessesOf kills and reaps every process of uid in this PID namespace, and fails if any survive stopRounds kills. The caller
// must own uid: its Mountpoint has been waited for and no other helper runs as uid, so the reap never takes a status Go waits for.
func (pm *ProcessManager) stopProcessesOf(uid uint32) error {
	if uid < mounter.UIDRangeStart || uid > mounter.UIDRangeEnd {
		return fmt.Errorf("refusing to stop the processes of UID %d: not a Mountpoint UID", uid)
	}
	for round := 1; ; round++ {
		uids, err := pm.runner.ProcessUIDs()
		if err != nil {
			return fmt.Errorf("failed to list processes: %w", err)
		}
		if !uids[uid] {
			return nil
		}
		if round > stopRounds {
			return fmt.Errorf("processes of UID %d are still running after %d kills", uid, stopRounds)
		}
		klog.Infof("Killing the processes of UID %d", uid)
		ctx, cancel := context.WithTimeout(context.Background(), killHelperTimeout)
		err = pm.runHelper(ctx, uid, killProcessesArg)
		cancel()
		if err != nil {
			return fmt.Errorf("failed to kill the processes of UID %d: %w", uid, err)
		}
		time.Sleep(stopRoundPause)
		reapOrphansOf(uid)
	}
}

// runAsKillHelper kills every other process of its own UID in this PID namespace and exits, when stopProcessesOf started this
// binary as the kill helper.
func runAsKillHelper(args []string) {
	if len(args) != 2 || args[1] != killProcessesArg {
		return
	}
	// As root the helper keeps CAP_KILL, and outside the range its UID is not a Mountpoint's: kill(-1) would reach other processes.
	if uid := os.Getuid(); uid < mounter.UIDRangeStart || uid > mounter.UIDRangeEnd {
		fmt.Fprintf(os.Stderr, "refusing to kill processes as UID %d: not a Mountpoint UID\n", uid)
		os.Exit(1)
	}
	// Reaches only what this UID may signal, i.e. its own processes in this PID namespace but not PID 1 or this one.
	// Its result says nothing about that UID's processes, so the caller rescans.
	syscall.Kill(-1, syscall.SIGKILL)
	os.Exit(0)
}

// reapOrphansOf reaps the zombies of uid that were reparented to the mounter, which as PID 1 adopts every orphan in its namespace.
func reapOrphansOf(uid uint32) {
	processes, err := listProcesses()
	if err != nil {
		klog.Errorf("Failed to list the processes of UID %d to reap: %v", uid, err)
		return
	}
	for _, p := range processes {
		// Only our own zombie keeps its PID until we reap it; any other PID could be reused by a child Go waits for.
		if p.zombie && p.ppid == os.Getpid() && slices.Contains(p.uids[:], uid) {
			syscall.Wait4(p.pid, nil, syscall.WNOHANG, nil)
		}
	}
}

// writeErrorFile reports a mount failure to the driver, whose waitForMount polls for this file — the
// only reply channel on the otherwise one-way mount socket.
func (pm *ProcessManager) writeErrorFile(mountId string, content []byte) {
	errPath := filepath.Join(pm.commDir, mountId+errorFileExt)
	// TODO(vlaad): write error file atomically (open,write,rename)
	if err := os.WriteFile(errPath, content, errorFilePerm); err != nil {
		klog.Errorf("Failed to write error file for mount %s: %v", mountId, err)
	}
}

// Shutdown sends SIGTERM to all processes and waits for them to exit.
func (pm *ProcessManager) Shutdown() {
	close(pm.shutdown)
	pm.mu.Lock()
	for _, p := range pm.processes {
		klog.Infof("Sending SIGTERM to Mountpoint for mount %s (pid %d)", p.mountId, p.handle.Pid())
		p.handle.Signal(syscall.SIGTERM)
	}
	pm.mu.Unlock()

	pm.wg.Wait()
}

// prefixWriter wraps an io.Writer and prefixes each line with a mount ID.
type prefixWriter struct {
	w      io.Writer
	prefix string
}

func newPrefixWriter(w io.Writer, mountId string) *prefixWriter {
	return &prefixWriter{w: w, prefix: fmt.Sprintf("[%s] ", mountId)}
}

func (pw *prefixWriter) Write(p []byte) (int, error) {
	lines := bytes.Split(p, []byte("\n"))
	for i, line := range lines {
		if len(line) == 0 && i == len(lines)-1 {
			break
		}
		if _, err := pw.w.Write([]byte(pw.prefix)); err != nil {
			return 0, err
		}
		if _, err := pw.w.Write(line); err != nil {
			return 0, err
		}
		if _, err := pw.w.Write([]byte("\n")); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// LogStatusPeriodically logs the number of tracked and actual child processes at the given interval.
func (pm *ProcessManager) LogStatusPeriodically(interval time.Duration) {
	for {
		time.Sleep(interval)

		pm.mu.Lock()
		tracked := len(pm.processes)
		var mountIds []string
		for _, p := range pm.processes {
			mountIds = append(mountIds, p.mountId)
		}
		pm.mu.Unlock()

		actual := countChildProcesses()
		openFDs := countOpenFDs()
		goroutines := runtime.NumGoroutine()
		klog.Infof("Status: tracked=%d actual_children=%d open_fds=%d goroutines=%d memory_limit_strategy=%s share_mib=%d cache_limit_strategy=%s cache_share_mib=%d mounts=%v",
			tracked, actual, openFDs, goroutines, pm.memory.strategy, pm.memory.shareMiB,
			pm.cache.strategy, pm.cache.shareMiB, mountIds)
	}
}

func countOpenFDs() int {
	p, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return -1
	}
	n, err := p.NumFDs()
	if err != nil {
		return -1
	}
	return int(n)
}

func countChildProcesses() int {
	p, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return -1
	}
	children, err := p.Children()
	if err != nil {
		return -1
	}
	return len(children)
}
