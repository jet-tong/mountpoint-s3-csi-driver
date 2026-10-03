package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
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

const (
	// Deleting 200,000 small cached objects takes about 35 s, so this stops only a helper that has hung.
	defaultRemovalHelperTimeout = 10 * time.Minute
	defaultReleaseRetryDelay    = time.Second
	maxReleaseRetryDelay        = time.Minute
	// Leaves exit cleanup and its error inside the default 30 s termination grace period.
	defaultShutdownTimeout = 20 * time.Second
)

// ProcessManager tracks and manages Mountpoint child processes.
type ProcessManager struct {
	commDir  string
	cacheDir string        // the cache volume's mount path, or "" when this container has none
	runner   ProcessRunner // interface for spawning processes; substituted in tests
	memory   memoryLimit
	cache    cacheLimit
	chown    func(path string, uid, gid int) error // nil = chown through a no-follow handle; tests are not root, so they record instead

	// Fields rather than constants, so tests can shorten them.
	removalHelperTimeout time.Duration
	releaseRetryDelay    time.Duration
	shutdownTimeout      time.Duration

	mu        sync.Mutex
	processes map[uint32]mountpointProcess // the UID a Mountpoint runs as -> that Mountpoint; one per UID
	releasing map[string]bool              // cache volume entries a release is removing; lost+found and the like tie up no UID
	wg        sync.WaitGroup               // tracks waiter and release goroutines
	stopping  chan struct{}                // closed by Shutdown, so a failing release stops retrying
}

// mountpointProcess is a running Mountpoint and the mount it serves. Its zero value marks a UID as Releasing: its
// Mountpoint has exited, its mount ID is free, and the UID stays taken until what it left behind is removed.
type mountpointProcess struct {
	mountId string
	handle  ProcessHandle
}

func NewProcessManager(commDir, cacheDir string, runner ProcessRunner, memory memoryLimit, cache cacheLimit) *ProcessManager {
	return &ProcessManager{
		commDir:              commDir,
		cacheDir:             cacheDir,
		runner:               runner,
		memory:               memory,
		cache:                cache,
		removalHelperTimeout: defaultRemovalHelperTimeout,
		releaseRetryDelay:    defaultReleaseRetryDelay,
		shutdownTimeout:      defaultShutdownTimeout,
		processes:            make(map[uint32]mountpointProcess),
		releasing:            make(map[string]bool),
		stopping:             make(chan struct{}),
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

// releaseLeftovers marks the UIDs that the cache volume's entries tie up as Releasing, and removes the entries in the background.
func (pm *ProcessManager) releaseLeftovers() error {
	if pm.cacheDir == "" {
		return nil
	}
	entries, err := os.ReadDir(pm.cacheDir)
	if err != nil {
		return fmt.Errorf("failed to list cache volume %q: %w", pm.cacheDir, err)
	}
	pm.mu.Lock()
	defer pm.mu.Unlock()
	var leftovers []leftover
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			klog.Errorf("Failed to find the owner of cache volume entry %s: %v", entry.Name(), err)
			continue
		}
		uids := uidsOf(entry.Name(), info.Sys().(*syscall.Stat_t).Uid)
		if slices.ContainsFunc(uids, func(uid uint32) bool { _, taken := pm.processes[uid]; return taken }) {
			// Two entries tie up one UID only after a bug; releasing both would put two removers on one path.
			klog.Errorf("Leaving cache volume entry %s: one of UIDs %v is already being released", entry.Name(), uids)
			continue
		}
		l := leftover{name: entry.Name(), uids: uids}
		pm.markReleasing(l)
		leftovers = append(leftovers, l)
	}
	if len(leftovers) > 0 {
		pm.startRelease(leftovers...)
	}
	return nil
}

// uidsOf returns the UIDs in the allocator's range that a cache volume entry ties up: its owner and, for a name
// uid-<n>, n.
func uidsOf(name string, owner uint32) []uint32 {
	var uids []uint32
	if inUIDRange(owner) {
		uids = append(uids, owner)
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(name, "uid-"), 10, 32)
	if named := uint32(n); err == nil && mountCacheDirName(named) == name && inUIDRange(named) && named != owner {
		uids = append(uids, named)
	}
	return uids
}

func inUIDRange(uid uint32) bool {
	return uid >= mounter.UIDRangeStart && uid <= mounter.UIDRangeEnd
}

// Launch spawns a Mountpoint process for the given mount and waits for it asynchronously.
// Takes ownership of options.Fd, caller must not close it after calling this function.
// Returns an error if a process with the same mountId or UID is already running, or the UID is still being released.
func (pm *ProcessManager) Launch(mountId string, mountpointPath string, options mountoptions.Options) error {
	fuseDev := os.NewFile(uintptr(options.Fd), "/dev/fuse")
	if fuseDev == nil {
		return fmt.Errorf("invalid FUSE file descriptor %d", options.Fd)
	}

	if !inUIDRange(options.Uid) {
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
	// A scan, as processes is keyed by UID; it holds at most max-volumes-per-node running entries, plus those still being released.
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
		if other.handle == nil {
			klog.Errorf("Refusing to launch mount %s with UID %d: what its previous mount left is still being removed", mountId, options.Uid)
		} else {
			klog.Errorf("Refusing to launch mount %s with UID %d already in use by mount %s", mountId, options.Uid, other.mountId)
		}
		// We rely on csi-node's UID allocator moving its cursor on, so the NodePublishVolume retry gets another UID.
		return fmt.Errorf("refusing to launch mount %s: UID %d is already in use by another mount", mountId, options.Uid)
	}
	// Delete any error files that earlier Mountpoint of this PV wrote after node deletes error files.
	// TODO if we add process to ensure Mountpoint exited, this should not be needed.
	os.Remove(filepath.Join(pm.commDir, mountId+errorFileExt))

	// No running Mountpoint has this UID, so its directory is a leftover; uncached too, as a Mountpoint owns and can enter it.
	if pm.cacheDir != "" {
		err := os.Remove(filepath.Join(pm.cacheDir, dirName))
		if errors.Is(err, syscall.ENOTEMPTY) {
			// Emptying it can take minutes, so it is released in the background and this launch refused as for a busy UID.
			l := leftover{name: dirName, uids: []uint32{options.Uid}}
			pm.markReleasing(l)
			pm.startRelease(l)
			pm.mu.Unlock()
			fuseDev.Close()
			klog.Errorf("Refusing to launch mount %s with UID %d: removing what an earlier mount left in %s", mountId, options.Uid, dirName)
			return fmt.Errorf("refusing to launch mount %s: UID %d is already in use by another mount", mountId, options.Uid)
		}
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
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

	handle, err := pm.runner.Start(cmd)
	if err != nil {
		if cached {
			// Removed in the background like any exit's, so no helper runs under the lock.
			l := leftover{name: dirName, uids: []uint32{options.Uid}}
			pm.markReleasing(l)
			pm.startRelease(l)
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

		pm.mu.Lock()
		// Before freeing mountId, so a relaunch's removal of a stale error file always comes after this write.
		if exitCode != 0 {
			pm.writeErrorFile(mountId, stderr)
		}
		// Frees mountId for a remount; the UID stays taken until release ends, so a relaunch as it cannot race the removal.
		l := leftover{name: dirName, uids: []uint32{options.Uid}}
		pm.markReleasing(l)
		pm.mu.Unlock()

		if exitCode != 0 {
			klog.Errorf("Mountpoint for mount %s exited with code %d", mountId, exitCode)
		} else {
			klog.Infof("Mountpoint for mount %s exited cleanly", mountId)
		}

		pm.release(l)
	}()

	return nil
}

// mountCacheDirName names the cache directory of the Mountpoint running as uid.
func mountCacheDirName(uid uint32) string {
	return fmt.Sprintf("uid-%d", uid)
}

// leftover is a cache volume entry to remove, and the UIDs kept Releasing until it is gone.
type leftover struct {
	name string
	uids []uint32
}

// markReleasing keeps l's UIDs taken, and its entry out of exit cleanup, until release removes it. Callers hold pm.mu.
func (pm *ProcessManager) markReleasing(l leftover) {
	for _, uid := range l.uids {
		pm.processes[uid] = mountpointProcess{}
	}
	pm.releasing[l.name] = true
}

// startRelease runs release in the background, counted so that Shutdown waits for it.
func (pm *ProcessManager) startRelease(leftovers ...leftover) {
	pm.wg.Add(1)
	go func() {
		defer pm.wg.Done()
		pm.release(leftovers...)
	}()
}

// release removes each leftover in turn, then frees its UIDs; a failed one is retried after the others, with backoff,
// until it succeeds or Shutdown starts.
func (pm *ProcessManager) release(leftovers ...leftover) {
	delay := pm.releaseRetryDelay
	for len(leftovers) > 0 {
		l := leftovers[0]
		leftovers = leftovers[1:]
		var err error
		if pm.cacheDir != "" {
			err = pm.removeCacheVolumeEntry(l.name)
		}
		if err != nil {
			klog.Errorf("Failed to remove cache volume entry %s, retrying in %v: %v", l.name, delay, err)
			leftovers = append(leftovers, l)
			select {
			case <-pm.stopping:
				// Its UIDs stay Releasing, so exit cleanup skips the entry and names it.
				return
			case <-time.After(delay):
			}
			delay = min(2*delay, maxReleaseRetryDelay)
			continue
		}
		pm.mu.Lock()
		delete(pm.releasing, l.name)
		for _, uid := range l.uids {
			delete(pm.processes, uid)
		}
		pm.mu.Unlock()
	}
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
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to find this binary to empty %q: %w", dir, err)
	}
	klog.Infof("Emptying cache directory %s as UID %d", dir, uid)
	cmd := exec.Command(self, emptyDirArg, dir)
	// No environment: a process of the same UID could read the helper's from /proc.
	cmd.Env = []string{}
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uid, Gid: uid, Groups: []uint32{}}}
	handle, err := pm.runner.Start(cmd)
	if err != nil {
		return fmt.Errorf("failed to empty cache directory %q as UID %d: %w", dir, uid, err)
	}
	var exitCode int
	var stderr []byte
	waited := make(chan struct{})
	go func() {
		exitCode, stderr = handle.Wait()
		close(waited)
	}()
	select {
	case <-waited:
	case <-time.After(pm.removalHelperTimeout):
		handle.Signal(syscall.SIGKILL)
		// Waited for, so a retry never starts a second helper on this path while a killed one in uninterruptible sleep still runs.
		<-waited
		return fmt.Errorf("failed to empty cache directory %q as UID %d: still running after %v", dir, uid, pm.removalHelperTimeout)
	}
	if exitCode != 0 {
		return fmt.Errorf("failed to empty cache directory %q as UID %d: exit status %d: %s", dir, uid, exitCode, bytes.TrimSpace(stderr))
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

// writeErrorFile reports a mount failure to the driver, whose waitForMount polls for this file — the
// only reply channel on the otherwise one-way mount socket.
func (pm *ProcessManager) writeErrorFile(mountId string, content []byte) {
	errPath := filepath.Join(pm.commDir, mountId+errorFileExt)
	// TODO(vlaad): write error file atomically (open,write,rename)
	if err := os.WriteFile(errPath, content, errorFilePerm); err != nil {
		klog.Errorf("Failed to write error file for mount %s: %v", mountId, err)
	}
}

// Shutdown sends SIGTERM to all Mountpoints, and waits at most shutdownTimeout for them to exit and every release to end.
func (pm *ProcessManager) Shutdown() {
	close(pm.stopping)
	pm.mu.Lock()
	for _, p := range pm.processes {
		// A Releasing UID's Mountpoint has already exited.
		if p.handle == nil {
			continue
		}
		klog.Infof("Sending SIGTERM to Mountpoint for mount %s (pid %d)", p.mountId, p.handle.Pid())
		p.handle.Signal(syscall.SIGTERM)
	}
	pm.mu.Unlock()

	waited := make(chan struct{})
	go func() {
		pm.wg.Wait()
		close(waited)
	}()
	select {
	case <-waited:
	case <-time.After(pm.shutdownTimeout):
		klog.Errorf("Stopped waiting after %v for Mountpoints to exit and leftovers to be removed", pm.shutdownTimeout)
	}
}

// emptyCacheVolume empties the cache volume at exit, skipping entries still in use, and returns an error for anything it
// did not remove.
func (pm *ProcessManager) emptyCacheVolume() error {
	if pm.cacheDir == "" {
		return nil
	}
	entries, err := os.ReadDir(pm.cacheDir)
	if err != nil {
		return fmt.Errorf("failed to list cache volume %q: %w", pm.cacheDir, err)
	}
	pm.mu.Lock()
	releasing := maps.Clone(pm.releasing)
	taken := make(map[uint32]bool, len(pm.processes))
	for uid := range pm.processes {
		taken[uid] = true
	}
	pm.mu.Unlock()
	var errs []error
	leftovers := make(map[string]bool, len(entries))
	for _, entry := range entries {
		leftovers[entry.Name()] = true
		var uids []uint32
		if info, err := entry.Info(); err == nil {
			uids = uidsOf(entry.Name(), info.Sys().(*syscall.Stat_t).Uid)
		}
		// A release Shutdown stopped waiting for, or a Mountpoint still running, may still use it; a second remover would race.
		if releasing[entry.Name()] || slices.ContainsFunc(uids, func(uid uint32) bool { return taken[uid] }) {
			errs = append(errs, fmt.Errorf("skipped cache volume entry %q: a Mountpoint or its removal still holds it, see the mounter log", entry.Name()))
			continue
		}
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
		releasing := 0
		var mountIds []string
		for _, p := range pm.processes {
			if p.handle == nil {
				releasing++
				continue
			}
			mountIds = append(mountIds, p.mountId)
		}
		tracked := len(mountIds)
		pm.mu.Unlock()

		actual := countChildProcesses()
		openFDs := countOpenFDs()
		goroutines := runtime.NumGoroutine()
		klog.Infof("Status: tracked=%d releasing=%d actual_children=%d open_fds=%d goroutines=%d memory_limit_strategy=%s share_mib=%d cache_limit_strategy=%s cache_share_mib=%d mounts=%v",
			tracked, releasing, actual, openFDs, goroutines, pm.memory.strategy, pm.memory.shareMiB,
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
