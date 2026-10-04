package main

// Five ways for the mounter, root without DAC_OVERRIDE, to remove a cache directory another UID owns.
// A prototype for comparison: nothing calls these, and removeCacheVolumeEntry is unchanged.

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"golang.org/x/sys/unix"
)

// maxCacheTreeDepth caps the chown walk, which runs in the mounter, so a deep tree cannot exhaust its memory or descriptors; Mountpoint's cache is 5 deep.
const maxCacheTreeDepth = 16

// removeWithDACOverride removes dir, a cache directory another UID owns, as root.
// Needs CAP_DAC_OVERRIDE added beside CHOWN in mounter-daemonset.yaml. Risk: the mounter can then read every mount's credentials and cache at any time.
func removeWithDACOverride(dir string) error {
	// RemoveAll opens each directory with O_NOFOLLOW, so a symlink a process of the owner plants is removed, not followed.
	return os.RemoveAll(dir)
}

// removeWithChownWalk removes dir, a cache directory another UID owns, by giving each directory under it to uid:gid before reading it.
// The mounter passes root, 0:0. Needs CAP_CHOWN only. Risk: root owns the mount's tree while removing it, in the mounter's own process, and is safe only because every step goes through a no-follow handle.
func removeWithChownWalk(dir string, uid, gid int) error {
	parent, err := unix.Open(filepath.Dir(dir), unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("failed to open %q: %w", filepath.Dir(dir), err)
	}
	defer unix.Close(parent)
	if err := chownWalk(parent, filepath.Base(dir), dir, uid, gid, 1); err != nil {
		return err
	}
	// Every directory is now uid's and 0700, so a process of the old owner can no longer add to the tree.
	return os.RemoveAll(dir)
}

// chownWalk gives the directory name under parentFd, and every directory below it, to uid:gid, each before reading it.
func chownWalk(parentFd int, name, path string, uid, gid, depth int) error {
	if depth > maxCacheTreeDepth {
		return fmt.Errorf("refusing to walk %q: deeper than %d directories", path, maxCacheTreeDepth)
	}
	fd, err := takeDir(parentFd, name, uid, gid)
	if err != nil {
		return fmt.Errorf("failed to take %q: %w", path, err)
	}
	defer unix.Close(fd)
	// "." opens the directory the handle holds, not whatever is at path now.
	dirFd, err := unix.Openat(fd, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("failed to read %q: %w", path, err)
	}
	dir := os.NewFile(uintptr(dirFd), path)
	entries, err := dir.ReadDir(-1)
	dir.Close()
	if err != nil {
		return fmt.Errorf("failed to read %q: %w", path, err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			if err := chownWalk(fd, entry.Name(), filepath.Join(path, entry.Name()), uid, gid, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

// takeDir gives the directory name under parentFd to uid:gid with mode 0700, and returns an O_PATH handle to it.
func takeDir(parentFd int, name string, uid, gid int) (int, error) {
	// O_PATH needs no permission on the directory itself; O_NOFOLLOW with O_DIRECTORY refuses a symlink.
	fd, err := unix.Openat(parentFd, name, unix.O_PATH|unix.O_NOFOLLOW|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	if err := unix.Fchownat(fd, "", uid, gid, unix.AT_EMPTY_PATH); err != nil {
		unix.Close(fd)
		return -1, err
	}
	// fchmod refuses an O_PATH handle and fchmodat2 needs Linux 6.6, so chmod through the handle's /proc link, as glibc does.
	if err := os.Chmod("/proc/self/fd/"+strconv.Itoa(fd), mountCacheDirPerm); err != nil {
		unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

// removeWithHelper removes dir, a cache directory owner owns, as removeCacheVolumeEntry does today: the removal helper empties it as owner, then root removes it.
// The helper runs removalHelperEmptyDir, which chmod-walks only after a permission error. Needs no capability beyond SETUID and SETGID.
// Risk: a second program to review, and its processes run as owner, so a leftover-process check must not count them.
func (pm *ProcessManager) removeWithHelper(dir string, owner uint32) error {
	if err := pm.spawnRemovalHelper(dir, owner); err != nil {
		return err
	}
	return os.Remove(dir)
}

// removalHelperEmptyDirNoWalk is the removal helper's work with no chmod walk, run in place of removalHelperEmptyDir under removeWithHelper.
// Needs no capability beyond SETUID and SETGID. Risk: a subdirectory its owner made unreadable fails the removal closed, so every launch as that UID is refused until the mounter pod is recreated.
func removalHelperEmptyDirNoWalk(dir string) error {
	return removalHelperRemoveContents(dir)
}

// releaseByRenameAside frees entryName, a UID's cache directory, at once: it gives the directory to uid:gid, renames it aside and removes it in the background.
// The mounter passes root, 0:0, and frees the UID as soon as this returns, since a launch as it then creates a fresh directory. Needs CAP_CHOWN only.
// Risk: the old files outlive the UID's release, so it is safe only once no process of the old Mountpoint can hold a descriptor in the tree.
func releaseByRenameAside(cacheDir, entryName string, uid, gid int) (removed <-chan error, err error) {
	parent, err := unix.Open(cacheDir, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open %q: %w", cacheDir, err)
	}
	defer unix.Close(parent)
	// Taken before the rename: a trash directory the UID still owned could be entered by that UID's next Mountpoint.
	fd, err := takeDir(parent, entryName, uid, gid)
	if err != nil {
		return nil, fmt.Errorf("failed to take %q: %w", filepath.Join(cacheDir, entryName), err)
	}
	unix.Close(fd)
	trash := fmt.Sprintf("deleting-%s-%d", entryName, time.Now().UnixNano())
	// Root owns the cache volume, so it can rename an entry within it without any permission on the entry.
	if err := unix.Renameat(parent, entryName, parent, trash); err != nil {
		return nil, fmt.Errorf("failed to rename %q aside: %w", filepath.Join(cacheDir, entryName), err)
	}
	done := make(chan error, 1)
	go func() {
		done <- removeWithChownWalk(filepath.Join(cacheDir, trash), uid, gid)
	}()
	return done, nil
}
