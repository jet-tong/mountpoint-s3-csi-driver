package main

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/util/testutil/assert"
)

// otherUID owns the leftover in the root-only tests, as a Mountpoint would.
const otherUID = 65536

// newLeftoverCache returns a cache volume holding uid-65536 as a killed Mountpoint running as owner would leave it, with one subdirectory it made unreadable.
// An owner other than the test user needs root.
func newLeftoverCache(t *testing.T, owner int) (cacheDir, dir string) {
	t.Helper()
	cacheDir = t.TempDir()
	dir = filepath.Join(cacheDir, "uid-65536")
	blocks := filepath.Join(dir, "mountpoint-cache", "V2", "ab")
	locked := filepath.Join(blocks, "locked")
	var deepestFirst []string
	for _, sub := range []string{"cd", "locked"} {
		block := filepath.Join(blocks, sub, "0000000000")
		assert.NoError(t, os.MkdirAll(filepath.Dir(block), 0700))
		assert.NoError(t, os.WriteFile(block, []byte("x"), 0600))
		deepestFirst = append(deepestFirst, block, filepath.Dir(block))
	}
	deepestFirst = append(deepestFirst, blocks, filepath.Dir(blocks), filepath.Join(dir, "mountpoint-cache"), dir)
	// Deepest first: root without CAP_DAC_OVERRIDE cannot enter a directory once it has given it away.
	for _, path := range deepestFirst {
		// Only the owner may chmod without CAP_FOWNER, so lock it while it is still ours, after its block is given away.
		if path == locked {
			assert.NoError(t, os.Chmod(locked, 0))
		}
		if owner != os.Getuid() {
			assert.NoError(t, os.Lchown(path, owner, owner))
		}
	}
	// So t.TempDir's cleanup can remove what a failing method leaves.
	t.Cleanup(func() { os.Chmod(locked, 0700) })
	return cacheDir, dir
}

// plantSymlink links dir/link to a read-only directory outside it, and returns that directory.
func plantSymlink(t *testing.T, dir string) string {
	t.Helper()
	outside := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(outside, "keep"), []byte("x"), 0600))
	assert.NoError(t, os.Chmod(outside, 0500))
	t.Cleanup(func() { os.Chmod(outside, 0700) })
	assert.NoError(t, os.Symlink(outside, filepath.Join(dir, "link")))
	return outside
}

func assertUntouched(t *testing.T, outside string) {
	t.Helper()
	fi, err := os.Stat(outside)
	assert.NoError(t, err)
	assert.Equals(t, fs.FileMode(0500), fi.Mode().Perm())
	_, err = os.Stat(filepath.Join(outside, "keep"))
	assert.NoError(t, err)
}

// supplementaryGroup returns a group of the test user other than its primary one, so an unprivileged chown changes something.
func supplementaryGroup(t *testing.T) int {
	t.Helper()
	groups, err := os.Getgroups()
	assert.NoError(t, err)
	for _, g := range groups {
		if g != os.Getgid() {
			return g
		}
	}
	t.Skip("the test user has no supplementary group to chown to")
	return 0
}

// rootOnlyTests are the tests that run as root; the rest of this package's tests assume they are not root.
const rootOnlyTests = "TestRemoveWithDACOverride|TestRemoveWithChownWalk|TestTakeDir|TestReleaseByRenameAside"

// newRootOnlyLeftover returns a leftover otherUID owns, skipping unless the test runs as root with CAP_DAC_OVERRIDE, or with neither it nor CAP_DAC_READ_SEARCH.
func newRootOnlyLeftover(t *testing.T, dacOverride bool) (cacheDir, dir string) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skipf("needs root, to give the tree to another UID; run -run '%s' as root, with and without CAP_DAC_OVERRIDE (setpriv --bounding-set)", rootOnlyTests)
	}
	header := unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3}
	var caps [2]unix.CapUserData
	assert.NoError(t, unix.Capget(&header, &caps[0]))
	hasDACOverride := caps[0].Effective&(1<<unix.CAP_DAC_OVERRIDE) != 0
	hasDACReadSearch := caps[0].Effective&(1<<unix.CAP_DAC_READ_SEARCH) != 0
	// Checked before building the tree, which root without these could not clean up after a skip.
	if dacOverride && !hasDACOverride {
		t.Skip("needs root with CAP_DAC_OVERRIDE")
	}
	if !dacOverride && (hasDACOverride || hasDACReadSearch) {
		t.Skip("needs root without CAP_DAC_OVERRIDE or CAP_DAC_READ_SEARCH, which would hide a walk that does not take each directory first")
	}
	return newLeftoverCache(t, otherUID)
}

func TestRemoveWithDACOverride(t *testing.T) {
	t.Run("removes a tree its owner can read, and a planted symlink but not its target", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "uid-65536")
		assert.NoError(t, os.MkdirAll(filepath.Join(dir, "mountpoint-cache", "V2", "ab", "cd"), 0700))
		assert.NoError(t, os.WriteFile(filepath.Join(dir, "mountpoint-cache", "V2", "ab", "cd", "0000000000"), []byte("x"), 0600))
		outside := plantSymlink(t, dir)

		assert.NoError(t, removeWithDACOverride(dir))

		assertNotExist(t, dir)
		assertUntouched(t, outside)
	})

	t.Run("removes another UID's tree, unreadable subdirectory included (root with CAP_DAC_OVERRIDE only)", func(t *testing.T) {
		_, dir := newRootOnlyLeftover(t, true)

		assert.NoError(t, removeWithDACOverride(dir))

		assertNotExist(t, dir)
	})
}

func TestRemoveWithChownWalk(t *testing.T) {
	t.Run("gives every directory to the new owner before reading it", func(t *testing.T) {
		gid := supplementaryGroup(t)
		_, dir := newLeftoverCache(t, os.Getuid())
		parentFd, err := unix.Open(filepath.Dir(dir), unix.O_PATH|unix.O_DIRECTORY, 0)
		assert.NoError(t, err)
		defer unix.Close(parentFd)

		// The walk alone, as the removal would delete the evidence; the locked subdirectory can only be read if it was taken first.
		assert.NoError(t, chownWalk(parentFd, filepath.Base(dir), dir, os.Getuid(), gid, 1))

		assert.NoError(t, filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			assert.NoError(t, err)
			if d.IsDir() {
				fi, err := d.Info()
				assert.NoError(t, err)
				assert.Equals(t, uint32(gid), fi.Sys().(*syscall.Stat_t).Gid)
				assert.Equals(t, fs.FileMode(0700), fi.Mode().Perm())
			}
			return nil
		}))
	})

	t.Run("removes the tree, and a planted symlink but not its target", func(t *testing.T) {
		_, dir := newLeftoverCache(t, os.Getuid())
		outside := plantSymlink(t, dir)

		assert.NoError(t, removeWithChownWalk(dir, os.Getuid(), os.Getgid()))

		assertNotExist(t, dir)
		assertUntouched(t, outside)
	})

	t.Run("refuses a tree deeper than a Mountpoint cache can be, and leaves it", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "uid-65536")
		deepest := dir
		for range maxCacheTreeDepth {
			deepest = filepath.Join(deepest, "d")
		}
		assert.NoError(t, os.MkdirAll(deepest, 0700))

		if err := removeWithChownWalk(dir, os.Getuid(), os.Getgid()); err == nil {
			t.Fatal("expected removeWithChownWalk to refuse a tree deeper than maxCacheTreeDepth")
		}

		_, err := os.Lstat(deepest)
		assert.NoError(t, err)
	})

	t.Run("gives another UID's tree to root and removes it (root without CAP_DAC_OVERRIDE only)", func(t *testing.T) {
		_, dir := newRootOnlyLeftover(t, false)

		assert.NoError(t, removeWithChownWalk(dir, 0, 0))

		assertNotExist(t, dir)
	})
}

func TestTakeDir(t *testing.T) {
	t.Run("gives a directory its owner made unreadable to the new owner, opened up to 0700", func(t *testing.T) {
		gid := supplementaryGroup(t)
		parent := t.TempDir()
		locked := filepath.Join(parent, "locked")
		assert.NoError(t, os.Mkdir(locked, 0))
		t.Cleanup(func() { os.Chmod(locked, 0700) })
		parentFd, err := unix.Open(parent, unix.O_PATH|unix.O_DIRECTORY, 0)
		assert.NoError(t, err)
		defer unix.Close(parentFd)

		fd, err := takeDir(parentFd, "locked", os.Getuid(), gid)
		assert.NoError(t, err)
		unix.Close(fd)

		fi, err := os.Lstat(locked)
		assert.NoError(t, err)
		assert.Equals(t, fs.FileMode(0700), fi.Mode().Perm())
		assert.Equals(t, uint32(gid), fi.Sys().(*syscall.Stat_t).Gid)
	})

	t.Run("refuses a symlink and leaves its target alone", func(t *testing.T) {
		parent := t.TempDir()
		outside := plantSymlink(t, parent)
		parentFd, err := unix.Open(parent, unix.O_PATH|unix.O_DIRECTORY, 0)
		assert.NoError(t, err)
		defer unix.Close(parentFd)

		_, err = takeDir(parentFd, "link", os.Getuid(), os.Getgid())

		assert.Equals(t, true, errors.Is(err, unix.ENOTDIR))
		assertUntouched(t, outside)
	})
}

func TestProcessManager_RemoveWithHelper(t *testing.T) {
	t.Run("empties the directory as its owner with the removal helper, then removes it", func(t *testing.T) {
		fr := &fakeProcessRunner{}
		pm, _ := newProcessManagerWithCache(t, fr, cacheLimit{strategy: cacheLimitNone})
		_, dir := newLeftoverCache(t, os.Getuid())
		owner := uint32(os.Getuid())

		assert.NoError(t, pm.removeWithHelper(dir, owner))

		assertNotExist(t, dir)
		// The fake runner runs removalHelperEmptyDir in-process as the test user, which is the helper's real setting: it runs as the owner.
		assert.Equals(t, 1, len(fr.helperCmds))
		// Note: the helper's credentials are tested in TestProcessManager_RemoveCacheVolumeEntry.
	})
}

func TestRemovalHelperEmptyDirNoWalk(t *testing.T) {
	t.Run("removes everything inside a directory its owner can read, but not the directory", func(t *testing.T) {
		dir := t.TempDir()
		assert.NoError(t, os.MkdirAll(filepath.Join(dir, "mountpoint-cache", "V2", "ab", "cd"), 0700))
		assert.NoError(t, os.WriteFile(filepath.Join(dir, "mountpoint-cache", "V2", "ab", "cd", "0000000000"), []byte("x"), 0600))

		assert.NoError(t, removalHelperEmptyDirNoWalk(dir))

		entries, err := os.ReadDir(dir)
		assert.NoError(t, err)
		assert.Equals(t, 0, len(entries))
	})

	t.Run("fails closed on a subdirectory its owner made unreadable", func(t *testing.T) {
		_, dir := newLeftoverCache(t, os.Getuid())

		err := removalHelperEmptyDirNoWalk(dir)

		assert.Equals(t, true, errors.Is(err, fs.ErrPermission))
		_, err = os.Lstat(filepath.Join(dir, "mountpoint-cache", "V2", "ab", "locked"))
		assert.NoError(t, err)
	})
}

func TestReleaseByRenameAside(t *testing.T) {
	t.Run("frees the directory's name at once and removes the renamed tree in the background", func(t *testing.T) {
		cacheDir, dir := newLeftoverCache(t, os.Getuid())

		removed, err := releaseByRenameAside(cacheDir, filepath.Base(dir), os.Getuid(), os.Getgid())
		assert.NoError(t, err)

		// The UID's next Mountpoint can have its directory while the old tree is still being removed.
		assert.NoError(t, os.Mkdir(dir, 0700))
		assert.NoError(t, <-removed)
		entries, err := os.ReadDir(cacheDir)
		assert.NoError(t, err)
		assert.Equals(t, 1, len(entries))
		assert.Equals(t, filepath.Base(dir), entries[0].Name())
	})

	t.Run("refuses a symlink planted at the directory's name", func(t *testing.T) {
		cacheDir := t.TempDir()
		outside := plantSymlink(t, cacheDir)

		if _, err := releaseByRenameAside(cacheDir, "link", os.Getuid(), os.Getgid()); err == nil {
			t.Fatal("expected releaseByRenameAside to refuse a symlink")
		}

		assertUntouched(t, outside)
		_, err := os.Lstat(filepath.Join(cacheDir, "link"))
		assert.NoError(t, err)
	})

	t.Run("renames another UID's tree aside and removes it (root without CAP_DAC_OVERRIDE only)", func(t *testing.T) {
		cacheDir, dir := newRootOnlyLeftover(t, false)

		removed, err := releaseByRenameAside(cacheDir, filepath.Base(dir), 0, 0)
		assert.NoError(t, err)

		assert.NoError(t, <-removed)
		entries, err := os.ReadDir(cacheDir)
		assert.NoError(t, err)
		assert.Equals(t, 0, len(entries))
	})
}
