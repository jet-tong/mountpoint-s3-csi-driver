package mounter

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/util/testutil/assert"
)

func TestCommDirFor(t *testing.T) {
	assert.Equals(t, "/var/lib/kubelet/pods/uid/volumes/kubernetes.io~empty-dir/comm",
		commDirForMounterDir("/var/lib/kubelet/pods/uid/volumes"))
}

func TestCacheDirFor(t *testing.T) {
	t.Run("emptyDir cache volume", func(t *testing.T) {
		mounterDir := t.TempDir()
		want := filepath.Join(mounterDir, emptyDirVolumesSubdir, CacheVolumeName)
		assert.NoError(t, os.MkdirAll(want, 0777))

		got, err := cacheDirForMounterDir(mounterDir)
		assert.NoError(t, err)
		assert.Equals(t, want, got)
	})

	t.Run("ephemeral cache volume is found under its bound PV name", func(t *testing.T) {
		mounterDir := t.TempDir()
		// The kubelet names a CSI volume's directory after the bound PersistentVolume, which is
		// generated and cannot be derived, so this must be discovered rather than constructed.
		// Any bound PV name, not just the `pvc-<uid>` shape dynamic provisioning generates.
		want := filepath.Join(mounterDir, csiVolumesSubdir, "my-static-cache-pv", "mount")
		assert.NoError(t, os.MkdirAll(want, 0777))

		got, err := cacheDirForMounterDir(mounterDir)
		assert.NoError(t, err)
		assert.Equals(t, want, got)
	})

	t.Run("empty mounter dir is not a cache volume", func(t *testing.T) {
		// cleanupMount can reach here with an unset MounterDir, and a relative-path probe from a
		// privileged process is not something to leave to chance.
		got, err := cacheDirForMounterDir("")
		assert.NoError(t, err)
		assert.Equals(t, "", got)
	})

	t.Run("a file where an ephemeral cache volume should be is not a cache volume", func(t *testing.T) {
		mounterDir := t.TempDir()
		assert.NoError(t, os.MkdirAll(filepath.Join(mounterDir, csiVolumesSubdir, "pvc-aaa"), 0777))
		assert.NoError(t, os.WriteFile(filepath.Join(mounterDir, csiVolumesSubdir, "pvc-aaa", "mount"), []byte("x"), 0600))

		_, err := cacheDirForMounterDir(mounterDir)
		assert.Equals(t, true, err != nil)
	})

	t.Run("no cache volume, i.e. caching disabled", func(t *testing.T) {
		mounterDir := t.TempDir()
		// The comm volume always exists and must not be mistaken for a cache volume.
		assert.NoError(t, os.MkdirAll(commDirForMounterDir(mounterDir), 0777))

		got, err := cacheDirForMounterDir(mounterDir)
		assert.NoError(t, err)
		assert.Equals(t, "", got)
	})

	t.Run("emptyDir wins over a CSI volume", func(t *testing.T) {
		// Only one can be the cache volume, but a future pod spec could carry an unrelated CSI
		// volume. The emptyDir path is exact, so it is preferred over the glob.
		mounterDir := t.TempDir()
		want := filepath.Join(mounterDir, emptyDirVolumesSubdir, CacheVolumeName)
		assert.NoError(t, os.MkdirAll(want, 0777))
		assert.NoError(t, os.MkdirAll(filepath.Join(mounterDir, csiVolumesSubdir, "pvc-other", "mount"), 0777))

		got, err := cacheDirForMounterDir(mounterDir)
		assert.NoError(t, err)
		assert.Equals(t, want, got)
	})

	t.Run("more than one CSI volume is an error rather than an arbitrary pick", func(t *testing.T) {
		mounterDir := t.TempDir()
		assert.NoError(t, os.MkdirAll(filepath.Join(mounterDir, csiVolumesSubdir, "pvc-aaa", "mount"), 0777))
		assert.NoError(t, os.MkdirAll(filepath.Join(mounterDir, csiVolumesSubdir, "pvc-bbb", "mount"), 0777))

		_, err := cacheDirForMounterDir(mounterDir)
		assert.Equals(t, true, err != nil)
	})

	t.Run("a file where the cache volume should be is not a cache volume", func(t *testing.T) {
		mounterDir := t.TempDir()
		assert.NoError(t, os.MkdirAll(filepath.Join(mounterDir, emptyDirVolumesSubdir), 0777))
		f := filepath.Join(mounterDir, emptyDirVolumesSubdir, CacheVolumeName)
		assert.NoError(t, os.WriteFile(f, []byte("not a directory"), 0600))

		got, err := cacheDirForMounterDir(mounterDir)
		assert.NoError(t, err)
		assert.Equals(t, "", got)
	})
}

func TestCreateAndRemoveCacheDir(t *testing.T) {
	t.Run("creates the mount's directory group-writable", func(t *testing.T) {
		cacheDir := t.TempDir()
		assert.NoError(t, createCacheDir(cacheDir, "s3-pv"))

		fi, err := os.Stat(filepath.Join(cacheDir, "s3-pv"))
		assert.NoError(t, err)
		assert.Equals(t, cacheDirPerm, fi.Mode().Perm())
	})

	t.Run("sets the mode on a directory that already exists with the wrong one", func(t *testing.T) {
		// The explicit Chmod is load-bearing - Mkdir subtracts the umask, clearing the group write
		// bit Mountpoint needs to create `mountpoint-cache`. Asserting only after a fresh Mkdir
		// would pass under umask 0 with the Chmod deleted, so start from a known-wrong mode.
		cacheDir := t.TempDir()
		mountCacheDir := filepath.Join(cacheDir, "s3-pv")
		assert.NoError(t, os.Mkdir(mountCacheDir, 0700))

		assert.NoError(t, createCacheDir(cacheDir, "s3-pv"))

		fi, err := os.Stat(mountCacheDir)
		assert.NoError(t, err)
		assert.Equals(t, cacheDirPerm, fi.Mode().Perm())
	})

	t.Run("is idempotent, so a re-publish does not fail", func(t *testing.T) {
		cacheDir := t.TempDir()
		assert.NoError(t, createCacheDir(cacheDir, "s3-pv"))
		assert.NoError(t, createCacheDir(cacheDir, "s3-pv"))
	})

	t.Run("refuses a symlink rather than chmod-ing its target", func(t *testing.T) {
		cacheDir := t.TempDir()
		victim := t.TempDir()
		assert.NoError(t, os.Symlink(victim, filepath.Join(cacheDir, "s3-pv")))

		err := createCacheDir(cacheDir, "s3-pv")
		assert.Equals(t, true, err != nil)

		// The symlink's target must keep its own mode - the whole point of the Lstat check.
		fi, statErr := os.Stat(victim)
		assert.NoError(t, statErr)
		assert.Equals(t, true, fi.Mode().Perm() != cacheDirPerm)
	})

	t.Run("fails when the mounter has no cache volume", func(t *testing.T) {
		err := createCacheDir("", "s3-pv")
		assert.Equals(t, true, err != nil)
	})

	t.Run("removes the mount's directory and its contents", func(t *testing.T) {
		cacheDir := t.TempDir()
		assert.NoError(t, createCacheDir(cacheDir, "s3-pv"))
		assert.NoError(t, os.WriteFile(filepath.Join(cacheDir, "s3-pv", "block"), []byte("x"), 0600))

		assert.NoError(t, removeCacheDir(cacheDir, "s3-pv"))
		_, err := os.Stat(filepath.Join(cacheDir, "s3-pv"))
		assert.Equals(t, true, os.IsNotExist(err))
	})

	t.Run("is a no-op for a mount without a cache, and never removes the volume root", func(t *testing.T) {
		cacheDir := t.TempDir()
		assert.NoError(t, removeCacheDir("", "s3-pv"))
		assert.NoError(t, removeCacheDir(cacheDir, ""))

		// None of these may turn this into a delete of the shared volume root or its parent.
		assert.NoError(t, removeCacheDir(cacheDir, "."))
		assert.NoError(t, removeCacheDir(cacheDir, ".."))
		assert.NoError(t, removeCacheDir(cacheDir, "a/b"))

		_, err := os.Stat(cacheDir)
		assert.NoError(t, err)
		_, err = os.Stat(filepath.Dir(cacheDir))
		assert.NoError(t, err)
	})
}
