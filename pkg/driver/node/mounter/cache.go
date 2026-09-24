package mounter

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/driver/node/volumecontext"
)

const (
	// CacheVolumeName is the /cache directory mounted inside the mounter pod.
	CacheVolumeName = "cache"

	// The kubelet's per-volume-type subdirectories of a pod's volumes directory.
	emptyDirVolumesSubdir = "kubernetes.io~empty-dir"
	csiVolumesSubdir      = "kubernetes.io~csi"

	// TODO: Remove to use process isolation PR defined permissions.
	cacheDirPerm = fs.FileMode(0770)
)

// CacheType is a node's cache backing, as printed in errors and logs. It distinguishes between Type
// "emptyDir" Medium "" (disk), Type "emptyDir" Medium "Memory" (tmpfs), and Type "ephemeral".
type CacheType string

// CacheType constants identify a volume's cache backing. They are also used in logs and errors.
const (
	CacheNone           CacheType = "no cache"
	CacheEmptyDirDisk   CacheType = "emptyDir"
	CacheEmptyDirMemory CacheType = "emptyDir (medium Memory)"
	CacheEphemeral      CacheType = "ephemeral"
)

// ParseCacheTypeFromPV returns the backing a PV's cache and cacheEmptyDirMedium attributes name.
func ParseCacheTypeFromPV(cacheType, emptyDirMedium string) (CacheType, error) {
	switch cacheType {
	case "":
		return CacheNone, nil
	// TODO(remove-notes): ignoring rather than rejecting {ephemeral, Memory} is deliberate - it is
	// what the code did before this change, and tightening it would break a PV that sets the medium
	// harmlessly. There is a test row for it so it stays a decision, not an accident.
	case volumecontext.CacheTypeEphemeral:
		return CacheEphemeral, nil // the medium names no backing here, so ignore it
	case volumecontext.CacheTypeEmptyDir:
		switch emptyDirMedium {
		case "":
			return CacheEmptyDirDisk, nil
		case string(corev1.StorageMediumMemory):
			return CacheEmptyDirMemory, nil
		default:
			// "HugePages" mediums are rejected from helm chart.
			return CacheNone, fmt.Errorf("unsupported emptyDir medium %q, must be %q or %q",
				emptyDirMedium, "", corev1.StorageMediumMemory)
		}
	default:
		return CacheNone, fmt.Errorf("%q is not a cache type, must be %q or %q",
			cacheType, volumecontext.CacheTypeEmptyDir, volumecontext.CacheTypeEphemeral)
	}
}

// cacheTypeFromPod returns the backing of the current mounter pod's cache volume, or CacheNone if it has none.
func cacheTypeFromPod(pod *corev1.Pod) CacheType {
	for _, v := range pod.Spec.Volumes {
		// Skip other volumes on mounter pod (e.g. commDir)
		if v.Name != CacheVolumeName {
			continue
		}
		switch {
		case v.EmptyDir != nil:
			// Both ""/"Memory" mediums share one path on the node, so identify from spec.
			if v.EmptyDir.Medium == corev1.StorageMediumMemory {
				return CacheEmptyDirMemory
			}
			return CacheEmptyDirDisk
		case v.Ephemeral != nil:
			return CacheEphemeral
		}
	}
	return CacheNone
}

// MountOptionCacheDir returns a mount's cache directory as Mountpoint sees it, which is inside the
// mounter pod. This is the value of the `--cache` argument.
func MountOptionCacheDir(volumeID string) string {
	return filepath.Join("/", CacheVolumeName, volumeID)
}

// createCacheDir creates a mount's cache directory on the node. Mountpoint requires it to exist
// before it starts, as it only creates its own `mountpoint-cache` directory inside it.
func createCacheDir(cacheDir, volumeID string) error {
	if cacheDir == "" {
		return fmt.Errorf("volume %s requests a local cache, but s3-csi-daemonset-mounter has no"+
			" cache volume. Add a daemonsetMounters[0].cache block and restart its pods", volumeID)
	}

	mountCacheDir := filepath.Join(cacheDir, volumeID)
	if err := os.Mkdir(mountCacheDir, cacheDirPerm); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("failed to create cache directory %q for volume %s: %w", mountCacheDir, volumeID, err)
	}
	// Mkdir tolerates EEXIST and Chmod follows symlinks, so without Lstat a symlink planted here
	// by a Mountpoint process (the volume root is group-writable) would redirect the Chmod below.
	if fi, err := os.Lstat(mountCacheDir); err != nil {
		return fmt.Errorf("failed to stat cache directory %q for volume %s: %w", mountCacheDir, volumeID, err)
	} else if !fi.Mode().IsDir() {
		return fmt.Errorf("cache directory %q for volume %s is not a directory (mode %s)", mountCacheDir, volumeID, fi.Mode())
	}
	// Mkdir subtracts the umask, which typically clears the group write bit Mountpoint needs.
	if err := os.Chmod(mountCacheDir, cacheDirPerm); err != nil {
		return fmt.Errorf("failed to set permissions on cache directory %q for volume %s: %w", mountCacheDir, volumeID, err)
	}

	klog.V(4).Infof("DaemonsetMounter: created cache directory %s for volume %s", mountCacheDir, volumeID)
	return nil
}

// removeCacheDir removes a mount's cache directory. Mountpoint removes its own cache when it exits
// cleanly, so this covers the cases it misses, such as being killed.
func removeCacheDir(cacheDir, volumeID string) error {
	// volumeID is a PV name from the kubelet's target path, so these cannot occur in practice.
	// Guarded anyway because this is a recursive delete and the volume root is shared by every
	// mount on the node: `..` would escape it, and a separator would leave the volume entirely.
	if cacheDir == "" || volumeID == "" || volumeID == "." || volumeID == ".." || strings.ContainsRune(volumeID, filepath.Separator) {
		return nil
	}
	return os.RemoveAll(filepath.Join(cacheDir, volumeID))
}
