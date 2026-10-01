package mounter

import (
	"path/filepath"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/driver/node/volumecontext"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/mountpoint"
)

const (
	// CacheVolumeName is the /cache directory mounted inside the mounter pod.
	CacheVolumeName = "cache"
)

// cacheDirForMount returns the cache directory this mount passes as `--cache`, "" when it does not cache,
// or an error for a cache request the mounter pod cannot serve.
func cacheDirForMount(args mountpoint.Args, volumeCtx map[string]string, volumeID string, mounterHasCache bool) (string, error) {
	pvCache := volumeCtx[volumecontext.Cache]
	cacheViaMountOptions := args.Has(mountpoint.ArgCache)

	// Reject a cache in both mountOptions and volumeAttributes, even `cache: disabled`, to match v2.
	if cacheViaMountOptions && pvCache != "" {
		return "", status.Error(codes.InvalidArgument,
			"Cache configured with both `mountOptions` and `volumeAttributes`, please remove the deprecated cache configuration in `mountOptions`")
	}

	switch {
	case cacheViaMountOptions:
		klog.Warningf("NodePublishVolume: volume %s enables the cache via the deprecated `cache` mount option,"+
			" so its path is ignored. Remove it from mountOptions and set the %q volume attribute to %q instead.",
			volumeID, volumecontext.Cache, volumecontext.CacheEnabled)
	case pvCache == "", strings.EqualFold(pvCache, volumecontext.CacheDisabled):
		return "", nil
	case strings.EqualFold(pvCache, volumecontext.CacheEnabled),
		pvCache == volumecontext.CacheTypeEmptyDir, pvCache == volumecontext.CacheTypeEphemeral:
		// The valid cache values all enable caching.
	default:
		return "", status.Errorf(codes.InvalidArgument,
			"Volume %s sets the %q volume attribute to %q. Set it to %q to use this node's cache, or %q for none.",
			volumeID, volumecontext.Cache, pvCache, volumecontext.CacheEnabled, volumecontext.CacheDisabled)
	}

	if !mounterHasCache {
		return "", status.Errorf(codes.InvalidArgument,
			"Volume %s requests a local cache, but s3-csi-daemonset-mounter has no cache volume."+
				" Add a daemonsetMounters[0].cache block to the Helm values and restart its pods.", volumeID)
	}

	return MountOptionCacheDir(volumeID), nil
}

// MountOptionCacheDir returns a mount's cache directory as Mountpoint sees it, passed to `--cache`.
func MountOptionCacheDir(volumeID string) string {
	return filepath.Join("/", CacheVolumeName, volumeID)
}
