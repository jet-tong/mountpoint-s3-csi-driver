package mounter

import (
	"slices"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/driver/node/volumecontext"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/mountpoint"
)

const (
	// CacheVolumeName is the name of the mounter pod's cache volume, which the chart mounts at /cache.
	CacheVolumeName = "cache"
)

// deprecatedVolumeAttributes are v2's settings for a PV's own cache volume.
var deprecatedVolumeAttributes = []string{
	volumecontext.CacheEmptyDirMedium,
	volumecontext.CacheEmptyDirSizeLimit,
	volumecontext.CacheEphemeralStorageClassName,
	volumecontext.CacheEphemeralStorageResourceRequest,
}

// mounterPodCacheVolume returns the volume a mounter pod mounts for its Mountpoints to cache in, or nil if it has none.
func mounterPodCacheVolume(pod corev1.Pod) *corev1.Volume {
	if i := slices.IndexFunc(pod.Spec.Volumes, func(v corev1.Volume) bool { return v.Name == CacheVolumeName }); i >= 0 {
		return &pod.Spec.Volumes[i]
	}
	return nil
}

// validateCacheRequest returns whether this mount caches, or an InvalidArgument error for a cache request it cannot serve.
func validateCacheRequest(args mountpoint.Args, volumeCtx map[string]string, volumeID string, mounterHasCache, newTarget bool) (caches bool, err error) {
	pvCache := volumeCtx[volumecontext.Cache]
	cacheViaMountOptions := args.Has(mountpoint.ArgCache)

	// Reject a cache in both mountOptions and volumeAttributes, even `cache: disabled`, to match v2.
	if cacheViaMountOptions && pvCache != "" {
		return false, status.Error(codes.InvalidArgument,
			"Cache configured with both `mountOptions` and `volumeAttributes`, please remove the deprecated cache configuration in `mountOptions`")
	}

	switch {
	case cacheViaMountOptions:
		if newTarget {
			klog.Warningf("NodePublishVolume: volume %s enables the cache via the deprecated `cache` mount option,"+
				" so its path is ignored. Remove it from mountOptions and set the %q volume attribute to %q instead.",
				volumeID, volumecontext.Cache, volumecontext.CacheEnabled)
		}
	case pvCache == "", strings.EqualFold(pvCache, volumecontext.CacheDisabled):
		return false, nil
	case strings.EqualFold(pvCache, volumecontext.CacheEnabled),
		pvCache == volumecontext.CacheTypeEmptyDir, pvCache == volumecontext.CacheTypeEphemeral:
		// The valid cache values all enable caching.
	default:
		return false, status.Errorf(codes.InvalidArgument,
			"Volume %s sets the %q volume attribute to %q. Set it to %q to use this node's cache, or %q for none.",
			volumeID, volumecontext.Cache, pvCache, volumecontext.CacheEnabled, volumecontext.CacheDisabled)
	}

	if !mounterHasCache {
		return false, status.Errorf(codes.InvalidArgument,
			"Volume %s requests a local cache, but s3-csi-daemonset-mounter has no cache volume."+
				" Add a daemonsetMounters[0].cache block to the Helm values and restart its pods.", volumeID)
	}

	return true, nil
}

// warnDeprecatedCacheSettings warns about a PV's v2 settings: a cache type, emptyDir medium or ephemeral StorageClass other than this node's cache volume, every v2 volume attribute it sets, or else its v2 cache value.
func warnDeprecatedCacheSettings(volumeCtx map[string]string, volumeID string, nodeCache *corev1.Volume) {
	pvCache := volumeCtx[volumecontext.Cache]
	var mismatch string
	switch pvCache {
	case volumecontext.CacheTypeEmptyDir:
		if nodeCache.EmptyDir == nil {
			mismatch = "cache type"
		} else if string(nodeCache.EmptyDir.Medium) != volumeCtx[volumecontext.CacheEmptyDirMedium] {
			mismatch = "emptyDir medium"
		}
	case volumecontext.CacheTypeEphemeral:
		if nodeCache.Ephemeral == nil {
			mismatch = "cache type"
		} else if ptr.Deref(nodeCache.Ephemeral.VolumeClaimTemplate.Spec.StorageClassName, "") != volumeCtx[volumecontext.CacheEphemeralStorageClassName] {
			mismatch = "ephemeral StorageClass"
		}
	}
	if mismatch != "" {
		klog.Warningf("NodePublishVolume: volume %s requests cache %q, but its %s differs from this node's cache volume, which"+
			" s3-csi-daemonset-mounter uses instead. Set %q to %q to use this node's cache without this warning.",
			volumeID, pvCache, mismatch, volumecontext.Cache, volumecontext.CacheEnabled)
	}

	var deprecated []string
	for _, attr := range deprecatedVolumeAttributes {
		if volumeCtx[attr] != "" {
			deprecated = append(deprecated, attr)
		}
	}
	if len(deprecated) > 0 {
		klog.Warningf("NodePublishVolume: volume %s sets the deprecated volume attributes %q. Remove them:"+
			" every mount runs in the mounter pod the daemonsetMounters[0] Helm values configure, and a PV opts in with %q set to %q.",
			volumeID, deprecated, volumecontext.Cache, volumecontext.CacheEnabled)
	} else if mismatch == "" && (pvCache == volumecontext.CacheTypeEmptyDir || pvCache == volumecontext.CacheTypeEphemeral) {
		klog.Warningf("NodePublishVolume: volume %s sets the v2 %q value %q. Set it to %q to use this node's cache without this warning.",
			volumeID, volumecontext.Cache, pvCache, volumecontext.CacheEnabled)
	}
}

// maxCacheSizeFromDeprecatedPVSizes returns the --max-cache-size to inject, 95% of the v2 cache size a PV's cache value names, or "" for none.
func maxCacheSizeFromDeprecatedPVSizes(args mountpoint.Args, volumeCtx map[string]string, volumeID string) string {
	var sizeAttr string
	switch volumeCtx[volumecontext.Cache] {
	case volumecontext.CacheTypeEmptyDir:
		sizeAttr = volumecontext.CacheEmptyDirSizeLimit
	case volumecontext.CacheTypeEphemeral:
		sizeAttr = volumecontext.CacheEphemeralStorageResourceRequest
	default:
		return ""
	}
	pvSize := volumeCtx[sizeAttr]
	// A PV's own --max-cache-size wins, and Mountpoint refuses one without --cache.
	if pvSize == "" || !args.Has(mountpoint.ArgCache) || args.Has(mountpoint.ArgMaxCacheSize) {
		return ""
	}

	quantity, err := resource.ParseQuantity(pvSize)
	if err != nil {
		klog.Warningf("NodePublishVolume: volume %s sets the %q volume attribute to %q, which is not a"+
			" Kubernetes quantity, so the driver injects no %s from it. Under cache.limitStrategy=none, set %s"+
			" in its mountOptions instead.",
			volumeID, sizeAttr, pvSize, mountpoint.ArgMaxCacheSize, mountpoint.ArgMaxCacheSize)
		return ""
	}

	// Match v2.
	boundMiB := int64(float64(quantity.Value()) * 0.95 / (1024 * 1024))
	// This node does not know the mounter's cache.limitStrategy, so the advice names both.
	klog.Warningf("NodePublishVolume: volume %s sets no %s in its mountOptions, so its cache is capped at %d MiB,"+
		" 95%% of its deprecated %q volume attribute (%q). Under cache.limitStrategy=none, set %s in its"+
		" mountOptions instead; under equalSplit, s3-csi-daemonset-mounter replaces it with an equal share.",
		volumeID, mountpoint.ArgMaxCacheSize, boundMiB, sizeAttr, pvSize, mountpoint.ArgMaxCacheSize)
	return strconv.FormatInt(boundMiB, 10)
}
