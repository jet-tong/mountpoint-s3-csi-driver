package mounter

import (
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

// deprecatedVolumeAttributes are v2's settings for a PV's own Mountpoint Pod: its cache volume, resources and service account.
var deprecatedVolumeAttributes = []string{
	volumecontext.CacheEmptyDirMedium,
	volumecontext.CacheEmptyDirSizeLimit,
	volumecontext.CacheEphemeralStorageClassName,
	volumecontext.CacheEphemeralStorageResourceRequest,
	volumecontext.MountpointPodServiceAccountName,
	volumecontext.MountpointContainerResourcesRequestsCpu,
	volumecontext.MountpointContainerResourcesRequestsMemory,
	volumecontext.MountpointContainerResourcesLimitsCpu,
	volumecontext.MountpointContainerResourcesLimitsMemory,
}

// CacheType is the mounter pod's cache type, also shown in logs. It distinguishes between:
// Type "emptyDir" Medium "" (disk), Type "emptyDir" Medium "Memory" (tmpfs), and Type "ephemeral" by its StorageClass.
type CacheType string

// CacheNone is the type of a mounter pod with no cache volume, and of a PV that names no v2 cache type.
const CacheNone CacheType = "no cache"

// mounterPodCacheType returns the type of the cache volume a mounter pod mounts for its Mountpoints, or CacheNone if it has none.
func mounterPodCacheType(pod corev1.Pod) CacheType {
	for _, v := range pod.Spec.Volumes {
		if v.Name != CacheVolumeName {
			continue
		}
		// Named the way a v2 PV asking for it is named, so the two compare.
		switch {
		case v.EmptyDir != nil:
			return cacheTypeFromPV(volumecontext.CacheTypeEmptyDir, string(v.EmptyDir.Medium), "")
		case v.Ephemeral != nil:
			return cacheTypeFromPV(volumecontext.CacheTypeEphemeral, "", ptr.Deref(v.Ephemeral.VolumeClaimTemplate.Spec.StorageClassName, ""))
		}
	}
	return CacheNone
}

// cacheTypeFromPV returns the cache type a PV's `cache`, `cacheEmptyDirMedium` and `cacheEphemeralStorageClassName` attributes name, or CacheNone if it names no v2 type.
func cacheTypeFromPV(pvCache, pvCacheEmptyDirMedium, pvCacheEphemeralStorageClassName string) CacheType {
	switch pvCache {
	case volumecontext.CacheTypeEphemeral:
		return CacheType(pvCache + " (StorageClass " + pvCacheEphemeralStorageClassName + ")")
	case volumecontext.CacheTypeEmptyDir:
		if pvCacheEmptyDirMedium == "" {
			return CacheType(pvCache)
		}
		return CacheType(pvCache + " (medium " + pvCacheEmptyDirMedium + ")")
	}
	return CacheNone
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

// warnDeprecatedCacheSettings warns when a v2 PV's cache type is not this node's, and names every v2 volume attribute it sets.
func warnDeprecatedCacheSettings(volumeCtx map[string]string, volumeID string, mounterCacheType CacheType) {
	pvCacheType := cacheTypeFromPV(volumeCtx[volumecontext.Cache], volumeCtx[volumecontext.CacheEmptyDirMedium],
		volumeCtx[volumecontext.CacheEphemeralStorageClassName])
	if pvCacheType != CacheNone && pvCacheType != mounterCacheType {
		klog.Warningf("NodePublishVolume: volume %s requests an %s cache, but s3-csi-daemonset-mounter on this node provides %s,"+
			" which it uses. Set %q to %q to use this node's cache without this warning.",
			volumeID, pvCacheType, mounterCacheType, volumecontext.Cache, volumecontext.CacheEnabled)
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
	}
}

// maxCacheSizeFromDeprecatedPVSizes returns the --max-cache-size to inject, 95% of a PV's v2 cache size as a final fallback to match v2 behaviour, or "" for none.
func maxCacheSizeFromDeprecatedPVSizes(args mountpoint.Args, volumeCtx map[string]string, volumeID string) string {
	sizeAttr := volumecontext.CacheEmptyDirSizeLimit
	if volumeCtx[sizeAttr] == "" {
		sizeAttr = volumecontext.CacheEphemeralStorageResourceRequest
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
	klog.Warningf("NodePublishVolume: volume %s sets no %s in its mountOptions, so the driver injects %d MiB,"+
		" 95%% of its deprecated %q volume attribute (%q). Under cache.limitStrategy=none, set %s in its"+
		" mountOptions instead; under equalSplit, s3-csi-daemonset-mounter replaces it with an equal share.",
		volumeID, mountpoint.ArgMaxCacheSize, boundMiB, sizeAttr, pvSize, mountpoint.ArgMaxCacheSize)
	return strconv.FormatInt(boundMiB, 10)
}
