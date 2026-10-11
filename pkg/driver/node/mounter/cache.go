package mounter

import (
	"fmt"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"

	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/driver/node/volumecontext"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/mountpoint"
)

const (
	// CacheVolumeName is the name of the mounter pod's cache volume, which the chart mounts at /cache.
	CacheVolumeName = "cache"
)

// v2VolumeAttributes configure a v2 PV's own Mountpoint Pod, which v3 does not create.
var v2VolumeAttributes = []string{
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

// mounterPodHasCacheVolume reports whether a mounter pod mounts the cache volume its Mountpoints cache in.
// TODO: we may need to expand this function to get exact cache type for v2 style PV compatibility validations.
func mounterPodHasCacheVolume(pod corev1.Pod) bool {
	return slices.ContainsFunc(pod.Spec.Volumes, func(v corev1.Volume) bool { return v.Name == CacheVolumeName })
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

// v2CacheSizeFor returns the cache size a PV's v2 volume attributes set, which caps its cache under
// cache.limitStrategy=none, and on a new target warns about the v2 volume attributes v3 handles differently.
func v2CacheSizeFor(volumeCtx map[string]string, volumeID string, caches, newTarget bool) (sizeBytes int64) {
	var notes, attrs []string
	if pvCache := volumeCtx[volumecontext.Cache]; pvCache == volumecontext.CacheTypeEmptyDir || pvCache == volumecontext.CacheTypeEphemeral {
		notes = append(notes, fmt.Sprintf("its %q volume attribute %q takes this node's cache, whose type, medium and StorageClass daemonsetMounters[0].cache sets in the Helm values",
			volumecontext.Cache, pvCache))
	}
	for _, attr := range v2VolumeAttributes {
		if volumeCtx[attr] != "" {
			attrs = append(attrs, attr)
		}
	}
	if len(attrs) > 0 {
		notes = append(notes, "it sets "+strings.Join(attrs, ", ")+" for v2's own Mountpoint Pod, which v3 does not create")
	}

	sizeAttr := volumecontext.CacheEmptyDirSizeLimit
	if volumeCtx[sizeAttr] == "" {
		sizeAttr = volumecontext.CacheEphemeralStorageResourceRequest
	}
	if size := volumeCtx[sizeAttr]; caches && size != "" {
		quantity, err := resource.ParseQuantity(size)
		if err != nil || quantity.Sign() <= 0 {
			notes = append(notes, fmt.Sprintf("its %s of %q is not a positive Kubernetes quantity, so it caps nothing", sizeAttr, size))
		} else {
			sizeBytes = quantity.Value()
			notes = append(notes, fmt.Sprintf("its %s of %s still caps its cache at 95%% under cache.limitStrategy=none, unless its"+
				" mountOptions set %s, which a recreated PV then needs under none to keep that cap", sizeAttr, size, mountpoint.ArgMaxCacheSize))
		}
	}

	if newTarget && len(notes) > 0 {
		klog.Warningf("NodePublishVolume: volume %s sets v2 volume attributes that v3 handles differently: %s."+
			" Recreate the PV without them, with the %q volume attribute %q if it caches, once you will not roll back to v2.",
			volumeID, strings.Join(notes, "; "), volumecontext.Cache, volumecontext.CacheEnabled)
	}
	return sizeBytes
}
