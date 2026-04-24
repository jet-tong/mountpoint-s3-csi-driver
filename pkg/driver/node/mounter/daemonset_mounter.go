// daemonset_mounter.go provides the daemonset-mode Mounter implementation.
// It targets the pre-placed Mountpoint DaemonSet Pod on the node instead of
// a controller-spawned per-mount Pod in pod_mounter.go.
//
// The Mount flow is a trimmed version of PodMounter.Mount with the S3PA CRD
// lookup replaced by a label-based pod lookup. Shared helper methods are
// called on a held *PodMounter reference. As the two modes diverge (multi-mount,
// pod-level credentials, etc.), helpers can be extracted into a shared file.
package mounter

import (
	"context"
	"fmt"
	"path/filepath"

	"k8s.io/klog/v2"

	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/cluster"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/driver/node/credentialprovider"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/driver/node/envprovider"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/mountpoint"
	mpmounter "github.com/awslabs/mountpoint-s3-csi-driver/pkg/mountpoint/mounter"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/podmounter/mppod/watcher"
)

// Labels identifying the Mountpoint DaemonSet Pod on this node.
// Must match the labels in charts/.../templates/mountpoint-daemonset.yaml.
// TODO: Use env var to pass it down from chart like MOUNTPOINT_NAMESPACE?
const (
	daemonsetLabelKey   = "app.kubernetes.io/name"
	daemonsetLabelValue = "s3-csi-mountpoint"
)

// DaemonsetMounter implements the Mounter interface for daemonset mode.
// It reuses PodMounter's helper methods for shared mechanics (wait for pod,
// credentials, FUSE mount, socket send, bind mount) while owning its own
// Mount orchestration.
// TODO: Remove PodMounter references and split helper functions out.
type DaemonsetMounter struct {
	// pm provides access to shared helper methods. DaemonsetMounter does NOT
	// call pm.Mount — it has its own Mount implementation.
	pm *PodMounter
}

// NewDaemonsetMounter creates a DaemonsetMounter backed by a PodMounter for
// shared helper access for now. The s3paCache is not needed (no CRD lookup),
// so nil is passed to NewPodMounter.
func NewDaemonsetMounter(
	podWatcher *watcher.Watcher,
	credProvider credentialprovider.ProviderInterface,
	mount *mpmounter.Mounter,
	kubernetesVersion string,
	nodeID string,
	variant cluster.Variant,
) (*DaemonsetMounter, error) {
	pm, err := NewPodMounter(podWatcher, nil, credProvider, mount, nil, nil,
		kubernetesVersion, nodeID, variant)
	if err != nil {
		return nil, err
	}
	return &DaemonsetMounter{pm: pm}, nil
}

// Mount mounts the given bucketName at the target path using the on-node
// Mountpoint DaemonSet Pod.
//
// Flow:
//  1. Find the DaemonSet Pod on this node by label
//  2. Wait for it to be Running
//  3. Write credentials to the Pod's credentials directory
//  4. Perform FUSE mount syscall at source path
//  5. Send mount options (including FUSE fd) to the Pod over Unix socket
//  6. Wait for mount-s3 to start serving, then bind mount source to target
func (dm *DaemonsetMounter) Mount(ctx context.Context, bucketName string, target string, credentialCtx credentialprovider.ProvideContext, args mountpoint.Args, fsGroup string, userEnv envprovider.Environment) error {
	// TODO: add helpMessageFor...() help messages for all errors

	// TODO: fsGroup is not yet used in daemonset mode. The Mounter interface
	// requires it but PodMounter uses it for S3PA lookup filtering which we skip.
	isTargetMountPoint, err := dm.IsMountPoint(target)
	if err != nil {
		err = dm.pm.verifyOrSetupMountTarget(target, err)
		if err != nil {
			return fmt.Errorf("Failed to verify target path can be used as a mount point %q: %w", target, err)
		}
	}

	// Step 1: Find the DaemonSet Pod on this node.
	mpPodName, err := getDaemonsetMounterPodName(dm.pm.podWatcher)
	if err != nil {
		klog.Errorf("Failed to find Mountpoint DaemonSet Pod for %q: %v", target, err)
		return fmt.Errorf("Failed to find Mountpoint DaemonSet Pod: %w", err)
	}

	// Step 2: Wait for the Pod to be Running.
	pod, podPath, err := dm.pm.waitForMountpointPod(ctx, mpPodName)
	if err != nil {
		klog.Errorf("Failed to wait for Mountpoint DaemonSet Pod %q to be ready for %q: %v", mpPodName, target, err)
		return fmt.Errorf("Failed to wait for Mountpoint DaemonSet Pod %q to be ready: %w", mpPodName, err)
	}
	unlockMountpointPod := lockMountpointPod(mpPodName)
	defer unlockMountpointPod()

	// TODO: sanitize VolumeID before using as path component (see credentialprovider's
	// use of k8s.io/utils/strings.EscapeQualifiedName for similar purpose).
	source := filepath.Join(SourceMountDir(dm.pm.kubeletPath), credentialCtx.VolumeID)
	isSourceMountPoint, err := dm.IsMountPoint(source)
	if err != nil {
		err = dm.pm.verifyOrSetupMountTarget(source, err)
		if err != nil {
			return fmt.Errorf("Failed to verify source path can be used as a mount point %q: %w", source, err)
		}
	}

	// Step 3: Provide credentials.
	// For daemonset MVP, driver-level credentials only (no pod-level IRSA role ARN).
	credEnv, authenticationSource, err := dm.pm.provideCredentials(ctx, podPath, string(pod.UID), "", credentialCtx)
	if err != nil {
		klog.Errorf("Failed to provide credentials for %q: %v", source, err)
		return fmt.Errorf("Failed to provide credentials for %q: %w", source, err)
	}

	// Steps 4-6: FUSE mount + send options + wait for mount.
	if !isSourceMountPoint {
		err = dm.pm.mountS3AtSource(ctx, source, pod, podPath, bucketName, credEnv, userEnv, authenticationSource, args)
		if err != nil {
			return fmt.Errorf("Failed to mount at source %q: %w", source, err)
		}
	}

	if isTargetMountPoint {
		klog.V(4).Infof("Target path %q is already mounted. Only refreshed credentials.", target)
		return nil
	}

	// Step 6: Bind mount source to target.
	err = dm.pm.bindMountSyscallWithDefault(source, target)
	if err != nil {
		klog.Errorf("Failed to bind mount %q to target %q: %v", source, target, err)
		return fmt.Errorf("Failed to bind mount %q to target %q: %w", source, target, err)
	}

	klog.V(4).Infof("Created bind mount to target %s from Mountpoint DaemonSet Pod %s at %s", target, pod.Name, source)
	return nil
}

// Unmount unmounts the bind mount at target. For daemonset MVP, delegates to
// PodMounter's Unmount which handles bind-mount removal.
// TODO: source FUSE mount cleanup — after unmounting the bind mount, the source
// FUSE mount at SourceMountDir/{VolumeID} persists until the DS pod restarts.
// For multi-mount support, add explicit source unmount when refcount reaches zero.
func (dm *DaemonsetMounter) Unmount(ctx context.Context, target string, credentialCtx credentialprovider.CleanupContext) error {
	return dm.pm.Unmount(ctx, target, credentialCtx)
}

// IsMountPoint checks whether target is a mountpoint.
func (dm *DaemonsetMounter) IsMountPoint(target string) (bool, error) {
	return dm.pm.IsMountPoint(target)
}

// getDaemonsetMounterPodName returns the name of the on-node Mountpoint DaemonSet Pod
// from the watcher's cache.
func getDaemonsetMounterPodName(w *watcher.Watcher) (string, error) {
	pods, err := w.List()
	if err != nil {
		return "", fmt.Errorf("list pods from watcher: %w", err)
	}
	for _, p := range pods {
		if p.Labels[daemonsetLabelKey] == daemonsetLabelValue {
			return p.Name, nil
		}
	}
	return "", fmt.Errorf("no Mountpoint DaemonSet pod found on this node (label %s=%s)",
		daemonsetLabelKey, daemonsetLabelValue)
}

// Compile-time check that DaemonsetMounter implements Mounter.
var _ Mounter = (*DaemonsetMounter)(nil)
