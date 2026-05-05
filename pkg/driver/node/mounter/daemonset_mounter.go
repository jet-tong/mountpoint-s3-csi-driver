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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	k8sstrings "k8s.io/utils/strings"

	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/cluster"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/driver/node/credentialprovider"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/driver/node/envprovider"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/mountpoint"
	mpmounter "github.com/awslabs/mountpoint-s3-csi-driver/pkg/mountpoint/mounter"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/podmounter/mppod/watcher"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/util"
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
	pm *PodMounter

	podWatcher  *watcher.Watcher
	mount       *mpmounter.Mounter
	kubeletPath string
	mu          sync.Mutex
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
	return &DaemonsetMounter{
		pm:          pm,
		podWatcher:  podWatcher,
		mount:       mount,
		kubeletPath: util.ContainerKubeletPath(),
	}, nil
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

	// Step 1-2: Find and get pod
	pod, podPath, err := dm.getDaemonsetMounterPodWithRetry(ctx)
	if err != nil {
		klog.Errorf("Failed to find running Mountpoint DaemonSet Pod for %q: %v", target, err)
		return fmt.Errorf("Failed to find running Mountpoint DaemonSet Pod: %w", err)
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Sanitize VolumeID before using as a path component
	source := dm.sourcePath(credentialCtx.VolumeID)
	isSourceMountPoint, err := dm.IsMountPoint(source)
	if err != nil {
		err = dm.pm.verifyOrSetupMountTarget(source, err)
		if err != nil {
			return fmt.Errorf("Failed to verify source path can be used as a mount point %q: %w", source, err)
		}
	}

	// Step 3: Credentials.
	credEnv := envprovider.Environment{}
	authenticationSource := credentialprovider.AuthenticationSourceDriver

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
	err = dm.mount.BindMount(source, target)
	if err != nil {
		klog.Errorf("Failed to bind mount %q to target %q: %v", source, target, err)
		return fmt.Errorf("Failed to bind mount %q to target %q: %w", source, target, err)
	}

	klog.V(4).Infof("Created bind mount to target %s from Mountpoint DaemonSet Pod %s at %s", target, pod.Name, source)
	return nil
}

// Unmount unmounts both the bind mount at target and the source FUSE mount.
// Unlike pod-mode which splits unmount across PodMounter (bind mount) and
// PodUnmounter (source FUSE, triggered by controller).
func (dm *DaemonsetMounter) Unmount(ctx context.Context, target string, credentialCtx credentialprovider.CleanupContext) error {
	// 1. Unmount the bind mount at target
	err := dm.mount.Unmount(target)
	// TODO: Handle systemd logic like PodMounter.Unmount
	if err != nil {
		klog.Errorf("Failed to unmount %q: %v", target, err)
		return fmt.Errorf("Failed to unmount %q: %w", target, err)
	}

	// Lock serializes all mount/unmount operations on source paths.
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// 2. Unmount the source FUSE mount.
	// TODO: continue adapting this based on PodUnmounter.unmountAndRemoveMountpointSource(): (or extract common parts elsewhere)
	source := dm.sourcePath(credentialCtx.VolumeID)
	isMountpoint, err := dm.mount.CheckMountpoint(source)
	isCorruptedMountpoint := err != nil && dm.mount.IsMountpointCorrupted(err)
	if err != nil && errors.Is(err, fs.ErrNotExist) {
		// Target does not exist, nothing to do
		return nil
	} else if err != nil && !isCorruptedMountpoint {
		return fmt.Errorf("failed to check source mountpoint %q: %w", source, err)
	}
	// TODO: PodUnmounter.waitUntilMountpointIsUnused - wait till all references to Mountpoint at source is gone, not needed without pod sharing.
	// TODO: PodUnmounter.waitUntilMountpointIsUnmounted like in PodUnmounter - wait till Mountpoint at source is unmounted

	if isMountpoint || isCorruptedMountpoint {
		if err := dm.mount.Unmount(source); err != nil {
			klog.Errorf("failed to unmount source Mountpoint %q: %v", source, err)
			return fmt.Errorf("failed to unmount source Mountpoint %q: %w", source, err)
		}
		klog.Infof("Unmounted source FUSE mount %s", source)
	}

	// Now we know there is no Mountpoint at `source`, and it should be a regular directory. Let's remove it.
	if err := os.Remove(source); err != nil {
		return fmt.Errorf("failed to remove source directory of source Mountpoint %q: %w", source, err)
	}

	return nil
}

// IsMountPoint checks whether target is a mountpoint.
func (dm *DaemonsetMounter) IsMountPoint(target string) (bool, error) {
	return dm.mount.CheckMountpoint(target)
}

// podPath returns `pod`'s basepath inside kubelet's path.
func (dm *DaemonsetMounter) podPath(podUID string) string {
	return filepath.Join(dm.kubeletPath, "pods", podUID)
}

// sourcePath returns the source mount directory for the given volumeID.
func (dm *DaemonsetMounter) sourcePath(volumeID string) string {
	return filepath.Join(SourceMountDir(dm.kubeletPath), k8sstrings.EscapeQualifiedName(volumeID))
}

// getDaemonsetMounterPodWithRetry finds the running Mountpoint DaemonSet Pod on this
// node, retrying until the pod appears in the watcher cache or the context expires.
func (dm *DaemonsetMounter) getDaemonsetMounterPodWithRetry(ctx context.Context) (*corev1.Pod, string, error) {
	ctx, cancel := context.WithTimeout(ctx, mountpointPodReadinessWaitDuration)
	defer cancel()

	for {
		pods, err := dm.podWatcher.List()
		if err != nil {
			return nil, "", fmt.Errorf("list pods from watcher: %w", err)
		}
		for i := range pods {
			if pods[i].Labels[daemonsetLabelKey] == daemonsetLabelValue &&
				pods[i].Status.Phase == corev1.PodRunning {
				pod := pods[i]
				klog.V(4).Infof("Mountpoint DaemonSet Pod %s/%s is running with id %s", pod.Namespace, pod.Name, pod.UID)
				return pod, dm.podPath(string(pod.UID)), nil
			}
		}

		select {
		case <-ctx.Done():
			return nil, "", fmt.Errorf("timed out waiting for Mountpoint DaemonSet Pod "+
				"(label %s=%s) to be running: %w", daemonsetLabelKey, daemonsetLabelValue, ctx.Err())
		case <-time.After(mountpointPodAttachmentPollInterval):
		}
	}
}

// Compile-time check that DaemonsetMounter implements Mounter.
var _ Mounter = (*DaemonsetMounter)(nil)
