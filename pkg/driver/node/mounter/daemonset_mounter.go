// daemonset_mounter.go provides the daemonset-mode Mounter implementation.
// It targets the pre-placed Mountpoint DaemonSet Pod on the node instead of
// a controller-spawned per-mount Pod in pod_mounter.go.
//
// The Mount flow is a trimmed version of PodMounter.Mount with the S3PA CRD
// lookup replaced by a label-based pod lookup. Shared helper methods are
// called on a held *PodMounter reference.
package mounter

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

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
// It reuses PodMounter's mountS3AtSource helper (FUSE mount syscall + Send
// fd/options to DS pod in mount_options.go).
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
//  1. Find the DaemonSet Pod on this node by label (with retry)
//  2. Perform FUSE mount syscall directly at target path
//  3. Send mount options (including FUSE fd) to the Pod over Unix socket
//  4. Wait for mount-s3 to start serving at target
//
// TODO: Note fsgroup param not used for now
func (dm *DaemonsetMounter) Mount(ctx context.Context, bucketName string, target string, credentialCtx credentialprovider.ProvideContext, args mountpoint.Args, fsGroup string, userEnv envprovider.Environment) error {
	// TODO: add helpMessageFor...() help messages for all errors

	isTargetMountPoint, err := dm.IsMountPoint(target)
	if err != nil {
		// TODO: extract to shared helper later
		err = dm.pm.verifyOrSetupMountTarget(target, err)
		if err != nil {
			return fmt.Errorf("Failed to verify target path can be used as a mount point %q: %w", target, err)
		}
	}

	pod, podPath, err := dm.getDaemonsetMounterPodWithRetry(ctx)
	if err != nil {
		klog.Errorf("Failed to find running Mountpoint DaemonSet Pod for %q: %v", target, err)
		return fmt.Errorf("Failed to find running Mountpoint DaemonSet Pod: %w", err)
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()

	credEnv := envprovider.Environment{}
	authenticationSource := credentialprovider.AuthenticationSourceDriver

	if !isTargetMountPoint {
		// TODO: extract to shared helper later.
		err = dm.pm.mountS3AtSource(ctx, target, pod, podPath, bucketName, credEnv, userEnv, authenticationSource, args)
		if err != nil {
			return fmt.Errorf("Failed to mount S3 bucket at %q: %w", target, err)
		}
	}

	klog.V(4).Infof("Mounted S3 bucket at target %s via Mountpoint DaemonSet Pod %s", target, pod.Name)
	return nil
}

// Unmount unmounts the FUSE mount at target.
func (dm *DaemonsetMounter) Unmount(ctx context.Context, target string, credentialCtx credentialprovider.CleanupContext) error {
	// TODO: Currently it will exit with exit code 1 (ShouldExitWithSuccessCode in csimounter.go),
	// and the DS pod restarts and waits on the socket for the next mount. Will be resolved when we
	// implement the daemonset mounter binary, keeping it this way during development.
	err := dm.mount.Unmount(target)
	if err != nil {
		klog.Errorf("Failed to unmount %q: %v", target, err)
		return fmt.Errorf("Failed to unmount %q: %w", target, err)
	}
	klog.V(4).Infof("Unmounted %s", target)
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
