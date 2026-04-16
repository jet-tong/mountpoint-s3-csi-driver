/*
Copyright 2022 The Kubernetes Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mounter

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"

	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/cluster"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/daemonsetmounter"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/driver/node/credentialprovider"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/driver/node/envprovider"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/driver/node/targetpath"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/mountpoint"
	mpmounter "github.com/awslabs/mountpoint-s3-csi-driver/pkg/mountpoint/mounter"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/util"
)

const (
	// mountReadyTimeout is how long we wait for mount-s3 to start serving
	// after sending the mount request to the mounter DaemonSet.
	mountReadyTimeout = 30 * time.Second

	// mountReadyPollInterval is how often we check if the mount is ready.
	mountReadyPollInterval = 500 * time.Millisecond

	// socketName is the name of the Unix socket in the comm directory.
	socketName = "mount.sock"
)

// A DaemonsetNodeMounter is a [Mounter] that sends mount requests to the
// mounter DaemonSet running on the same node, instead of to individual
// Mountpoint Pods (which is what PodMounter does).
//
// This eliminates the scheduling problem: the mounter DaemonSet is pre-placed
// on every node, so there's never a missing pod for the workload to wait on.
type DaemonsetNodeMounter struct {
	mount             *mpmounter.Mounter
	kubeletPath       string
	commDir           string
	mountSyscall      mountSyscall
	bindMountSyscall  bindMountSyscall
	credProvider      credentialprovider.ProviderInterface
	kubernetesVersion string
	variant           cluster.Variant
}

// NewDaemonsetNodeMounter creates a new DaemonsetNodeMounter.
//
// commDir is the shared directory between the CSI node service and the mounter
// DaemonSet (e.g., /var/lib/kubelet/plugins/s3.csi.aws.com/comm/).
func NewDaemonsetNodeMounter(
	mount *mpmounter.Mounter,
	commDir string,
	credProvider credentialprovider.ProviderInterface,
	mountSyscall mountSyscall,
	bindMountSyscall bindMountSyscall,
	kubernetesVersion string,
	variant cluster.Variant,
) *DaemonsetNodeMounter {
	return &DaemonsetNodeMounter{
		mount:             mount,
		kubeletPath:       util.ContainerKubeletPath(),
		commDir:           commDir,
		mountSyscall:      mountSyscall,
		bindMountSyscall:  bindMountSyscall,
		credProvider:      credProvider,
		kubernetesVersion: kubernetesVersion,
		variant:           variant,
	}
}

// Mount mounts the given S3 bucket at the target path by sending a mount
// request to the mounter DaemonSet.
//
// The flow:
//  1. Extract volume ID from target path
//  2. Check if target is already mounted → return early
//  3. Build source path and check if source is already mounted
//  4. Build credentials and environment
//  5. Open /dev/fuse and perform kernel mount at source
//  6. Send MountRequest to mounter DaemonSet via Unix socket
//  7. Wait for mount-s3 to start serving (poll IsMountPoint or error file)
//  8. Bind mount from source to target
func (dm *DaemonsetNodeMounter) Mount(
	ctx context.Context,
	bucketName string,
	target string,
	credentialCtx credentialprovider.ProvideContext,
	args mountpoint.Args,
	fsGroup string,
	userEnv envprovider.Environment,
) error {
	volumeID, err := dm.volumeIDFromTargetPath(target)
	if err != nil {
		return fmt.Errorf("failed to extract volume ID from %q: %w", target, err)
	}

	// Check if target is already a mountpoint.
	isTargetMounted, err := dm.IsMountPoint(target)
	if err != nil {
		if err = dm.ensureTargetDir(target, err); err != nil {
			return fmt.Errorf("failed to prepare target path %q: %w", target, err)
		}
	}
	if isTargetMounted {
		klog.V(4).Infof("DaemonsetMounter: target %q already mounted, skipping", target)
		return nil
	}

	// Build source path. In daemonset mode we key by volumeID (not pod name).
	source := filepath.Join(SourceMountDir(dm.kubeletPath), volumeID)
	isSourceMounted, err := dm.IsMountPoint(source)
	if err != nil {
		if err = dm.ensureTargetDir(source, err); err != nil {
			return fmt.Errorf("failed to prepare source path %q: %w", source, err)
		}
	}

	// If source is not yet mounted, perform the full mount flow.
	if !isSourceMounted {
		if err := dm.mountS3AtSource(ctx, source, volumeID, bucketName, credentialCtx, args, userEnv); err != nil {
			return fmt.Errorf("failed to mount at source %q: %w", source, err)
		}
	}

	// Bind mount from source to target.
	if err := dm.bindMountWithDefault(source, target); err != nil {
		return fmt.Errorf("failed to bind mount %q to %q: %w", source, target, err)
	}

	klog.V(4).Infof("DaemonsetMounter: mounted volume %s at %s (source: %s)", volumeID, target, source)
	return nil
}

// mountS3AtSource performs the full mount flow: open FUSE, kernel mount, send
// request to mounter DaemonSet, wait for mount-s3 to start serving.
func (dm *DaemonsetNodeMounter) mountS3AtSource(
	ctx context.Context,
	source string,
	volumeID string,
	bucketName string,
	credentialCtx credentialprovider.ProvideContext,
	args mountpoint.Args,
	userEnv envprovider.Environment,
) error {
	// Build environment with precedence (highest wins): credEnv > Default() > userEnv.
	env := envprovider.Environment{}
	env.Merge(userEnv)
	env.Merge(envprovider.Default())

	// For daemonset mode MVP, we use driver-level credentials only.
	// The credential provider reads the CSI node service's own environment
	// (IRSA token, Pod Identity token, K8s secrets, or instance profile)
	// and returns env vars to pass to mount-s3.
	credentialCtx.SetAsPodMountpoint()
	// In daemonset mode, credential files are written to the comm dir
	// and mount-s3 reads them from there.
	credDir := filepath.Join(dm.commDir, "credentials", volumeID)
	if err := os.MkdirAll(credDir, credentialprovider.CredentialDirPerm); err != nil {
		return fmt.Errorf("failed to create credential dir %s: %w", credDir, err)
	}
	credentialCtx.SetWriteAndEnvPath(credDir, credDir)

	credEnv, authSource, err := dm.credProvider.Provide(ctx, credentialCtx)
	if err != nil {
		return fmt.Errorf("failed to provide credentials: %w", err)
	}
	env.Merge(credEnv)

	// Move --aws-max-attempts to env if provided in mount options.
	if maxAttempts, ok := args.Remove(mountpoint.ArgAWSMaxAttempts); ok {
		env.Set(envprovider.EnvMaxAttempts, maxAttempts)
	}

	args.Set(mountpoint.ArgUserAgentPrefix, UserAgent(authSource, dm.kubernetesVersion, dm.variant))

	klog.V(4).Infof("DaemonsetMounter: mounting %s at source %s", bucketName, source)

	// Open /dev/fuse and perform kernel mount syscall.
	fuseFd, err := dm.mountSyscallWithDefault(source, args)
	if err != nil {
		return fmt.Errorf("failed to mount %s: %w", source, err)
	}

	// Remove --read-only from args (already passed as MS_RDONLY flag in mount syscall).
	args.Remove(mountpoint.ArgReadOnly)

	// Ensure we clean up the source mount if anything fails after this point.
	unmountOnFailure := true
	defer func() {
		if unmountOnFailure {
			if unmountErr := dm.mount.Unmount(source); unmountErr != nil {
				klog.Errorf("DaemonsetMounter: failed to unmount source %s on failure: %v", source, unmountErr)
			}
		}
	}()

	// Always close the parent's copy of the FUSE FD. After SendMountRequest,
	// the mounter DaemonSet has its own copy (received via SCM_RIGHTS).
	defer mpmounter.CloseFD(fuseFd)

	// Remove stale error file from a previous attempt.
	_ = os.Remove(daemonsetmounter.ErrorFilePath(dm.commDir, volumeID))

	// Send mount request to the mounter DaemonSet.
	sockPath := filepath.Join(dm.commDir, socketName)
	err = daemonsetmounter.SendMountRequest(ctx, sockPath, daemonsetmounter.MountRequest{
		ProtocolVersion: daemonsetmounter.ProtocolVersion,
		VolumeID:        volumeID,
		BucketName:      bucketName,
		Args:            args.SortedList(),
		Env:             env.List(),
		FuseFd:          fuseFd,
	})
	if err != nil {
		return fmt.Errorf("failed to send mount request for %s: %w", volumeID, err)
	}

	// Wait for mount-s3 to start serving, or for an error file to appear.
	if err := dm.waitForMountReady(ctx, source, volumeID); err != nil {
		return err
	}

	// Mount succeeded — don't unmount on return.
	unmountOnFailure = false
	return nil
}

// waitForMountReady polls until the FUSE mount at source is serving, or until
// an error file appears (meaning mount-s3 failed to start).
func (dm *DaemonsetNodeMounter) waitForMountReady(ctx context.Context, source, volumeID string) error {
	ctx, cancel := context.WithTimeout(ctx, mountReadyTimeout)
	defer cancel()

	errFilePath := daemonsetmounter.ErrorFilePath(dm.commDir, volumeID)
	mountResultCh := make(chan error, 1)

	// Poll for error file.
	go func() {
		_ = wait.PollUntilContextCancel(ctx, mountReadyPollInterval, true, func(ctx context.Context) (bool, error) {
			content, err := os.ReadFile(errFilePath)
			if err != nil {
				return false, nil // file doesn't exist yet, keep polling
			}
			// Clean up the error file after reading.
			_ = os.Remove(errFilePath)
			mountResultCh <- fmt.Errorf("mount-s3 failed for volume %s: %s", volumeID, content)
			return true, nil
		})
	}()

	// Poll for mount success.
	go func() {
		err := wait.PollUntilContextCancel(ctx, mountReadyPollInterval, true, func(ctx context.Context) (bool, error) {
			return dm.IsMountPoint(source)
		})
		if err != nil {
			mountResultCh <- fmt.Errorf("timed out waiting for mount at %s: %w", source, err)
		} else {
			mountResultCh <- nil
		}
	}()

	return <-mountResultCh
}

// Unmount unmounts the bind mount at target. If the source FUSE mount has no
// remaining bind mount references, it is also unmounted (which causes the
// mount-s3 child in the mounter DaemonSet to exit).
func (dm *DaemonsetNodeMounter) Unmount(ctx context.Context, target string, credentialCtx credentialprovider.CleanupContext) error {
	// Unmount the bind mount at target.
	if err := dm.mount.Unmount(target); err != nil {
		return fmt.Errorf("failed to unmount bind mount at %q: %w", target, err)
	}

	klog.V(4).Infof("DaemonsetMounter: unmounted bind mount at %s", target)

	// For the MVP (no pod sharing), we also unmount the source FUSE mount.
	// In phase 2a (pod sharing), we'd check refcount before unmounting source.
	volumeID, err := dm.volumeIDFromTargetPath(target)
	if err != nil {
		klog.Warningf("DaemonsetMounter: could not extract volume ID from %s for source cleanup: %v", target, err)
		return nil
	}

	source := filepath.Join(SourceMountDir(dm.kubeletPath), volumeID)
	isMounted, err := dm.IsMountPoint(source)
	if err != nil || !isMounted {
		return nil // source already unmounted or doesn't exist
	}

	if err := dm.mount.Unmount(source); err != nil {
		klog.Errorf("DaemonsetMounter: failed to unmount source %s: %v", source, err)
		return fmt.Errorf("failed to unmount source %q: %w", source, err)
	}

	klog.V(4).Infof("DaemonsetMounter: unmounted source FUSE mount at %s", source)
	return nil
}

// IsMountPoint checks if the given path is a mountpoint-s3 FUSE mount.
func (dm *DaemonsetNodeMounter) IsMountPoint(target string) (bool, error) {
	return dm.mount.CheckMountpoint(target)
}

// --- Helper methods ---

// volumeIDFromTargetPath extracts the volume ID from a kubelet target path.
func (dm *DaemonsetNodeMounter) volumeIDFromTargetPath(target string) (string, error) {
	tp, err := targetpath.Parse(target)
	if err != nil {
		return "", err
	}
	return tp.VolumeID, nil
}

// ensureTargetDir creates the target directory if it doesn't exist, or
// unmounts it if it's a corrupted mount.
func (dm *DaemonsetNodeMounter) ensureTargetDir(target string, checkErr error) error {
	if errors.Is(checkErr, fs.ErrNotExist) {
		return os.MkdirAll(target, targetDirPerm)
	}
	if dm.mount.IsMountpointCorrupted(checkErr) {
		klog.V(4).Infof("DaemonsetMounter: target %q is corrupted, unmounting", target)
		return dm.mount.Unmount(target)
	}
	return checkErr
}

// mountSyscallWithDefault delegates to the injected mountSyscall (for testing)
// or falls back to the real platform mount.
func (dm *DaemonsetNodeMounter) mountSyscallWithDefault(target string, args mountpoint.Args) (int, error) {
	if dm.mountSyscall != nil {
		return dm.mountSyscall(target, args)
	}
	opts := mpmounter.MountOptions{
		ReadOnly:   args.Has(mountpoint.ArgReadOnly),
		AllowOther: args.Has(mountpoint.ArgAllowOther) || args.Has(mountpoint.ArgAllowRoot),
	}
	return dm.mount.Mount(target, opts)
}

// bindMountWithDefault delegates to the injected bindMountSyscall (for testing)
// or falls back to the real platform bind mount.
func (dm *DaemonsetNodeMounter) bindMountWithDefault(source, target string) error {
	if dm.bindMountSyscall != nil {
		return dm.bindMountSyscall(source, target)
	}
	return dm.mount.BindMount(source, target)
}
