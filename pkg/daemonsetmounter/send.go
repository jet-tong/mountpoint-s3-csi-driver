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

package daemonsetmounter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
)

// dialRetryInterval is the interval between retries when connecting to the
// mounter DaemonSet's socket. The socket may not exist yet if the mounter
// DaemonSet is still starting up.
const dialRetryInterval = 50 * time.Millisecond

// SendMountRequest sends a MountRequest to the mounter DaemonSet over the
// Unix socket at sockPath. The JSON payload is sent as data, and the FUSE
// file descriptor is sent out-of-band via SCM_RIGHTS.
//
// This is the CSI node service side of the protocol. The mounter DaemonSet's
// RecvMountRequest (in request.go) is the receiving side.
//
// The function retries on ENOENT (socket doesn't exist yet) and ECONNREFUSED
// (socket exists but mounter isn't accepting yet), same as the V2
// mountoptions.Send does for Mountpoint Pod sockets.
func SendMountRequest(ctx context.Context, sockPath string, req MountRequest) error {
	// Serialize the JSON payload (everything except FuseFd, which goes via SCM_RIGHTS).
	payload, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("failed to marshal mount request: %w", err)
	}

	conn, err := dialWithRetry(ctx, sockPath)
	if err != nil {
		return fmt.Errorf("failed to connect to mounter socket %s: %w", sockPath, err)
	}
	defer conn.Close()

	// Respect the context deadline for the write operation.
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return fmt.Errorf("failed to set deadline on socket %s: %w", sockPath, err)
		}
	}

	// Send JSON payload + FUSE FD via SCM_RIGHTS in a single WriteMsgUnix call.
	rights := syscall.UnixRights(req.FuseFd)
	dataN, rightsN, err := conn.WriteMsgUnix(payload, rights, nil)
	if err != nil {
		return fmt.Errorf("failed to write to mounter socket %s: %w", sockPath, err)
	}
	if dataN != len(payload) || rightsN != len(rights) {
		return fmt.Errorf("partial write to mounter socket %s: data %d/%d, rights %d/%d",
			sockPath, dataN, len(payload), rightsN, len(rights))
	}

	klog.V(4).Infof("Sent mount request for volume %s to %s", req.VolumeID, sockPath)
	return nil
}

// dialWithRetry connects to the Unix socket at sockPath, retrying on transient
// errors until the context is cancelled or the deadline is reached.
func dialWithRetry(ctx context.Context, sockPath string) (*net.UnixConn, error) {
	var d net.Dialer
	var conn *net.UnixConn

	err := wait.PollUntilContextCancel(ctx, dialRetryInterval, true, func(ctx context.Context) (bool, error) {
		c, err := d.DialContext(ctx, "unix", sockPath)
		if err == nil {
			conn = c.(*net.UnixConn)
			return true, nil
		}

		// Retry on ENOENT (socket file doesn't exist yet) and ECONNREFUSED
		// (socket exists but nobody is accepting). These are expected during
		// mounter DaemonSet startup.
		if errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ECONNREFUSED) {
			return false, nil
		}

		// Non-retryable error.
		return false, err
	})

	return conn, err
}
