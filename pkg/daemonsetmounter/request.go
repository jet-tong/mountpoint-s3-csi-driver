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

// Package daemonsetmounter implements the secondary DaemonSet mounter for the
// two-daemonset architecture. It listens on a Unix socket, receives FUSE file
// descriptors from the primary CSI node DaemonSet, and spawns mount-s3
// processes as child processes.
//
// This package is independent of the V2 Mountpoint Pod mode — it does not
// import or depend on pkg/mountpoint/mountoptions or pkg/podmounter.
package daemonsetmounter

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
)

// ProtocolVersion is the current version of the IPC protocol between the
// primary and secondary DaemonSets. Both sides must agree on this version.
// Increment this when the MountRequest struct changes in a breaking way.
const ProtocolVersion = 1

// A MountRequest contains everything the secondary needs to spawn a mount-s3
// process for a single volume. The primary DaemonSet sends this over the Unix
// socket alongside the FUSE file descriptor (via SCM_RIGHTS).
type MountRequest struct {
	// ProtocolVersion allows detecting mismatches during rolling upgrades.
	// If the primary sends a version the secondary doesn't understand,
	// the secondary rejects the request with a clear error.
	ProtocolVersion int `json:"protocolVersion"`

	// VolumeID identifies the volume. Used for error file naming and logging.
	// This is the PV's volumeHandle from the CSI NodePublishVolume request.
	VolumeID string `json:"volumeID"`

	// BucketName is the S3 bucket to mount.
	BucketName string `json:"bucketName"`

	// Args are the mount-s3 CLI arguments (e.g., ["--allow-root", "--region=us-east-1"]).
	Args []string `json:"args"`

	// Env are environment variables for the mount-s3 process (e.g., ["AWS_REGION=us-east-1"]).
	Env []string `json:"env"`

	// FuseFd is the FUSE file descriptor received via SCM_RIGHTS.
	// It is NOT serialized in JSON — it's passed out-of-band through the socket.
	FuseFd int `json:"-"`
}

// RecvMountRequest reads a MountRequest from an accepted Unix socket connection.
// The JSON payload is read from the connection, and the FUSE file descriptor is
// received via SCM_RIGHTS (a Linux mechanism for passing file descriptors
// between processes over Unix sockets).
//
// This function is the daemonset-mode equivalent of mountoptions.Recv, but
// operates on an already-accepted connection rather than creating a listener.
// The two implementations are intentionally independent so the V2 and daemonset
// protocols can evolve separately.
func RecvMountRequest(conn *net.UnixConn) (MountRequest, error) {
	// Read the full message in a loop. The JSON payload may arrive in multiple
	// chunks, and the FUSE FD arrives as a socket control message (SCM_RIGHTS)
	// alongside one of the data chunks.
	var jsonBuf []byte
	var rightsBuf []byte

	for {
		data := make([]byte, 4096)
		rights := make([]byte, syscall.CmsgSpace(4)) // space for one 32-bit FD

		dataN, rightsN, _, _, err := conn.ReadMsgUnix(data, rights)
		if err != nil {
			// EOF means the sender closed the connection — we have the full message.
			if errors.Is(err, io.EOF) {
				break
			}
			return MountRequest{}, fmt.Errorf("failed to read from connection: %w", err)
		}

		if dataN == 0 && rightsN == 0 {
			break
		}

		jsonBuf = append(jsonBuf, data[:dataN]...)
		rightsBuf = append(rightsBuf, rights[:rightsN]...)
	}

	// Parse the JSON payload into a MountRequest.
	var req MountRequest
	if err := json.Unmarshal(jsonBuf, &req); err != nil {
		return MountRequest{}, fmt.Errorf("failed to unmarshal mount request: %w", err)
	}

	// Parse the SCM_RIGHTS control message to extract the FUSE file descriptor.
	fd, err := parseFuseFd(rightsBuf)
	if err != nil {
		return MountRequest{}, fmt.Errorf("failed to parse FUSE fd: %w", err)
	}
	req.FuseFd = fd

	return req, nil
}

// parseFuseFd extracts exactly one file descriptor from a SCM_RIGHTS socket
// control message. SCM_RIGHTS is the Linux mechanism that allows passing open
// file descriptors between processes over Unix domain sockets.
func parseFuseFd(buf []byte) (int, error) {
	msgs, err := syscall.ParseSocketControlMessage(buf)
	if err != nil {
		return 0, fmt.Errorf("failed to parse socket control message: %w", err)
	}

	var fds []int
	for _, msg := range msgs {
		parsed, err := syscall.ParseUnixRights(&msg)
		if err != nil {
			return 0, fmt.Errorf("failed to parse unix rights: %w", err)
		}
		fds = append(fds, parsed...)
	}

	if len(fds) != 1 {
		return 0, fmt.Errorf("expected exactly 1 file descriptor, got %d", len(fds))
	}

	return fds[0], nil
}
