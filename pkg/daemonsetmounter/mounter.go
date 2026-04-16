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
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"k8s.io/klog/v2"
)

const defaultSocketName = "mount.sock"

// Config holds the configuration for the DaemonsetMounter.
type Config struct {
	// CommDir is the shared directory between primary and secondary DaemonSets.
	// The Unix socket and error files live here.
	CommDir string

	// MountpointPath is the path to the mount-s3 binary inside the container.
	MountpointPath string

	// ProtocolVersion is the version this secondary expects. Requests with a
	// different version are rejected.
	ProtocolVersion int
}

// DaemonsetMounter is the core of the secondary DaemonSet. It listens on a
// Unix socket, receives mount requests from the primary, and spawns mount-s3
// child processes.
type DaemonsetMounter struct {
	config   Config
	listener *net.UnixListener
	children *ChildTracker
}

// New creates a new DaemonsetMounter. Call Run() to start accepting connections.
func New(config Config) *DaemonsetMounter {
	return &DaemonsetMounter{
		config:   config,
		children: NewChildTracker(),
	}
}

// Children returns the child tracker, used by the entrypoint for signal handling.
func (m *DaemonsetMounter) Children() *ChildTracker {
	return m.children
}

// Run starts the accept loop. It blocks until the listener is closed (via Stop).
// Each connection is handled sequentially — the kernel's listen backlog queues
// concurrent connection attempts.
//
// Sequential handling is intentional: each HandleConnection call is fast
// (recv + exec + close, typically milliseconds), and sequential processing
// avoids races on shared state. If this becomes a bottleneck, connections
// can be handled in goroutines in the future.
func (m *DaemonsetMounter) Run() error {
	sockPath := filepath.Join(m.config.CommDir, defaultSocketName)

	addr, err := net.ResolveUnixAddr("unix", sockPath)
	if err != nil {
		return fmt.Errorf("failed to resolve socket address %s: %w", sockPath, err)
	}

	m.listener, err = net.ListenUnix("unix", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", sockPath, err)
	}
	klog.Infof("Listening on %s", sockPath)

	for {
		conn, err := m.listener.AcceptUnix()
		if err != nil {
			// Check if the listener was closed (Stop was called).
			// net.ErrClosed is returned when Accept is called on a closed listener.
			if isClosedError(err) {
				klog.Info("Listener closed, stopping accept loop")
				return nil
			}
			klog.Errorf("Failed to accept connection: %v", err)
			continue
		}

		m.HandleConnection(conn)
	}
}

// Stop closes the listener, causing Run() to return. Called from signal handler.
func (m *DaemonsetMounter) Stop() {
	if m.listener != nil {
		m.listener.Close()
	}
}

// HandleConnection processes a single mount request from the primary DaemonSet.
//
// The flow:
//  1. Receive the MountRequest (JSON + FUSE FD via SCM_RIGHTS)
//  2. Validate the protocol version
//  3. Build and start the mount-s3 command
//  4. Close the parent's copy of the FUSE FD (child has its own)
//  5. Close the connection
//  6. Launch a goroutine to wait for the child and handle its exit
func (m *DaemonsetMounter) HandleConnection(conn *net.UnixConn) {
	defer conn.Close()

	// Step 1: Receive the mount request.
	req, err := RecvMountRequest(conn)
	if err != nil {
		klog.Errorf("Failed to receive mount request: %v", err)
		return
	}

	klog.Infof("Received mount request for volume %s (bucket: %s, protocol: %d)",
		req.VolumeID, req.BucketName, req.ProtocolVersion)

	// Step 2: Validate protocol version.
	if req.ProtocolVersion != m.config.ProtocolVersion {
		errMsg := fmt.Sprintf("protocol version mismatch: secondary expects %d, primary sent %d — drain the node to complete the upgrade",
			m.config.ProtocolVersion, req.ProtocolVersion)
		klog.Error(errMsg)
		if writeErr := WriteErrorFile(m.config.CommDir, req.VolumeID, []byte(errMsg)); writeErr != nil {
			klog.Errorf("Failed to write error file for version mismatch: %v", writeErr)
		}
		// Close the FUSE FD since we're not going to use it.
		syscall.Close(req.FuseFd)
		return
	}

	// Step 3: Build the mount-s3 command.
	//
	// mount-s3 expects: mount-s3 <bucket> /dev/fd/3 [args...]
	// The FUSE FD is passed via cmd.ExtraFiles, which makes it available as
	// fd 3 in the child process (fd 0=stdin, 1=stdout, 2=stderr, 3=first ExtraFile).
	fuseDev := os.NewFile(uintptr(req.FuseFd), "/dev/fuse")
	if fuseDev == nil {
		klog.Errorf("Invalid FUSE fd %d for volume %s", req.FuseFd, req.VolumeID)
		return
	}

	args := append([]string{req.BucketName, "/dev/fd/3"}, req.Args...)
	cmd := exec.Command(m.config.MountpointPath, args...)
	cmd.ExtraFiles = []*os.File{fuseDev}
	cmd.Env = req.Env

	// Capture stderr for error reporting. Also pipe to our own stderr so
	// mount-s3 logs appear in the secondary DaemonSet's kubectl logs.
	var stderrBuf bytes.Buffer
	cmd.Stdout = os.Stdout
	cmd.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)

	if err := cmd.Start(); err != nil {
		klog.Errorf("Failed to start mount-s3 for volume %s: %v", req.VolumeID, err)
		if writeErr := WriteErrorFile(m.config.CommDir, req.VolumeID, []byte(err.Error())); writeErr != nil {
			klog.Errorf("Failed to write error file: %v", writeErr)
		}
		fuseDev.Close()
		return
	}

	klog.Infof("Started mount-s3 for volume %s (pid %d)", req.VolumeID, cmd.Process.Pid)

	// Step 4: Close the parent's copy of the FUSE FD.
	// The child process has its own copy via ExtraFiles. Keeping the parent's
	// copy open would prevent the FUSE device from being released when the
	// child exits.
	fuseDev.Close()

	// Step 5: Connection is closed by the deferred conn.Close() above.

	// Step 6: Track the child and wait for it in a goroutine.
	child := &ChildProcess{
		VolumeID:  req.VolumeID,
		Cmd:       cmd,
		StartTime: time.Now(),
	}
	m.children.Register(child)

	go m.waitForChild(child, &stderrBuf)
}

// waitForChild waits for a mount-s3 child process to exit, then cleans up.
// This runs in a goroutine — one per child.
func (m *DaemonsetMounter) waitForChild(child *ChildProcess, stderrBuf *bytes.Buffer) {
	err := child.Cmd.Wait()
	m.children.Deregister(child.VolumeID)

	if err != nil {
		exitCode := -1
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		}
		klog.Errorf("mount-s3 for volume %s exited with code %d: %v", child.VolumeID, exitCode, err)

		// Write error file so the primary can detect the failure.
		stderr := stderrBuf.Bytes()
		if len(stderr) == 0 {
			stderr = []byte(err.Error())
		}
		if writeErr := WriteErrorFile(m.config.CommDir, child.VolumeID, stderr); writeErr != nil {
			klog.Errorf("Failed to write error file for volume %s: %v", child.VolumeID, writeErr)
		}
	} else {
		klog.Infof("mount-s3 for volume %s exited cleanly", child.VolumeID)
	}
}

// isClosedError checks if an error is due to the listener being closed.
func isClosedError(err error) bool {
	if err == nil {
		return false
	}
	// net.ErrClosed was added in Go 1.16. We also check the error string
	// for compatibility.
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	// The error message from Accept on a closed listener contains this string.
	return strings.Contains(err.Error(), "use of closed network connection")
}
