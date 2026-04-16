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
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// sendMountRequest is a test helper that connects to the mounter's socket and
// sends a MountRequest with a FUSE FD (we use a pipe FD as a stand-in for
// /dev/fuse since tests run unprivileged).
func sendMountRequest(t *testing.T, sockPath string, req MountRequest) {
	t.Helper()

	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: sockPath, Net: "unix"})
	if err != nil {
		t.Fatalf("Failed to connect to socket: %v", err)
	}
	defer conn.Close()

	// Serialize the JSON payload.
	payload, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}

	// Send JSON + FD via SCM_RIGHTS.
	rights := syscall.UnixRights(req.FuseFd)
	_, _, err = conn.WriteMsgUnix(payload, rights, nil)
	if err != nil {
		t.Fatalf("Failed to send mount request: %v", err)
	}
}

// createPipeFd creates a pipe and returns the read end's FD.
// This is used as a stand-in for /dev/fuse in tests (we can't open /dev/fuse
// without privileges). The caller must close the returned FD.
func createPipeFd(t *testing.T) int {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Failed to create pipe: %v", err)
	}
	// Close the write end — we only need the read end as a valid FD to pass.
	w.Close()
	// Return the raw FD. We detach it from the *os.File so it's not
	// garbage-collected (which would close it).
	fd, err := syscall.Dup(int(r.Fd()))
	if err != nil {
		t.Fatalf("Failed to dup pipe fd: %v", err)
	}
	r.Close()
	return fd
}

// startMounter creates a DaemonsetMounter with a temp comm dir and starts it
// in a goroutine. Returns the mounter and the socket path. The mounter is
// stopped when the test completes.
func startMounter(t *testing.T, mountpointPath string) (*DaemonsetMounter, string) {
	t.Helper()

	commDir := t.TempDir()
	sockPath := filepath.Join(commDir, "mount.sock")

	m := New(Config{
		CommDir:         commDir,
		MountpointPath:  mountpointPath,
		ProtocolVersion: ProtocolVersion,
	})

	// Start in a goroutine — Run blocks until Stop is called.
	go func() {
		if err := m.Run(); err != nil {
			// Don't use t.Fatal in a goroutine — it panics.
			fmt.Fprintf(os.Stderr, "Mounter.Run failed: %v\n", err)
		}
	}()

	// Wait for the socket to appear.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(sockPath); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Cleanup(func() {
		m.Stop()
		// Give the accept loop time to notice the closed listener.
		time.Sleep(50 * time.Millisecond)
	})

	return m, sockPath
}

func TestBasicMountAndChildTracking(t *testing.T) {
	// Use "sh" as a fake mount-s3 binary. We pass "-c" and "sleep 60" as the
	// bucket name and fd path (which become the first args), but sh ignores
	// positional args after -c's command string. The actual Args field contains
	// the real command to run.
	//
	// The command built by HandleConnection is:
	//   sh <BucketName> /dev/fd/3 <Args...>
	// So we set BucketName="-c" and Args=["sleep 60"] to get:
	//   sh -c /dev/fd/3 sleep 60
	// sh -c runs the first arg after -c ("/dev/fd/3" fails but that's fine)
	// Actually, let's just use a script approach:
	m, sockPath := startMounterWithScript(t, "sleep 60")

	fd := createPipeFd(t)
	defer syscall.Close(fd)

	sendMountRequest(t, sockPath, MountRequest{
		ProtocolVersion: ProtocolVersion,
		VolumeID:        "test-vol-1",
		BucketName:      "test-bucket",
		Args:            []string{},
		Env:             []string{},
		FuseFd:          fd,
	})

	// Wait for the child to be registered.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if m.Children().Count() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if m.Children().Count() != 1 {
		t.Fatalf("Expected 1 child, got %d", m.Children().Count())
	}
}

// startMounterWithScript creates a temp shell script that ignores all arguments
// and runs the given command. This is used as a fake mount-s3 binary because
// HandleConnection prepends bucket name and /dev/fd/3 to the args, which real
// binaries like "sleep" don't understand.
func startMounterWithScript(t *testing.T, command string) (*DaemonsetMounter, string) {
	t.Helper()

	// Create a temp script that ignores args and runs the command.
	scriptDir := t.TempDir()
	scriptPath := filepath.Join(scriptDir, "fake-mount-s3.sh")
	script := fmt.Sprintf("#!/bin/sh\n%s\n", command)
	if err := os.WriteFile(scriptPath, []byte(script), 0755); err != nil {
		t.Fatalf("Failed to write fake script: %v", err)
	}

	return startMounter(t, scriptPath)
}

func TestSequentialMultiMount(t *testing.T) {
	m, sockPath := startMounterWithScript(t, "sleep 60")

	// Send two mount requests back-to-back.
	for i := 1; i <= 2; i++ {
		fd := createPipeFd(t)
		sendMountRequest(t, sockPath, MountRequest{
			ProtocolVersion: ProtocolVersion,
			VolumeID:        fmt.Sprintf("test-vol-%d", i),
			BucketName:      "test-bucket",
			Args:            []string{},
			Env:             []string{},
			FuseFd:          fd,
		})
		// Small delay to let the sequential handler process each connection.
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for both children to be registered.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if m.Children().Count() >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if m.Children().Count() != 2 {
		t.Fatalf("Expected 2 children, got %d", m.Children().Count())
	}
}

func TestProtocolVersionMismatch(t *testing.T) {
	m, sockPath := startMounterWithScript(t, "sleep 60")
	commDir := m.config.CommDir

	fd := createPipeFd(t)
	defer syscall.Close(fd)

	// Send a request with a wrong protocol version.
	sendMountRequest(t, sockPath, MountRequest{
		ProtocolVersion: ProtocolVersion + 999,
		VolumeID:        "mismatch-vol",
		BucketName:      "test-bucket",
		Args:            []string{},
		Env:             []string{},
		FuseFd:          fd,
	})

	// Wait for the error file to appear.
	errPath := ErrorFilePath(commDir, "mismatch-vol")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(errPath); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	content, err := os.ReadFile(errPath)
	if err != nil {
		t.Fatalf("Expected error file at %s, got: %v", errPath, err)
	}

	if len(content) == 0 {
		t.Fatal("Error file is empty")
	}

	// No child should have been spawned.
	if m.Children().Count() != 0 {
		t.Fatalf("Expected 0 children after version mismatch, got %d", m.Children().Count())
	}
}

func TestErrorFileOnChildFailure(t *testing.T) {
	// Use a script that exits immediately with code 1.
	m, sockPath := startMounterWithScript(t, "exit 1")
	commDir := m.config.CommDir

	fd := createPipeFd(t)

	sendMountRequest(t, sockPath, MountRequest{
		ProtocolVersion: ProtocolVersion,
		VolumeID:        "fail-vol",
		BucketName:      "test-bucket",
		Args:            []string{},
		Env:             []string{},
		FuseFd:          fd,
	})

	// Wait for the error file to appear (child exits quickly).
	errPath := ErrorFilePath(commDir, "fail-vol")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(errPath); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if _, err := os.Stat(errPath); err != nil {
		t.Fatalf("Expected error file at %s after child failure", errPath)
	}

	// Child should have been deregistered after exit.
	time.Sleep(100 * time.Millisecond)
	if m.Children().Count() != 0 {
		t.Fatalf("Expected 0 children after failure, got %d", m.Children().Count())
	}
}

func TestStaleCleanupOnStartup(t *testing.T) {
	commDir := t.TempDir()

	// Create stale files.
	staleSock := filepath.Join(commDir, "mount.sock")
	os.WriteFile(staleSock, []byte("stale"), 0600)
	staleErr := filepath.Join(commDir, "old-vol.error")
	os.WriteFile(staleErr, []byte("stale error"), 0600)

	CleanStaleFiles(commDir, "mount.sock")

	if _, err := os.Stat(staleSock); !os.IsNotExist(err) {
		t.Fatal("Stale socket should have been removed")
	}
	if _, err := os.Stat(staleErr); !os.IsNotExist(err) {
		t.Fatal("Stale error file should have been removed")
	}
}

// (end of tests)
