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
	"net"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// TestSendRecvRoundTrip verifies that SendMountRequest and RecvMountRequest
// correctly transfer all fields including the FUSE FD via SCM_RIGHTS.
func TestSendRecvRoundTrip(t *testing.T) {
	sockDir := t.TempDir()
	sockPath := filepath.Join(sockDir, "test.sock")

	// Create a pipe FD to use as a stand-in for /dev/fuse.
	pipeFd := createPipeFd(t)
	defer syscall.Close(pipeFd)

	sent := MountRequest{
		ProtocolVersion: 1,
		VolumeID:        "test-volume",
		BucketName:      "my-bucket",
		Args:            []string{"--allow-root", "--region=us-east-1"},
		Env:             []string{"AWS_REGION=us-east-1", "AWS_DEFAULT_REGION=us-east-1"},
		FuseFd:          pipeFd,
	}

	// Start a listener that accepts one connection and receives the request.
	recvCh := make(chan MountRequest, 1)
	errCh := make(chan error, 1)

	addr, err := net.ResolveUnixAddr("unix", sockPath)
	if err != nil {
		t.Fatalf("Failed to resolve addr: %v", err)
	}
	listener, err := net.ListenUnix("unix", addr)
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.AcceptUnix()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()

		req, err := RecvMountRequest(conn)
		if err != nil {
			errCh <- err
			return
		}
		recvCh <- req
	}()

	// Send the request.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := SendMountRequest(ctx, sockPath, sent); err != nil {
		t.Fatalf("SendMountRequest failed: %v", err)
	}

	// Wait for the receiver.
	select {
	case received := <-recvCh:
		// Verify all JSON fields match.
		if received.ProtocolVersion != sent.ProtocolVersion {
			t.Errorf("ProtocolVersion: got %d, want %d", received.ProtocolVersion, sent.ProtocolVersion)
		}
		if received.VolumeID != sent.VolumeID {
			t.Errorf("VolumeID: got %q, want %q", received.VolumeID, sent.VolumeID)
		}
		if received.BucketName != sent.BucketName {
			t.Errorf("BucketName: got %q, want %q", received.BucketName, sent.BucketName)
		}
		if len(received.Args) != len(sent.Args) {
			t.Errorf("Args length: got %d, want %d", len(received.Args), len(sent.Args))
		}
		for i := range sent.Args {
			if i < len(received.Args) && received.Args[i] != sent.Args[i] {
				t.Errorf("Args[%d]: got %q, want %q", i, received.Args[i], sent.Args[i])
			}
		}
		if len(received.Env) != len(sent.Env) {
			t.Errorf("Env length: got %d, want %d", len(received.Env), len(sent.Env))
		}

		// Verify the FD was received (it will be a different number in this
		// process, but it should be a valid FD).
		if received.FuseFd <= 0 {
			t.Errorf("FuseFd: got %d, expected a positive FD", received.FuseFd)
		}
		// Clean up the received FD.
		syscall.Close(received.FuseFd)

	case err := <-errCh:
		t.Fatalf("RecvMountRequest failed: %v", err)

	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for RecvMountRequest")
	}
}

// TestSendRetryOnMissingSocket verifies that SendMountRequest retries when
// the socket doesn't exist yet (ENOENT), and succeeds once it appears.
func TestSendRetryOnMissingSocket(t *testing.T) {
	sockDir := t.TempDir()
	sockPath := filepath.Join(sockDir, "delayed.sock")

	pipeFd := createPipeFd(t)
	defer syscall.Close(pipeFd)

	req := MountRequest{
		ProtocolVersion: 1,
		VolumeID:        "retry-vol",
		BucketName:      "bucket",
		FuseFd:          pipeFd,
	}

	// Start sending before the socket exists — it should retry.
	sendDone := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		sendDone <- SendMountRequest(ctx, sockPath, req)
	}()

	// Wait a bit, then create the socket.
	time.Sleep(200 * time.Millisecond)

	addr, _ := net.ResolveUnixAddr("unix", sockPath)
	listener, err := net.ListenUnix("unix", addr)
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	// Accept the connection (the send should have retried and connected).
	go func() {
		conn, err := listener.AcceptUnix()
		if err != nil {
			return
		}
		defer conn.Close()
		// Read and discard — we just need to accept so Send completes.
		RecvMountRequest(conn)
	}()

	if err := <-sendDone; err != nil {
		t.Fatalf("SendMountRequest should have succeeded after retry: %v", err)
	}
}
