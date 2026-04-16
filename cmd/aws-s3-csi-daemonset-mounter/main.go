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

// Binary aws-s3-csi-daemonset-mounter is the secondary DaemonSet mounter for
// the two-daemonset architecture. It runs as a long-lived daemon on every node,
// listening for mount requests from the primary CSI node DaemonSet and spawning
// mount-s3 processes as child processes.
//
// Usage:
//
//	aws-s3-csi-daemonset-mounter --comm-dir=/comm --mountpoint-path=/bin/mount-s3
package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/klog/v2"

	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/daemonsetmounter"
)

const (
	// shutdownGracePeriod is how long we wait for children to exit after
	// forwarding SIGTERM. If children don't exit in time, we exit anyway
	// and let the container runtime clean them up.
	shutdownGracePeriod = 30 * time.Second
)

func main() {
	// Flags
	commDir := flag.String("comm-dir", "/comm", "Shared directory for socket and error files")
	mountpointPath := flag.String("mountpoint-path", "/bin/mount-s3", "Path to the mount-s3 binary")
	flag.Parse()

	klog.Infof("Starting aws-s3-csi-daemonset-mounter (protocol version %d)", daemonsetmounter.ProtocolVersion)
	klog.Infof("  comm-dir: %s", *commDir)
	klog.Infof("  mountpoint-path: %s", *mountpointPath)

	// Ensure the comm directory exists.
	if err := os.MkdirAll(*commDir, 0755); err != nil {
		klog.Fatalf("Failed to create comm directory %s: %v", *commDir, err)
	}

	// Clean up stale files from a previous instance (crash recovery).
	daemonsetmounter.CleanStaleFiles(*commDir, "mount.sock")

	// Create the mounter.
	mounter := daemonsetmounter.New(daemonsetmounter.Config{
		CommDir:         *commDir,
		MountpointPath:  *mountpointPath,
		ProtocolVersion: daemonsetmounter.ProtocolVersion,
	})

	// Handle SIGTERM/SIGINT for graceful shutdown.
	// On node drain, the pod receives SIGTERM. We:
	//   1. Stop accepting new connections
	//   2. Forward SIGTERM to all mount-s3 children
	//   3. Wait for children to exit (up to shutdownGracePeriod)
	//   4. Exit
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-sigCh
		klog.Infof("Received signal %v, initiating graceful shutdown", sig)

		// Stop accepting new connections.
		mounter.Stop()

		// Forward signal to all children.
		children := mounter.Children()
		children.SignalAll(syscall.SIGTERM)

		// Wait for children to exit.
		remaining := children.WaitAll(shutdownGracePeriod)
		if remaining > 0 {
			klog.Warningf("Exiting with %d children still running", remaining)
		}

		klog.Info("Shutdown complete")
		os.Exit(0)
	}()

	// Run the accept loop (blocks until Stop is called).
	if err := mounter.Run(); err != nil {
		klog.Fatalf("Mounter failed: %v", err)
	}
}
