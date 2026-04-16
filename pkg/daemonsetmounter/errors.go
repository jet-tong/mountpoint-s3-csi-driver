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
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"k8s.io/klog/v2"
)

// errorFileSuffix is the extension for error files written by the secondary
// when a mount-s3 child process exits with a non-zero code.
const errorFileSuffix = ".error"

// errorFilePerm is the permission for error files. Only the owner (the
// secondary DaemonSet's user) can read/write.
const errorFilePerm = fs.FileMode(0600)

// ErrorFilePath returns the path to the error file for a given volume ID.
// The primary DaemonSet polls for this file to detect mount failures.
//
// Example: ErrorFilePath("/comm", "s3-pv") → "/comm/s3-pv.error"
func ErrorFilePath(commDir, volumeID string) string {
	return filepath.Join(commDir, volumeID+errorFileSuffix)
}

// WriteErrorFile writes mount-s3's stderr output to the error file for the
// given volume. The primary reads this to report the failure to kubelet.
func WriteErrorFile(commDir, volumeID string, stderr []byte) error {
	path := ErrorFilePath(commDir, volumeID)
	if err := os.WriteFile(path, stderr, errorFilePerm); err != nil {
		return fmt.Errorf("failed to write error file %s: %w", path, err)
	}
	klog.Infof("Wrote error file for volume %s: %s", volumeID, path)
	return nil
}

// CleanStaleFiles removes leftover socket and error files from a previous
// instance of the secondary DaemonSet. This is called on startup to ensure
// a clean state.
//
// Why this is needed: if the secondary crashes, it may leave behind a stale
// mount.sock (preventing the new instance from binding) and stale .error
// files (which the primary might misinterpret as current failures).
func CleanStaleFiles(commDir, socketName string) {
	// Remove stale socket
	sockPath := filepath.Join(commDir, socketName)
	if err := os.Remove(sockPath); err != nil && !os.IsNotExist(err) {
		klog.Warningf("Failed to remove stale socket %s: %v", sockPath, err)
	}

	// Remove stale error files
	entries, err := os.ReadDir(commDir)
	if err != nil {
		klog.Warningf("Failed to read comm dir %s for stale cleanup: %v", commDir, err)
		return
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), errorFileSuffix) {
			path := filepath.Join(commDir, entry.Name())
			if err := os.Remove(path); err != nil {
				klog.Warningf("Failed to remove stale error file %s: %v", path, err)
			} else {
				klog.Infof("Cleaned stale error file: %s", path)
			}
		}
	}
}
