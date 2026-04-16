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
	"os/exec"
	"sync"
	"syscall"
	"time"

	"k8s.io/klog/v2"
)

// ChildProcess represents a running mount-s3 process managed by the secondary DaemonSet.
type ChildProcess struct {
	VolumeID  string
	Cmd       *exec.Cmd
	StartTime time.Time
}

// ChildTracker keeps track of all running mount-s3 child processes.
// It provides methods to register, deregister, and signal children.
//
// Why we need this: the secondary DaemonSet is a long-lived daemon that spawns
// many mount-s3 processes over its lifetime. When a child exits, we must call
// Wait() on it to avoid zombie processes. When the daemon receives SIGTERM
// (during node drain), we must forward the signal to all children so they
// shut down cleanly.
type ChildTracker struct {
	// mu protects the children map. We use a regular mutex instead of sync.Map
	// because we need to iterate over all children for SignalAll/WaitAll.
	mu       sync.Mutex
	children map[string]*ChildProcess
}

// NewChildTracker creates a new empty ChildTracker.
func NewChildTracker() *ChildTracker {
	return &ChildTracker{
		children: make(map[string]*ChildProcess),
	}
}

// Register adds a child process to the tracker. If a child with the same
// volumeID already exists, it is overwritten (this shouldn't happen in normal
// operation, but is safe).
func (ct *ChildTracker) Register(child *ChildProcess) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.children[child.VolumeID] = child
	klog.Infof("Registered child process for volume %s (pid %d)", child.VolumeID, child.Cmd.Process.Pid)
}

// Deregister removes a child process from the tracker.
func (ct *ChildTracker) Deregister(volumeID string) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	delete(ct.children, volumeID)
	klog.Infof("Deregistered child process for volume %s", volumeID)
}

// SignalAll sends the given signal to all tracked child processes.
// Used during SIGTERM handling to forward the shutdown signal to all mount-s3
// processes before the daemon exits.
func (ct *ChildTracker) SignalAll(sig syscall.Signal) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	for volumeID, child := range ct.children {
		if child.Cmd.Process != nil {
			klog.Infof("Sending signal %v to child process for volume %s (pid %d)", sig, volumeID, child.Cmd.Process.Pid)
			if err := child.Cmd.Process.Signal(sig); err != nil {
				klog.Warningf("Failed to signal child process for volume %s: %v", volumeID, err)
			}
		}
	}
}

// WaitAll waits for all tracked child processes to exit, up to the given timeout.
// Returns the number of children that were still running when the timeout expired.
func (ct *ChildTracker) WaitAll(timeout time.Duration) int {
	done := make(chan struct{})
	go func() {
		ct.mu.Lock()
		var wg sync.WaitGroup
		for _, child := range ct.children {
			wg.Add(1)
			go func(c *ChildProcess) {
				defer wg.Done()
				_ = c.Cmd.Wait()
			}(child)
		}
		ct.mu.Unlock()
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return 0
	case <-time.After(timeout):
		ct.mu.Lock()
		remaining := len(ct.children)
		ct.mu.Unlock()
		klog.Warningf("Timed out waiting for %d children to exit", remaining)
		return remaining
	}
}

// Count returns the number of tracked children.
func (ct *ChildTracker) Count() int {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	return len(ct.children)
}
