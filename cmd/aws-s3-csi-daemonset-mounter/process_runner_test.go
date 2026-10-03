package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/util/testutil/assert"
)

func TestDefaultProcessRunner_ProcessUIDs(t *testing.T) {
	t.Run("lists a running child, and one that exited but is not yet reaped, as the test user's", func(t *testing.T) {
		running := exec.Command("sleep", "60")
		assert.NoError(t, running.Start())
		t.Cleanup(func() {
			running.Process.Kill()
			running.Wait()
		})
		exited := exec.Command("true")
		assert.NoError(t, exited.Start())
		t.Cleanup(func() { exited.Wait() })
		// It stays a zombie until Wait; 5s bounds the failing case only.
		waitAndAssert(t, "true became a zombie", 5*time.Second, func() bool { return procState(t, exited.Process.Pid) == "Z" })

		processes, err := listProcesses()
		assert.NoError(t, err)
		self := uint32(os.Getuid())
		for _, want := range []procStatus{
			{pid: running.Process.Pid, ppid: os.Getpid(), zombie: false, uids: [3]uint32{self, self, self}},
			{pid: exited.Process.Pid, ppid: os.Getpid(), zombie: true, uids: [3]uint32{self, self, self}},
		} {
			i := slices.IndexFunc(processes, func(got procStatus) bool { return got.pid == want.pid })
			if i < 0 {
				t.Fatalf("PID %d is not listed", want.pid)
			}
			// Formatted, as cmp cannot compare unexported fields.
			assert.Equals(t, fmt.Sprintf("%+v", want), fmt.Sprintf("%+v", processes[i]))
		}

		uids, err := (&defaultProcessRunner{}).ProcessUIDs()
		assert.NoError(t, err)
		assert.Equals(t, true, uids[uint32(os.Getuid())])
	})
}

// procState returns the state letter of pid from /proc, or "" once it is gone.
func procState(t *testing.T, pid int) string {
	t.Helper()
	stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return ""
	}
	// The command name in parentheses may contain spaces, so the state is the first field after the last ')'.
	fields := strings.Fields(string(stat[strings.LastIndexByte(string(stat), ')')+1:]))
	return fields[0]
}
