// process_runner.go defines the ProcessRunner abstraction that decouples ProcessManager
// from real process lifecycle (fork/exec/wait). Tests inject a fake implementation to
// control when and how child processes "start" and "exit" without spawning real processes.

package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/armon/circbuf"
	"k8s.io/klog/v2"
)

// ProcessHandle represents a started process that can be waited on.
type ProcessHandle interface {
	Pid() int
	Wait() (exitCode int, stderr []byte)
	Signal(sig os.Signal) error
}

// ProcessRunner starts a command and returns a handle to wait on it, and lists the processes in this PID namespace.
type ProcessRunner interface {
	Start(cmd *exec.Cmd) (ProcessHandle, error)
	// ProcessUIDs returns every UID that has a process in this PID namespace, zombies included.
	ProcessUIDs() (map[uint32]bool, error)
}

// defaultProcessRunner is the real implementation that starts OS processes.
type defaultProcessRunner struct {
	stderrCapacity uint
}

func (r *defaultProcessRunner) Start(cmd *exec.Cmd) (ProcessHandle, error) {
	stderrBuf, err := circbuf.NewBuffer(int64(r.stderrCapacity))
	if err != nil {
		return nil, err
	}
	if cmd.Stderr != nil {
		cmd.Stderr = io.MultiWriter(cmd.Stderr, stderrBuf)
	} else {
		cmd.Stderr = stderrBuf
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &defaultProcessHandle{cmd: cmd, stderrBuf: stderrBuf}, nil
}

func (r *defaultProcessRunner) ProcessUIDs() (map[uint32]bool, error) {
	processes, err := listProcesses()
	if err != nil {
		return nil, err
	}
	uids := make(map[uint32]bool)
	for _, p := range processes {
		for _, uid := range p.uids {
			uids[uid] = true
		}
	}
	return uids, nil
}

// procStatus is what the mounter reads of a process from /proc/<pid>/status.
type procStatus struct {
	pid    int
	ppid   int
	zombie bool
	uids   [3]uint32 // real, effective and saved
}

// listProcesses reads every process in this PID namespace, zombies included.
func listProcesses() ([]procStatus, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	var processes []procStatus
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		status, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "status"))
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
			continue // reaped since the listing
		}
		if err != nil {
			return nil, err
		}
		field := func(name string) string {
			_, value, _ := strings.Cut(string(status), "\n"+name+":")
			value, _, _ = strings.Cut(value, "\n")
			return strings.TrimSpace(value)
		}
		p := procStatus{pid: pid, zombie: strings.HasPrefix(field("State"), "Z")}
		if p.ppid, err = strconv.Atoi(field("PPid")); err != nil {
			return nil, fmt.Errorf("unexpected PPid in /proc/%d/status: %w", pid, err)
		}
		uids := strings.Fields(field("Uid"))
		if len(uids) < 3 {
			return nil, fmt.Errorf("no real, effective and saved UID in /proc/%d/status", pid)
		}
		for i, value := range uids[:3] {
			uid, err := strconv.ParseUint(value, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("unexpected UID %q in /proc/%d/status: %w", value, pid, err)
			}
			p.uids[i] = uint32(uid)
		}
		processes = append(processes, p)
	}
	return processes, nil
}

type defaultProcessHandle struct {
	cmd       *exec.Cmd
	stderrBuf *circbuf.Buffer
}

func (h *defaultProcessHandle) Pid() int { return h.cmd.Process.Pid }

func (h *defaultProcessHandle) Wait() (int, []byte) {
	err := h.cmd.Wait()
	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			klog.Errorf("Unexpected error waiting for process %d: %v", h.cmd.Process.Pid, err)
			exitCode = 1
		}
	} else {
		exitCode = h.cmd.ProcessState.ExitCode()
	}
	return exitCode, h.stderrBuf.Bytes()
}

func (h *defaultProcessHandle) Signal(sig os.Signal) error {
	return h.cmd.Process.Signal(sig)
}
