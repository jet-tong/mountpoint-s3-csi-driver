package main

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/util/testutil/assert"
)

func TestServe_EmptiesTheCacheVolumeAfterStartingAndAfterStopping(t *testing.T) {
	pm, cacheDir := newProcessManagerWithCache(t, &fakeProcessRunner{}, cacheLimit{strategy: cacheLimitNone})
	assert.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "pv-old", "mountpoint-cache"), 0700))
	sock := filepath.Join(t.TempDir(), mountSockName)
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- serve(pm, sock, "/usr/bin/mount-s3", stop) }()

	waitForSocket(t, sock)
	// Polled before stop, so only the startup release can have removed it.
	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if _, err := os.Lstat(filepath.Join(cacheDir, "pv-old")); errors.Is(err, fs.ErrNotExist) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("serve did not remove the leftover it found at startup")
		}
	}

	assert.NoError(t, os.Mkdir(filepath.Join(cacheDir, "pv-new"), 0700))
	close(stop)
	assert.NoError(t, <-done)
	assertNotExist(t, filepath.Join(cacheDir, "pv-new"))
}

func TestServe_ListensBeforeTheStartupRemovalEndsAndNamesWhatItStillHoldsAtExit(t *testing.T) {
	fr := &fakeProcessRunner{helperGate: make(chan struct{})}
	pm, cacheDir := newProcessManagerWithCache(t, fr, cacheLimit{strategy: cacheLimitNone})
	// Well inside the 2s bound below, so a Shutdown that ignores it fails as a timeout.
	pm.shutdownTimeout = 100 * time.Millisecond
	t.Cleanup(func() {
		close(fr.helperGate)
		pm.wg.Wait()
	})
	// What a killed Mountpoint as UID 65536 left behind; it waits behind lost+found, whose removal helper is held until cleanup.
	assert.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "uid-65536", "mountpoint-cache"), 0700))
	// Root-owned in production, so it ties up no UID; exit cleanup must still leave it to the release that holds it.
	assert.NoError(t, os.MkdirAll(filepath.Join(cacheDir, "lost+found", "x"), 0700))
	sock := filepath.Join(t.TempDir(), mountSockName)
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- serve(pm, sock, "/usr/bin/mount-s3", stop) }()

	waitForSocket(t, sock)
	pm.mu.Lock()
	_, releasing := pm.processes[65536]
	pm.mu.Unlock()
	assert.Equals(t, true, releasing)

	close(stop)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected serve to fail while a leftover is still being removed")
		}
		assert.Contains(t, err.Error(), `skipped cache volume entry "uid-65536"`)
		assert.Contains(t, err.Error(), `skipped cache volume entry "lost+found"`)
	// 2s bounds only the failing case: Shutdown waiting for the held helper, or exit cleanup starting a second one.
	case <-time.After(2 * time.Second):
		t.Fatal("serve did not return after stop while a leftover's removal was held")
	}
}

func TestServe_ExitsWithoutListeningWhenTheCacheVolumeCannotBeLocked(t *testing.T) {
	pm, _ := newProcessManagerWithCache(t, &fakeProcessRunner{}, cacheLimit{strategy: cacheLimitNone})
	pm.chown = func(string, int, int) error { return syscall.EPERM }
	sock := filepath.Join(t.TempDir(), mountSockName)
	// Already closed, so a serve that wrongly gets past the lock returns instead of serving forever.
	stop := make(chan struct{})
	close(stop)

	err := serve(pm, sock, "/usr/bin/mount-s3", stop)
	if err == nil {
		t.Fatal("expected serve to fail when the cache volume cannot be locked")
	}
	assert.Contains(t, err.Error(), "cannot chown")
	assertNotExist(t, sock)
}

// waitForSocket waits until serve listens on sock; 5s bounds only the failing case.
func waitForSocket(t *testing.T, sock string) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if _, err := os.Lstat(sock); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("serve did not listen on %s", sock)
		}
	}
}
