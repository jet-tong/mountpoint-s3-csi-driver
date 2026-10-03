package main

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/util/testutil/assert"
)

func TestServe_EmptiesTheCacheVolumeBeforeListeningAndAfterStopping(t *testing.T) {
	pm, cacheDir := newProcessManagerWithCache(t, &fakeProcessRunner{}, cacheLimit{strategy: cacheLimitNone})
	assert.NoError(t, os.Mkdir(filepath.Join(cacheDir, "pv-old"), 0700))
	sock := filepath.Join(t.TempDir(), mountSockName)
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- serve(pm, sock, "/usr/bin/mount-s3", stop) }()

	// The socket exists only once the startup cleanup has run.
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if _, err := os.Lstat(sock); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("serve did not listen on %s", sock)
		}
	}
	assertNotExist(t, filepath.Join(cacheDir, "pv-old"))

	assert.NoError(t, os.Mkdir(filepath.Join(cacheDir, "pv-new"), 0700))
	close(stop)
	assert.NoError(t, <-done)
	assertNotExist(t, filepath.Join(cacheDir, "pv-new"))
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

func TestServe_RemovesStaleUIDMarkersBeforeListening(t *testing.T) {
	commDir := t.TempDir()
	pm := NewProcessManager(commDir, "", &fakeProcessRunner{}, memoryLimit{strategy: memoryLimitNone}, cacheLimit{strategy: cacheLimitNone})
	// What a mounter container that crashed leaves in the comm directory its pod keeps.
	assert.NoError(t, os.WriteFile(filepath.Join(commDir, ".uid-65536"), nil, 0600))
	errorFile := filepath.Join(commDir, "s3-pv.error")
	assert.NoError(t, os.WriteFile(errorFile, []byte("boom"), 0600))
	sock := filepath.Join(t.TempDir(), mountSockName)
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- serve(pm, sock, "/usr/bin/mount-s3", stop) }()

	// The socket exists only once startup has run; 5s bounds the failing case.
	waitAndAssert(t, "serve listened", 5*time.Second, func() bool {
		_, err := os.Lstat(sock)
		return err == nil
	})
	assertNotExist(t, filepath.Join(commDir, ".uid-65536"))
	_, err := os.Stat(errorFile)
	assert.NoError(t, err)

	close(stop)
	assert.NoError(t, <-done)
}
