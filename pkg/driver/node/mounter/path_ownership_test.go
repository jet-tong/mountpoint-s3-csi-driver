package mounter

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/driver/node/credentialprovider"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/util/testutil/assert"
)

// Runs the real chmod, so this covers the [os.Root] resolution that keeps a planted link from
// reaching the file it names.
func TestChmodWithDefaultRefusesALinkOutOfTheDirectory(t *testing.T) {
	dm := &DaemonsetMounter{}

	credDir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "another-mounts-token")
	assert.NoError(t, os.WriteFile(outside, []byte("secret"), 0o600))
	link := filepath.Join(credDir, "planted")
	assert.NoError(t, os.Symlink(outside, link))

	if err := dm.chmodWithDefault(link, credentialprovider.IsolatedCredentialFilePerm); err == nil {
		t.Error("expected a link naming a file outside the directory to be refused")
	}

	st, err := os.Stat(outside)
	assert.NoError(t, err)
	assert.Equals(t, fs.FileMode(0o600), st.Mode().Perm())
}

// The mounter owns the comm directory and can replace entries in it, so a link it plants at mounts/ must not
// lead s3-csi-node outside it when it creates or removes a mount's directory.
func TestMountDirectoryRefusesALinkOutOfTheCommDirectory(t *testing.T) {
	commDir := t.TempDir()
	outside := t.TempDir()
	assert.NoError(t, os.Symlink(outside, filepath.Join(commDir, MountsDirName)))
	// A file the credential provider's cleanup removes by name, so it shows whether that cleanup ran through the link.
	keep := filepath.Join(outside, "pv-a", "credentials", "token")
	assert.NoError(t, os.MkdirAll(filepath.Dir(keep), 0o700))
	assert.NoError(t, os.WriteFile(keep, nil, 0o600))
	dm := &DaemonsetMounter{credProvider: credentialprovider.New(nil, nil)}

	if err := dm.cleanupCredentials(commDir, "pv-a", credentialprovider.CleanupContext{}); err == nil {
		t.Error("expected removing a mount's directory through a planted link to be refused")
	}
	if _, err := dm.ensureCredentialsDirOwnedBy(commDir, "pv-b", 65536); err == nil {
		t.Error("expected creating a mount's directory through a planted link to be refused")
	}

	_, err := os.Stat(keep)
	assert.NoError(t, err)
	assert.FileNotExists(t, filepath.Join(outside, "pv-b"))
}

// Runs the real chown and chmod, so this covers the [os.Root] resolution that applies ownership inside the comm
// directory and refuses a path through a link planted out of it.
func TestOwnIn(t *testing.T) {
	commDir := t.TempDir()
	root, err := os.OpenRoot(commDir)
	assert.NoError(t, err)
	defer root.Close()

	inside := filepath.Join(commDir, MountsDirName, "pv-a", "credentials")
	assert.NoError(t, os.MkdirAll(inside, 0o755))
	assert.NoError(t, (&DaemonsetMounter{}).ownIn(root, filepath.Join(MountsDirName, "pv-a", "credentials"), os.Getuid(), os.Getgid(), 0o700))
	st, err := os.Stat(inside)
	assert.NoError(t, err)
	assert.Equals(t, fs.FileMode(0o700), st.Mode().Perm())

	outside := t.TempDir()
	victim := filepath.Join(outside, "pv-b", "credentials")
	assert.NoError(t, os.MkdirAll(victim, 0o755))
	assert.NoError(t, os.Symlink(filepath.Join(outside, "pv-b"), filepath.Join(commDir, MountsDirName, "pv-b")))
	// Each real call on its own, as a chown to its own UID changes nothing visible to check afterwards.
	for op, dm := range map[string]*DaemonsetMounter{
		"chown": {chmod: func(string, fs.FileMode) error { return nil }},
		"chmod": {chown: func(string, int, int) error { return nil }},
	} {
		if err := dm.ownIn(root, filepath.Join(MountsDirName, "pv-b", "credentials"), os.Getuid(), os.Getgid(), 0o700); err == nil {
			t.Errorf("expected the %s through a planted link to be refused", op)
		}
	}
	st, err = os.Stat(victim)
	assert.NoError(t, err)
	assert.Equals(t, fs.FileMode(0o755), st.Mode().Perm())
}

// The mounter can swap mounts/ for a link at any moment, so ensureCredentialsDirOwnedBy must not be led outside the
// comm directory by one swapped in after it creates the mount's directories.
func TestEnsureCredentialsDirOwnedBy_KeepsOwnershipInsideTheCommDir(t *testing.T) {
	for _, tc := range []struct {
		name string
		// The swap happens in the chown or chmod hook of `at`; the other call is real.
		swapOnChown bool
		at          string
		// What a call through the link would touch: an existing directory it would chmod, or one it would create.
		victim, victimExisting string
	}{
		{name: "after the chown of the mount's directory", swapOnChown: true, at: "mounts/pv-a", victim: "pv-a", victimExisting: "pv-a"},
		{name: "after the chown of its credentials", swapOnChown: true, at: "mounts/pv-a/credentials", victim: "pv-a/credentials", victimExisting: "pv-a/credentials"},
		{name: "before the credentials are created", at: "mounts/pv-a", victim: "pv-a/credentials", victimExisting: "pv-a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			commDir := t.TempDir()
			outside := t.TempDir()
			assert.NoError(t, os.MkdirAll(filepath.Join(outside, tc.victimExisting), 0o755))
			swapped := false
			swap := func(path string) {
				if path == filepath.Join(commDir, tc.at) && !swapped {
					swapped = true
					assert.NoError(t, os.Rename(filepath.Join(commDir, MountsDirName), filepath.Join(commDir, "mounts-moved")))
					assert.NoError(t, os.Symlink(outside, filepath.Join(commDir, MountsDirName)))
				}
			}
			dm := &DaemonsetMounter{chown: func(path string, uid, gid int) error { swap(path); return nil }}
			if !tc.swapOnChown {
				dm.chown = func(string, int, int) error { return nil }
				dm.chmod = func(path string, mode fs.FileMode) error { swap(path); return nil }
			}

			_, err := dm.ensureCredentialsDirOwnedBy(commDir, "pv-a", 65536)
			assert.Equals(t, true, swapped)
			if err == nil {
				t.Error("expected the call through the swapped-in link to be refused")
			}
			victim := filepath.Join(outside, tc.victim)
			if tc.victim == tc.victimExisting {
				st, err := os.Stat(victim)
				assert.NoError(t, err)
				assert.Equals(t, fs.FileMode(0o755), st.Mode().Perm())
			} else {
				assert.FileNotExists(t, victim)
			}
		})
	}
}

func TestCleanupCredentials_CommDirGone(t *testing.T) {
	dm := &DaemonsetMounter{credProvider: credentialprovider.New(nil, nil)}
	// A replaced mounter pod takes its comm directory with it; the mounts made through it must still clean up.
	assert.NoError(t, dm.cleanupCredentials(filepath.Join(t.TempDir(), "gone"), "pv-a", credentialprovider.CleanupContext{}))
}
