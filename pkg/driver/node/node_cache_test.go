// In `package node` rather than `package node_test` because `configureCacheForDaemonsetMode` is
// unexported.
package node

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/driver/node/mounter"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/driver/node/volumecontext"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/mountpoint"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/util/testutil/assert"
	"k8s.io/utils/ptr"
)

func TestConfigureCacheForDaemonsetMode(t *testing.T) {
	// This function decides *whether* a mount caches; DaemonsetMounter.Mount names the directory,
	// so `--cache` is expected valueless here. Keeping both in one place is what stops them
	// disagreeing - they used to be derived separately, and a mismatch pointed Mountpoint at a
	// directory nothing created. See the cache directory test in daemonset_mounter_test.go.
	const volumeID = "test-volume-id"

	// The node's cache, defaulting to disk-backed emptyDir when a case does not say otherwise.
	emptyDirNode := mounter.CacheEmptyDirDisk
	tmpfsNode := mounter.CacheEmptyDirMemory
	ephemeralNode := mounter.CacheEphemeral

	testCases := []struct {
		name         string
		mountOptions []string
		volumeCtx    map[string]string
		nodeCache    *mounter.CacheType // nil means emptyDirNode
		expectedArgs []string
		expectError  bool
	}{

		{
			name:         "requests a cache when the attribute matches the node's emptyDir",
			volumeCtx:    map[string]string{volumecontext.Cache: volumecontext.CacheTypeEmptyDir},
			nodeCache:    &emptyDirNode,
			expectedArgs: []string{"--cache"},
		},
		{
			name: "requests a cache when the attribute matches the node's tmpfs",
			volumeCtx: map[string]string{
				volumecontext.Cache:               volumecontext.CacheTypeEmptyDir,
				volumecontext.CacheEmptyDirMedium: "Memory",
			},
			nodeCache:    &tmpfsNode,
			expectedArgs: []string{"--cache"},
		},
		{
			name:         "requests a cache when the attribute matches the node's ephemeral volume",
			volumeCtx:    map[string]string{volumecontext.Cache: volumecontext.CacheTypeEphemeral},
			nodeCache:    &ephemeralNode,
			expectedArgs: []string{"--cache"},
		},
		{
			// Substituting the node's disk for a tmpfs would change how the cache behaves when full,
			// so the medium is part of the type rather than an ignored sizing hint.
			name: "rejects a tmpfs request on a disk-backed node",
			volumeCtx: map[string]string{
				volumecontext.Cache:               volumecontext.CacheTypeEmptyDir,
				volumecontext.CacheEmptyDirMedium: "Memory",
			},
			nodeCache:   &emptyDirNode,
			expectError: true,
		},
		{
			name:        "rejects a disk request on a tmpfs node",
			volumeCtx:   map[string]string{volumecontext.Cache: volumecontext.CacheTypeEmptyDir},
			nodeCache:   &tmpfsNode,
			expectError: true,
		},
		{
			name:        "rejects an emptyDir request on an ephemeral node",
			volumeCtx:   map[string]string{volumecontext.Cache: volumecontext.CacheTypeEmptyDir},
			nodeCache:   &ephemeralNode,
			expectError: true,
		},
		{
			name:        "rejects an ephemeral request on an emptyDir node",
			volumeCtx:   map[string]string{volumecontext.Cache: volumecontext.CacheTypeEphemeral},
			nodeCache:   &emptyDirNode,
			expectError: true,
		},
		{
			// The medium is meaningless for ephemeral, so it is ignored rather than required empty.
			name: "ignores the medium when the type is ephemeral",
			volumeCtx: map[string]string{
				volumecontext.Cache:               volumecontext.CacheTypeEphemeral,
				volumecontext.CacheEmptyDirMedium: "Memory",
			},
			nodeCache:    &ephemeralNode,
			expectedArgs: []string{"--cache"},
		},
		{
			// Was accepted before the type had to match; `true` names no backing.
			name:        "rejects `true`, which is not a cache type",
			volumeCtx:   map[string]string{volumecontext.Cache: "true"},
			expectError: true,
		},
		{
			name:        "rejects an unknown cache type",
			volumeCtx:   map[string]string{volumecontext.Cache: "emtpyDir"},
			expectError: true,
		},
		{
			name:        "rejects a cache request when the node has no cache volume",
			volumeCtx:   map[string]string{volumecontext.Cache: volumecontext.CacheTypeEmptyDir},
			nodeCache:   ptr.To(mounter.CacheNone),
			expectError: true,
		},
		{
			// The deprecated mount option cannot express a type, and it is the surface every existing
			// cache user is on, so it takes whatever the node provides.
			name:         "the deprecated mount option accepts the node's tmpfs",
			mountOptions: []string{"cache /tmp/customer-path"},
			volumeCtx:    map[string]string{},
			nodeCache:    &tmpfsNode,
			expectedArgs: []string{"--cache"},
		},
		{
			name:         "does not request a cache when no volume attribute or mount option asks for one",
			volumeCtx:    map[string]string{},
			expectedArgs: []string{},
		},
		{
			name: "discards the path of a `cache` mount option",
			// A path from the customer would point into this pod, not the mounter's.
			mountOptions: []string{"cache /tmp/customer-path"},
			volumeCtx:    map[string]string{},
			expectedArgs: []string{"--cache"},
		},
		{
			name:         "rejects a cache configured with both a mount option and a volume attribute",
			mountOptions: []string{"cache /tmp/customer-path"},
			volumeCtx:    map[string]string{volumecontext.Cache: volumecontext.CacheTypeEmptyDir},
			expectError:  true,
		},
		{
			name:         "keeps max-cache-size for a cached volume",
			mountOptions: []string{"max-cache-size 1024"},
			volumeCtx:    map[string]string{volumecontext.Cache: volumecontext.CacheTypeEmptyDir},
			expectedArgs: []string{"--cache", "--max-cache-size=1024"},
		},
		{
			name: "drops max-cache-size when the cache is not enabled",
			// Mountpoint rejects `--max-cache-size` without `--cache`, which would fail the
			// mount inside the mounter with an error the customer cannot act on.
			mountOptions: []string{"max-cache-size 1024"},
			volumeCtx:    map[string]string{},
			expectedArgs: []string{},
		},
		{
			name: "ignores the per-mount cache sizing attributes without leaking them into args",
			volumeCtx: map[string]string{
				volumecontext.Cache:                                volumecontext.CacheTypeEmptyDir,
				volumecontext.CacheEmptyDirMedium:                  "Memory",
				volumecontext.CacheEmptyDirSizeLimit:               "10Gi",
				volumecontext.CacheEphemeralStorageClassName:       "gp3",
				volumecontext.CacheEphemeralStorageResourceRequest: "10Gi",
			},
			nodeCache:    &tmpfsNode,
			expectedArgs: []string{"--cache"},
		},
		{
			name:         "leaves unrelated mount options alone",
			mountOptions: []string{"region us-west-2"},
			volumeCtx:    map[string]string{volumecontext.Cache: volumecontext.CacheTypeEmptyDir},
			expectedArgs: []string{"--cache", "--region=us-west-2"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			args := mountpoint.ParseArgs(testCase.mountOptions)

			nodeCache := emptyDirNode
			if testCase.nodeCache != nil {
				nodeCache = *testCase.nodeCache
			}

			err := configureCacheForDaemonsetMode(&args, testCase.volumeCtx, volumeID, nodeCache)

			if testCase.expectError {
				// The code matters: kubelet retries Internal but treats InvalidArgument as terminal.
				assert.Equals(t, codes.InvalidArgument, status.Code(err))
				return
			}
			assert.NoError(t, err)
			assert.Equals(t, testCase.expectedArgs, args.SortedList())
		})
	}
}
