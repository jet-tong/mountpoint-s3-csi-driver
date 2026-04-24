package mounter

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/cluster"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/podmounter/mppod/watcher"
	"github.com/awslabs/mountpoint-s3-csi-driver/pkg/util/testutil/assert"
)

const (
	testNamespace = "mount-s3"
	testNode      = "node-a"
)

func TestDaemonsetMounter(t *testing.T) {
	t.Run("Creates a valid mounter", func(t *testing.T) {
		client := fake.NewClientset()
		w := startWatcher(t, client)

		dm, err := NewDaemonsetMounter(w, nil, nil, "v1.36.0", "test-node", cluster.DefaultKubernetes)
		assert.NoError(t, err)
		assert.Equals(t, true, dm != nil)
	})

	t.Run("Finding the DaemonSet pod", func(t *testing.T) {
		client := fake.NewClientset(
			&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "s3-csi-mountpoint-abcde",
					Namespace: testNamespace,
					Labels:    map[string]string{daemonsetLabelKey: daemonsetLabelValue},
				},
				Spec: corev1.PodSpec{NodeName: testNode},
			},
		)
		w := startWatcher(t, client)

		name, err := getDaemonsetMounterPodName(w)
		assert.NoError(t, err)
		assert.Equals(t, "s3-csi-mountpoint-abcde", name)
	})

	// TODO: add test groups as daemonset mode implementation grows:
	// t.Run("Mounting", func(t *testing.T) { ... })
	//   - Correctly passes mount options
	//   - Uses VolumeID for source path (not pod name)
	//   - Handles already-mounted target (credential refresh)
	// t.Run("Unmounting", func(t *testing.T) { ... })
	//   - Cleans up source FUSE mount when refcount reaches zero
} // end TestDaemonsetMounter

func startWatcher(t *testing.T, client *fake.Clientset) *watcher.Watcher {
	t.Helper()
	w := watcher.New(client, testNamespace, testNode, time.Minute)
	stopCh := make(chan struct{})
	t.Cleanup(func() { close(stopCh) })
	err := w.Start(stopCh)
	assert.NoError(t, err)
	return w
}
