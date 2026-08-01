// Tests that volume-limit-triggered autoscaling works correctly.
// Requires a cluster with an autoscaler (Karpenter or Cluster Autoscaler)
// that respects CSINode allocatable volume limits.

package custom_testsuites

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	admissionapi "k8s.io/pod-security-admission/api"
)

type s3CSIAutoscalerTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

const (
	// Time to wait for autoscaler to provision new nodes and pods to become Running.
	autoscalerScaleUpTimeout = 5 * time.Minute

	// Time to poll for new node appearance.
	autoscalerNodePollInterval = 5 * time.Second
)

var _ storageframework.TestSuite = &s3CSIAutoscalerTestSuite{}

// InitS3CSIAutoscalerTestSuite returns s3CSIAutoscalerTestSuite that implements TestSuite interface.
func InitS3CSIAutoscalerTestSuite() storageframework.TestSuite {
	return &s3CSIAutoscalerTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name: "autoscaler",
			TestPatterns: []storageframework.TestPattern{
				storageframework.DefaultFsPreprovisionedPV,
			},
		},
	}
}

func (t *s3CSIAutoscalerTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return t.tsInfo
}

func (t *s3CSIAutoscalerTestSuite) SkipUnsupportedTests(driver storageframework.TestDriver, _ storageframework.TestPattern) {
	dInfo := driver.GetDriverInfo()
	if !dInfo.Capabilities[storageframework.CapVolumeLimits] {
		e2eskipper.Skipf("Driver %s does not support volume limits -- autoscaler test requires volume limits", dInfo.Name)
	}
	if ClusterType != "karpenter" {
		e2eskipper.Skipf("Autoscaler test requires cluster-type=karpenter, got %q", ClusterType)
	}
}

func (t *s3CSIAutoscalerTestSuite) DefineTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {
	type local struct {
		resources []*storageframework.VolumeResource
		config    *storageframework.PerTestConfig
	}
	var l local

	f := framework.NewFrameworkWithCustomTimeouts(NamespacePrefix+"autoscaler", storageframework.GetDriverTimeouts(driver))
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	cleanup := func(ctx context.Context) {
		var errs []error
		for _, resource := range l.resources {
			errs = append(errs, resource.CleanupResource(ctx))
		}
		framework.ExpectNoError(errors.NewAggregate(errs), "while cleanup resource")
	}
	ginkgo.BeforeEach(func(ctx context.Context) {
		l = local{}
		l.config = driver.PrepareTest(ctx, f)
		ginkgo.DeferCleanup(cleanup)

		// Remove any Karpenter nodes from previous tests so each spec
		// starts from a known baseline (managed nodes only).
		ginkgo.By("Deleting existing Karpenter nodeclaims to reset to baseline")
		deleteKarpenterNodes(ctx, f)
	})

	// Creates 3 * maxVolumesPerNode pods (each with 1 PV). The initial nodes cannot
	// fit all of them, so the autoscaler must provision at least one new node.
	// Validates that all pods become Running and volume limits are respected on every node.
	f.It("should scale up nodes when volume limits are exceeded", f.WithSerial(), func(ctx context.Context) {
		driverInfo := driver.GetDriverInfo()

		ginkgo.By("Getting initial node count")
		initialNodes, err := e2enode.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err)
		initialNodeCount := len(initialNodes.Items)
		framework.Logf("Initial schedulable node count: %d", initialNodeCount)
		if initialNodeCount == 0 {
			framework.Failf("No ready schedulable nodes found -- cannot determine volume limits")
		}

		ginkgo.By("Checking volume limit from CSINode")
		nodeName := initialNodes.Items[0].Name
		limit, err := getCSINodeLimits(ctx, f, nodeName, driverInfo.Name)
		framework.ExpectNoError(err)
		framework.Logf("Volume limit per node: %d", limit)

		// Create (initialNodeCount + 1) * limit pods -- always exceeds current
		// capacity by one node's worth, guaranteeing at least one scale-up.
		totalPods := limit * (initialNodeCount + 1)
		framework.Logf("Creating %d pods ((%d nodes + 1) * %d limit) to trigger scale-up", totalPods, initialNodeCount, limit)

		ginkgo.By(fmt.Sprintf("Creating %d pods with 1 volume each", totalPods))
		var pods []*v1.Pod
		for i := range totalPods {
			resource := createVolumeResourceWithMountOptions(ctx, l.config, pattern, nil)
			l.resources = append(l.resources, resource)

			pod := e2epod.MakePod(f.Namespace.Name, nil, []*v1.PersistentVolumeClaim{resource.Pvc}, admissionapi.LevelBaseline, "")
			pod.Name = fmt.Sprintf("autoscaler-pod-%d", i)
			pod, err = createPodWithoutWaiting(ctx, f.ClientSet, f.Namespace.Name, pod)
			framework.ExpectNoError(err, "creating pod %d", i)
			pods = append(pods, pod)
		}
		ginkgo.DeferCleanup(func(ctx context.Context) {
			for _, pod := range pods {
				e2epod.DeletePodWithWait(ctx, f.ClientSet, pod)
			}
		})

		ginkgo.By("Waiting for all pods to become Running (autoscaler should provision new nodes)")
		for _, pod := range pods {
			err := e2epod.WaitTimeoutForPodRunningInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name, autoscalerScaleUpTimeout)
			framework.ExpectNoError(err, "pod %s did not become Running within timeout -- autoscaler may have failed to scale up", pod.Name)
		}

		ginkgo.By("Verifying node count increased")
		finalNodes, err := e2enode.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err)
		finalNodeCount := len(finalNodes.Items)
		framework.Logf("Final schedulable node count: %d (was %d)", finalNodeCount, initialNodeCount)
		if finalNodeCount <= initialNodeCount {
			framework.Failf("Expected node count to increase from %d, but got %d -- autoscaler did not provision new nodes", initialNodeCount, finalNodeCount)
		}

		ginkgo.By("Verifying all nodes have volume limits in CSINode")
		for _, node := range finalNodes.Items {
			nodeLimit, err := getCSINodeLimits(ctx, f, node.Name, driverInfo.Name)
			if err != nil {
				framework.Failf("Node %s does not have volume limits in CSINode: %v", node.Name, err)
			}
			framework.Logf("Node %s: allocatable.count = %d", node.Name, nodeLimit)
		}

		ginkgo.By("Verifying no node exceeds its volume limit")
		err = verifyVolumeDistribution(ctx, f, driverInfo.Name, f.Namespace.Name)
		framework.ExpectNoError(err)
	})
}

// deleteKarpenterNodes removes all Karpenter-provisioned nodes by deleting their
// NodeClaims (Karpenter's canonical lifecycle API) and waits for the nodes to
// terminate, so the next test starts from a clean baseline (managed nodes only).
//
// Karpenter will not re-provision during cleanup because:
// 1. DaemonSet pods don't trigger scale-up (not "pending unschedulable")
// 2. Previous test pods are cleaned via DeferCleanup before this BeforeEach runs
func deleteKarpenterNodes(ctx context.Context, f *framework.Framework) {
	nodeClaimGVR := schema.GroupVersionResource{
		Group:    "karpenter.sh",
		Version:  "v1",
		Resource: "nodeclaims",
	}

	// Delete all NodeClaims (Karpenter will drain and terminate the nodes).
	nodeClaims, err := f.DynamicClient.Resource(nodeClaimGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		framework.Logf("Warning: could not list NodeClaims: %v", err)
		return
	}
	if len(nodeClaims.Items) == 0 {
		framework.Logf("No Karpenter NodeClaims to clean up")
		return
	}
	framework.Logf("Deleting %d Karpenter NodeClaim(s)...", len(nodeClaims.Items))
	for _, nc := range nodeClaims.Items {
		err := f.DynamicClient.Resource(nodeClaimGVR).Delete(ctx, nc.GetName(), metav1.DeleteOptions{})
		if err != nil {
			framework.Logf("Warning: could not delete NodeClaim %s: %v", nc.GetName(), err)
		}
	}

	// Wait for Karpenter nodes to disappear.
	err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		remaining, err := f.ClientSet.CoreV1().Nodes().List(ctx, metav1.ListOptions{
			LabelSelector: "karpenter.sh/nodepool",
		})
		if err != nil {
			return false, nil
		}
		if len(remaining.Items) > 0 {
			framework.Logf("Waiting for %d Karpenter node(s) to terminate...", len(remaining.Items))
			return false, nil
		}
		return true, nil
	})
	framework.ExpectNoError(err, "timed out waiting for Karpenter nodes to terminate -- cluster may be unhealthy")
}

// verifyVolumeDistribution checks that no node has more pods (each assumed to use
// exactly one CSI volume) than its advertised CSINode allocatable limit.
func verifyVolumeDistribution(ctx context.Context, f *framework.Framework, driverName, namespace string) error {
	pods, err := f.ClientSet.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("listing pods: %w", err)
	}

	// Count test pods per node (filter by name prefix to exclude framework pods).
	podsPerNode := make(map[string]int)
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase == v1.PodRunning && pod.Spec.NodeName != "" && strings.HasPrefix(pod.Name, "autoscaler-pod-") {
			podsPerNode[pod.Spec.NodeName]++
		}
	}

	// Compare against CSINode limits.
	var errs []error
	for nodeName, count := range podsPerNode {
		var limit int
		err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			csiNode, err := f.ClientSet.StorageV1().CSINodes().Get(ctx, nodeName, metav1.GetOptions{})
			if err != nil {
				return false, nil
			}
			for _, d := range csiNode.Spec.Drivers {
				if d.Name == driverName && d.Allocatable != nil && d.Allocatable.Count != nil {
					limit = int(*d.Allocatable.Count)
					return true, nil
				}
			}
			return false, nil
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("node %s: could not get volume limit: %w", nodeName, err))
			continue
		}
		if count > limit {
			errs = append(errs, fmt.Errorf("node %s has %d volumes but limit is %d", nodeName, count, limit))
		}
		framework.Logf("Node %s: %d/%d volumes used", nodeName, count, limit)
	}
	return errors.NewAggregate(errs)
}
