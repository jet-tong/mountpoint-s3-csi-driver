package custom_testsuites

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	admissionapi "k8s.io/pod-security-admission/api"
)

// This suite reconfigures the mounter DaemonSet's cache volume via Helm, which is a cluster-wide
// mutation: restarting mounter pods kills every Mountpoint process on every node. It is therefore
// Serial, and Ordered so the reconfigurations happen in a fixed sequence. `ginkgo -p` runs Serial
// specs last on a single process, so no parallel-phase workload can be running.
//
// A spec failure skips the remaining specs in the container (Ginkgo's Ordered semantics) but cleanup
// still runs, so the cluster is restored. If the restore itself fails, recover with:
//
//	helm upgrade aws-mountpoint-s3-csi-driver -n kube-system ./charts/aws-mountpoint-s3-csi-driver \
//	    --values ./charts/aws-mountpoint-s3-csi-driver/values.yaml --reuse-values \
//	  && kubectl delete pod -n kube-system -l app=s3-csi-daemonset-mounter

// daemonsetCacheStorageClassName is fixed rather than derived from f.UniqueName: inside an Ordered
// container that would be the first spec's namespace, which is deleted before the container ends,
// and this is a cluster-scoped resource living for the whole container.
const daemonsetCacheStorageClassName = "s3-csi-e2e-daemonset-cache-sc"

type s3CSIDaemonsetCacheTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

func InitS3CSIDaemonsetCacheTestSuite() storageframework.TestSuite {
	return &s3CSIDaemonsetCacheTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name: "daemonset-cache",
			TestPatterns: []storageframework.TestPattern{
				storageframework.DefaultFsPreprovisionedPV,
			},
		},
	}
}

func (t *s3CSIDaemonsetCacheTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return t.tsInfo
}

func (t *s3CSIDaemonsetCacheTestSuite) SkipUnsupportedTests(_ storageframework.TestDriver, pattern storageframework.TestPattern) {
	if pattern.VolType != storageframework.PreprovisionedPV {
		e2eskipper.Skipf("Suite %q does not support %v", t.tsInfo.Name, pattern.VolType)
	}
}

func (t *s3CSIDaemonsetCacheTestSuite) DefineTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {
	f := framework.NewFrameworkWithCustomTimeouts(NamespacePrefix+"daemonset-cache", storageframework.GetDriverTimeouts(driver))
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	Describe("Cache backings on the mounter DaemonSet", Ordered, Serial, func() {
		var (
			config       *storageframework.PerTestConfig
			helmCfg      *action.Configuration
			helmRelease  string
			helmChart    *chart.Chart
			baseValues   map[string]any
			baseMounter  map[string]any
			nodeImage    string
			ebsAvailable bool
		)

		BeforeAll(func(ctx context.Context) {
			if !isDaemonsetMounterMode(ctx, f) {
				Skip("cache backings are configured on the mounter DaemonSet, which only exists in daemonset mode")
			}

			_, helmCfg = initHelmClient()

			var err error
			helmChart, err = loader.Load(helmChartSource)
			framework.ExpectNoError(err, "loading the working-tree chart")

			// Captured before any upgrade: this is both what we build each reconfiguration from and
			// the restore payload.
			helmRelease = csiDriverReleaseName(helmCfg)
			baseValues = currentReleaseValues(helmCfg, helmRelease)
			baseMounter = mounterElement(helmChart, baseValues)
			nodeImage = csiDriverNodeImage(ctx, f)

			// DeferCleanup registered in BeforeAll runs *after* the framework's AfterEach, which
			// nils f.ClientSet - so the cleanups below must not touch f. Every helper they call uses
			// only ClientSet, so a framework carrying just that survives teardown.
			cleanupF := &framework.Framework{ClientSet: f.ClientSet}

			// Registered first, so LIFO ordering runs it last - after the Helm restore has dropped
			// the mounter pods, which garbage-collects any ephemeral cache PVC via its owner ref.
			deleteSC := createEBSCacheStorageClass(ctx, cleanupF, daemonsetCacheStorageClassName)
			DeferCleanup(deleteSC)

			DeferCleanup(func(ctx context.Context) {
				By("Restoring the original cache configuration")

				// Only the Helm state matters for later specs; if the pod restart is what fails, the
				// release is already correct and a plain delete recovers it.
				helmRestored := false
				defer func() {
					if r := recover(); r != nil {
						recovery := "kubectl delete pod -n kube-system -l app=s3-csi-daemonset-mounter"
						if !helmRestored {
							recovery = fmt.Sprintf("helm upgrade %s -n %s %s --reuse-values && %s",
								helmRelease, helmReleaseNamespace, helmChartSource, recovery)
						}
						fmt.Printf("::error file=cache_daemonset.go::Failed to restore the mounter DaemonSet's"+
							" cache configuration. Later Serial specs may fail with \"no cache volume\"."+
							" Recover with: %s\n", recovery)
						panic(r)
					}
				}()

				// Restores the element complete, keeping whatever cache it originally specified. Sending
				// baseValues verbatim would write back a partial element if a previous run left one.
				var originalCache map[string]any
				if c, ok := baseMounter["cache"].(map[string]any); ok {
					originalCache = c
				}
				upgradeCacheConfig(ctx, helmCfg, helmRelease, helmChart, withCacheBlock(baseValues, baseMounter, originalCache))
				helmRestored = true
				framework.ExpectNoError(waitForCSIDriverDaemonSetRollout(ctx, cleanupF), "waiting for the node DaemonSet")
				restartMounterPodsAndWait(ctx, cleanupF)
			})

			ebsAvailable = ebsCSIDriverDaemonSet(ctx, f) != nil
		})

		BeforeEach(func(ctx context.Context) {
			config = driver.PrepareTest(ctx, f)
		})

		// reconfigure swaps the mounter's cache block and makes the change take effect. The Helm
		// upgrade alone does nothing: the DaemonSet's updateStrategy is OnDelete, so it reports Ready
		// with the old pods and Helm's own readiness check short-circuits for a non-RollingUpdate
		// DaemonSet without ever observing them.
		reconfigure := func(ctx context.Context, cache map[string]any) {
			upgradeCacheConfig(ctx, helmCfg, helmRelease, helmChart, withCacheBlock(baseValues, baseMounter, cache))
			framework.ExpectNoError(waitForCSIDriverDaemonSetRollout(ctx, f), "waiting for the node DaemonSet")
			restartMounterPodsAndWait(ctx, f)

			// The upgrade must not have dropped the image overrides the CI install carries; if it
			// had, the driver would silently revert to the last released image.
			gomega.Expect(csiDriverNodeImage(ctx, f)).To(gomega.Equal(nodeImage),
				"the Helm upgrade changed the driver image, so the values round-trip lost an override")

			assertMounterCacheVolume(ctx, f, cache)
		}

		// cachedPodOnNode creates a workload whose PV asks for a cache, and returns it with the
		// mounter pod sharing its node.
		cachedPodOnNode := func(ctx context.Context, attrs map[string]string) (*v1.Pod, *v1.Pod, string) {
			ctx = contextWithVolumeAttributes(ctx, attrs)
			vol := createVolumeResourceWithMountOptions(ctx, config, pattern, []string{"allow-delete"})
			DeferCleanup(vol.CleanupResource)

			pod := e2epod.MakePod(f.Namespace.Name, nil, []*v1.PersistentVolumeClaim{vol.Pvc}, admissionapi.LevelBaseline, "")
			pod, err := createPod(ctx, f.ClientSet, f.Namespace.Name, pod)
			framework.ExpectNoError(err)
			DeferCleanup(func(ctx context.Context) error { return e2epod.DeletePodWithWait(ctx, f.ClientSet, pod) })

			pvName := vol.Pv.Name
			return pod, mounterPodOnNode(ctx, f, pod.Spec.NodeName), pvName
		}

		It("uses a tmpfs cache when the backing is emptyDir with medium Memory", func(ctx context.Context) {
			reconfigure(ctx, map[string]any{
				"type":     "emptyDir",
				"emptyDir": map[string]any{"medium": "Memory", "sizeLimit": "128Mi"},
			})

			// The PV must name the node's backing exactly, medium included.
			pod, mounter, pvName := cachedPodOnNode(ctx, map[string]string{
				"cache": "emptyDir", "cacheEmptyDirMedium": "Memory",
			})

			commFS := statfsInMounter(ctx, f, mounter, "/comm")
			cacheFS := statfsInMounter(ctx, f, mounter, "/cache")
			gomega.Expect(cacheFS.fsType).To(gomega.Equal("tmpfs"))
			// sizeLimit reaches the tmpfs, which is what stops the kubelet sizing it to
			// node-allocatable memory. df rounds, so allow a little slack.
			gomega.Expect(cacheFS.sizeKiB).To(gomega.BeNumerically("~", 128*1024, 4*1024))
			gomega.Expect(cacheFS.device).NotTo(gomega.Equal(commFS.device),
				"a tmpfs cache must be a different filesystem from the comm volume")

			assertMountpointCacheDir(ctx, f, mounter, pvName)
			checkCachedRead(ctx, f, pod)
		})

		It("uses the provisioned volume when the backing is ephemeral", func(ctx context.Context) {
			if !ebsAvailable {
				// ebsCSIDriverDaemonSet looks for `ebs-csi-node` in kube-system; OpenShift names it
				// differently and puts it elsewhere, so this skips on ROSA. That is the intended
				// outcome there, but verify the namespace before trusting the gate elsewhere.
				// TODO NOT SKIP IT FOR ROSA
				Skip("the EBS CSI driver is not installed, so an ephemeral cache volume cannot be provisioned")
			}

			reconfigure(ctx, map[string]any{
				"type":      "ephemeral",
				"ephemeral": map[string]any{"storageClassName": daemonsetCacheStorageClassName, "size": "1Gi"},
			})

			pod, mounter, pvName := cachedPodOnNode(ctx, map[string]string{"cache": "ephemeral"})

			// The claim the ephemeral-volume controller creates for the mounter pod.
			pvc, err := f.ClientSet.CoreV1().PersistentVolumeClaims(csiDriverDaemonSetNamespace).
				Get(ctx, mounter.Name+"-cache", metav1.GetOptions{})
			framework.ExpectNoError(err, "the mounter's ephemeral cache claim should exist")
			gomega.Expect(pvc.Status.Phase).To(gomega.Equal(v1.ClaimBound))

			commFS := statfsInMounter(ctx, f, mounter, "/comm")
			cacheFS := statfsInMounter(ctx, f, mounter, "/cache")
			gomega.Expect(cacheFS.device).NotTo(gomega.Equal(commFS.device),
				"an ephemeral cache must be its own device, so filling it cannot reach the node's disk")
			gomega.Expect(cacheFS.sizeKiB).To(gomega.BeNumerically(">", 512*1024))

			// fsGroup makes the provisioned root group-writable without making it world-writable.
			// Omitting fsGroup on OpenShift is what broke this before.
			mode := statInMounter(ctx, f, mounter, "%A", "/cache")
			gomega.Expect(mode).To(gomega.HavePrefix("drwxrw"), "cache volume root %q is not group-writable", mode)
			gomega.Expect(mode).NotTo(gomega.HaveSuffix("rwx"), "cache volume root %q is world-writable", mode)

			assertMountpointCacheDir(ctx, f, mounter, pvName)
			checkCachedRead(ctx, f, pod)
		})
	})
}

// --- Helm helpers -----------------------------------------------------------------------------

// csiDriverReleaseName finds the installed release of this chart. Discovered rather than taken from
// helmReleaseName, because that constant is what CI installs (scripts/run.sh) while dev clusters use
// a different name (dev/mp-dev.sh installs `aws-mountpoint-s3-csi-driver`), and hardcoding either
// makes the suite pass in one place and fail in the other.
func csiDriverReleaseName(cfg *action.Configuration) string {
	list := action.NewList(cfg)
	list.All = true
	list.SetStateMask()
	releases, err := list.Run()
	framework.ExpectNoError(err, "listing Helm releases")

	var names []string
	for _, r := range releases {
		if r.Chart != nil && r.Chart.Metadata != nil && r.Chart.Metadata.Name == helmChartName {
			names = append(names, r.Name)
		}
	}
	if len(names) != 1 {
		Fail(fmt.Sprintf("expected exactly one installed %q release, found %v."+
			" This suite upgrades the release in place and cannot guess which one to touch.",
			helmChartName, names))
	}
	framework.Logf("Found the driver's Helm release: %s", names[0])
	return names[0]
}

// currentReleaseValues returns the release's user-supplied values: the overrides an in-place upgrade
// must resend, and the restore payload. The CI install carries image and service-account overrides
// that are not chart defaults, and hand-building a values map would silently drop them. AllValues is
// off so chart defaults stay coalesced by Helm rather than written back as user overrides.
func currentReleaseValues(cfg *action.Configuration, release string) map[string]any {
	get := action.NewGetValues(cfg)
	get.AllValues = false
	vals, err := get.Run(release)
	framework.ExpectNoError(err, "reading the user-supplied values of release %q", release)
	return vals
}

// mounterElement returns a complete daemonsetMounters[0] element: the chart's own default, with any
// user-supplied element overlaid on top.
//
// It must start from the chart rather than the release's coalesced values. Helm *replaces* a list
// rather than merging it, so once this suite has written a partial element into the release's
// user-supplied values, the coalesced value is that partial element and the chart's default becomes
// unreachable - each run stripping it further until the rendered mounter is missing required fields
// (an absent logLevel renders `--v=`, and the container exits 2 on the unparseable flag). Building
// from the chart makes that self-healing.
func mounterElement(ch *chart.Chart, userVals map[string]any) map[string]any {
	element := deepCopyMap(singleMounterElement(ch.Values, "the chart's values.yaml"))
	if user, ok := userVals["daemonsetMounters"]; ok {
		for k, v := range singleMounterElement(map[string]any{"daemonsetMounters": user}, "the release's user-supplied values") {
			element[k] = deepCopyValue(v)
		}
	}
	return element
}

func singleMounterElement(vals map[string]any, source string) map[string]any {
	mounters, ok := vals["daemonsetMounters"].([]any)
	if !ok || len(mounters) != 1 {
		Fail(fmt.Sprintf("expected exactly one daemonsetMounters entry in %s, got %#v."+
			" The install shape changed, so this suite's values assumptions are void.", source, vals["daemonsetMounters"]))
	}
	element, ok := mounters[0].(map[string]any)
	if !ok {
		Fail(fmt.Sprintf("daemonsetMounters[0] in %s is not a map: %#v", source, mounters[0]))
	}
	return element
}

// withCacheBlock returns a deep copy of userVals with daemonsetMounters set to one complete element
// whose cache block is replaced by cache, or removed when cache is nil. Always resent complete,
// including on restore, because Helm replaces a whole list element rather than merging it - sending a
// partial element is what poisons later runs.
func withCacheBlock(userVals, element map[string]any, cache map[string]any) map[string]any {
	// A nil element would send a values file whose only mounter key is `cache`, and the chart would
	// render the rest empty - an absent logLevel becomes `--v=` and the mounter exits 2 on it.
	if len(element) == 0 {
		Fail("withCacheBlock was given an empty mounter element; mounterElement must run first")
	}
	out := deepCopyMap(userVals)
	mounter := deepCopyMap(element)
	if cache == nil {
		delete(mounter, "cache")
	} else {
		mounter["cache"] = cache
	}
	out["daemonsetMounters"] = []any{mounter}
	return out
}

func deepCopyMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = deepCopyValue(v)
	}
	return out
}

func deepCopyValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return deepCopyMap(t)
	case []any:
		s := make([]any, len(t))
		for i := range t {
			s[i] = deepCopyValue(t[i])
		}
		return s
	default:
		return v
	}
}

// upgradeCacheConfig upgrades the installed release in place. No PostRenderer: unlike the upgrade
// suite, this must not alter CSIDriver token expiry.
//
// Wait is deliberately off. Helm's readiness check reports the mounter DaemonSet as
// `InProgress, Updated: 0/2` and waits for every pod to be updated - which never happens, because
// the updateStrategy is OnDelete and nothing updates a pod until it is deleted. So Wait: true is
// guaranteed to time out on any change to the mounter. Readiness is waited for by the caller
// instead, after the pods have actually been replaced.
func upgradeCacheConfig(ctx context.Context, cfg *action.Configuration, release string, ch *chart.Chart, vals map[string]any) {
	up := action.NewUpgrade(cfg)
	up.Namespace = helmReleaseNamespace
	up.Wait = false
	up.Timeout = 2 * time.Minute // the API calls only, since nothing is waited for
	up.ReuseValues = false       // vals is already the complete user-supplied set

	_, err := up.RunWithContext(ctx, release, ch, vals)
	framework.ExpectNoError(err, "upgrading release %q to change the cache configuration", release)
}

// --- Mounter helpers --------------------------------------------------------------------------

// restartMounterPodsAndWait deletes every mounter pod and waits for their replacements. Required
// after any change to the mounter DaemonSet, whose updateStrategy is OnDelete. Cluster-wide rather
// than per-node: the upgrade changes the spec everywhere, and leaving other nodes on the old spec
// would make the restore incomplete.
func restartMounterPodsAndWait(ctx context.Context, f *framework.Framework) {
	nodes, err := f.ClientSet.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	framework.ExpectNoError(err)

	for i := range nodes.Items {
		node := nodes.Items[i].Name
		pods, err := f.ClientSet.CoreV1().Pods(csiDriverDaemonSetNamespace).List(ctx, metav1.ListOptions{
			LabelSelector: "app=s3-csi-daemonset-mounter",
			FieldSelector: "spec.nodeName=" + node,
		})
		framework.ExpectNoError(err)
		if len(pods.Items) == 0 {
			continue // no mounter scheduled here
		}
		killMounterPodOnNode(ctx, f, node)
	}

	// Generous: an ephemeral cache volume must be provisioned, attached and mounted before the pod
	// can start, and an unbound volume blocks it for 2m3s per attempt.
	waitForMounterDaemonSetReady(ctx, f, 6*time.Minute)
}

// waitForMounterDaemonSetReady waits until every mounter pod is Running and Ready, at the current
// generation. NumberReady alone is not enough: a stale-generation read can satisfy it before the
// deletions are observed.
func waitForMounterDaemonSetReady(ctx context.Context, f *framework.Framework, timeout time.Duration) {
	gomega.Eventually(ctx, func(ctx context.Context) (bool, error) {
		ds, err := f.ClientSet.AppsV1().DaemonSets(csiDriverDaemonSetNamespace).
			Get(ctx, "s3-csi-daemonset-mounter", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		if ds.Status.ObservedGeneration != ds.Generation || ds.Status.DesiredNumberScheduled == 0 ||
			ds.Status.NumberReady != ds.Status.DesiredNumberScheduled {
			return false, nil
		}

		pods, err := f.ClientSet.CoreV1().Pods(csiDriverDaemonSetNamespace).List(ctx, metav1.ListOptions{
			LabelSelector: "app=s3-csi-daemonset-mounter",
		})
		if err != nil || int32(len(pods.Items)) != ds.Status.DesiredNumberScheduled {
			return false, nil
		}
		for i := range pods.Items {
			if pods.Items[i].Status.Phase != v1.PodRunning || !isPodReady(&pods.Items[i]) {
				return false, nil
			}
		}
		return true, nil
	}).WithTimeout(timeout).WithPolling(5*time.Second).Should(gomega.BeTrue(),
		func() string { return dumpMounterPods(ctx, f) })
}

// dumpMounterPods reports why the mounter pods are not ready. Without this a bad pod spec - a
// rendered arg the binary rejects, say - looks identical to a slow ephemeral volume: both are just a
// timeout. The container's terminated state carries the exit code and the args show the rendered
// flags, so a rejected flag (the `--v=` failure mode this suite's values machinery guards against)
// is distinguishable from a claim that never bound.
func dumpMounterPods(ctx context.Context, f *framework.Framework) string {
	var b strings.Builder
	b.WriteString("the mounter DaemonSet did not become ready:\n")

	pods, err := f.ClientSet.CoreV1().Pods(csiDriverDaemonSetNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=s3-csi-daemonset-mounter",
	})
	if err != nil {
		fmt.Fprintf(&b, "  could not list mounter pods: %v\n", err)
		return b.String()
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		fmt.Fprintf(&b, "  pod %s on %s: phase=%s\n", pod.Name, pod.Spec.NodeName, pod.Status.Phase)
		for _, cs := range pod.Status.ContainerStatuses {
			fmt.Fprintf(&b, "    container %s: ready=%v restarts=%d state=%+v lastState=%+v\n",
				cs.Name, cs.Ready, cs.RestartCount, cs.State, cs.LastTerminationState)
		}
		if len(pod.Spec.Containers) > 0 {
			fmt.Fprintf(&b, "    args: %v\n", pod.Spec.Containers[0].Args)
		}
		// An ephemeral cache blocks the pod until its claim binds, so its phase names the cause.
		if pvc, err := f.ClientSet.CoreV1().PersistentVolumeClaims(pod.Namespace).
			Get(ctx, pod.Name+"-cache", metav1.GetOptions{}); err == nil {
			fmt.Fprintf(&b, "    cache PVC %s: phase=%s\n", pvc.Name, pvc.Status.Phase)
		}
	}
	return b.String()
}

// assertMounterCacheVolume checks the mounter DaemonSet renders the cache volume that was asked for,
// so a spec fails on the chart rather than later on a confusing mount error. Reads the DaemonSet's
// own pod template rather than a live pod: right after a restart the pod list can still contain a
// terminating pod carrying the previous spec.
func assertMounterCacheVolume(ctx context.Context, f *framework.Framework, cache map[string]any) {
	ds, err := f.ClientSet.AppsV1().DaemonSets(csiDriverDaemonSetNamespace).
		Get(ctx, "s3-csi-daemonset-mounter", metav1.GetOptions{})
	framework.ExpectNoError(err, "reading the mounter DaemonSet")

	var found *v1.Volume
	for i := range ds.Spec.Template.Spec.Volumes {
		if ds.Spec.Template.Spec.Volumes[i].Name == "cache" {
			found = &ds.Spec.Template.Spec.Volumes[i]
			break
		}
	}

	if cache == nil {
		gomega.Expect(found).To(gomega.BeNil(), "expected no cache volume on the mounter DaemonSet")
		return
	}
	gomega.Expect(found).NotTo(gomega.BeNil(), "expected a cache volume on the mounter DaemonSet")

	switch cache["type"] {
	case "ephemeral":
		gomega.Expect(found.Ephemeral).NotTo(gomega.BeNil(), "expected an ephemeral cache volume")
	case "emptyDir":
		gomega.Expect(found.EmptyDir).NotTo(gomega.BeNil(), "expected an emptyDir cache volume")
		if ed, ok := cache["emptyDir"].(map[string]any); ok {
			gomega.Expect(string(found.EmptyDir.Medium)).To(gomega.Equal(fmt.Sprint(ed["medium"])))
		}
	}
}

// mounterPodOnNode returns the mounter pod sharing a node with the workload. Never pods.Items[0]:
// on a multi-node cluster that is usually the wrong node.
func mounterPodOnNode(ctx context.Context, f *framework.Framework, nodeName string) *v1.Pod {
	gomega.Expect(nodeName).NotTo(gomega.BeEmpty(), "workload pod has no node assigned")
	pods, err := f.ClientSet.CoreV1().Pods(csiDriverDaemonSetNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=s3-csi-daemonset-mounter",
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	framework.ExpectNoError(err)
	gomega.Expect(pods.Items).To(gomega.HaveLen(1), "expected exactly one mounter pod on node %s", nodeName)
	return &pods.Items[0]
}

func csiDriverNodeImage(ctx context.Context, f *framework.Framework) string {
	ds, err := f.ClientSet.AppsV1().DaemonSets(csiDriverDaemonSetNamespace).
		Get(ctx, csiDriverDaemonSetName, metav1.GetOptions{})
	framework.ExpectNoError(err)
	return ds.Spec.Template.Spec.Containers[0].Image
}

// --- Filesystem assertions inside the mounter -------------------------------------------------

type mounterFilesystem struct {
	device  string
	fsType  string
	sizeKiB int
}

// statfsInMounter reports the filesystem backing a path inside the mounter container. Both specs
// read device, type and size from one df call, so it returns all three rather than asserting inline.
func statfsInMounter(ctx context.Context, f *framework.Framework, mounter *v1.Pod, path string) mounterFilesystem {
	// -P for POSIX single-line output, -T for the type, -k for KiB so the unit is not locale- or
	// size-dependent.
	stdout, stderr, err := execInPodWithNamespace(ctx, f, mounter.Namespace, mounter.Name, "mounter",
		[]string{"/bin/sh", "-c", "df -PTk " + path + " | tail -1"})
	framework.ExpectNoError(err, "df %s in the mounter: %s", path, stderr)

	fields := strings.Fields(strings.TrimSpace(stdout))
	gomega.Expect(len(fields)).To(gomega.BeNumerically(">=", 3), "unexpected df output for %s: %q", path, stdout)
	size, convErr := strconv.Atoi(fields[2])
	framework.ExpectNoError(convErr, "parsing the size from df output %q", stdout)

	framework.Logf("mounter %s: %s is %s (%s), %d KiB", mounter.Name, path, fields[0], fields[1], size)
	return mounterFilesystem{device: fields[0], fsType: fields[1], sizeKiB: size}
}

func statInMounter(ctx context.Context, f *framework.Framework, mounter *v1.Pod, format, path string) string {
	stdout, stderr, err := execInPodWithNamespace(ctx, f, mounter.Namespace, mounter.Name, "mounter",
		[]string{"stat", "-c", format, path})
	framework.ExpectNoError(err, "stat %s in the mounter: %s", path, stderr)
	return strings.TrimSpace(stdout)
}

// assertMountpointCacheDir checks the driver created this mount's directory and that Mountpoint
// could use it. `mountpoint-cache` existing inside is the end-to-end proof that the group bits are
// right, since Mountpoint creates it as the unprivileged user and does so non-recursively.
func assertMountpointCacheDir(ctx context.Context, f *framework.Framework, mounter *v1.Pod, pvName string) {
	dir := filepath.Join("/cache", pvName)
	gomega.Expect(statInMounter(ctx, f, mounter, "%A", dir)).To(gomega.Equal("drwxrwx---"),
		"the driver must create %s group-writable so Mountpoint can write inside it", dir)

	gomega.Eventually(ctx, func(ctx context.Context) (string, error) {
		out, _, err := execInPodWithNamespace(ctx, f, mounter.Namespace, mounter.Name, "mounter",
			[]string{"/bin/sh", "-c", "test -d " + filepath.Join(dir, "mountpoint-cache") + " && echo yes || echo no"})
		return strings.TrimSpace(out), err
	}).WithTimeout(time.Minute).WithPolling(5*time.Second).Should(gomega.Equal("yes"),
		"Mountpoint did not create its cache directory inside %s", dir)
}

// checkCachedRead reads a file twice; the second read is served from the cache. Only asserts the
// data is correct - cache hit rates are not observable from here.
func checkCachedRead(ctx context.Context, f *framework.Framework, pod *v1.Pod) {
	seed := time.Now().UTC().UnixNano()
	path := filepath.Join(e2epod.VolumeMountPath1, fmt.Sprintf("cached-%d.txt", seed))
	const size = 1024

	checkWriteToPathSucceedEventually(ctx, f, pod, path, size, seed)
	checkReadFromPathSucceedEventually(ctx, f, pod, path, size, seed)
	checkReadFromPathSucceedEventually(ctx, f, pod, path, size, seed)
}

// --- StorageClass -----------------------------------------------------------------------------

// createEBSCacheStorageClass creates a StorageClass with the EBS provisioner under the given name
// and returns a delete func, so the caller controls its lifetime. Tolerates an existing object so a
// leaked one from an interrupted run does not fail the suite.
func createEBSCacheStorageClass(ctx context.Context, f *framework.Framework, name string) func(context.Context) {
	sc := ebsCacheStorageClass(name)
	framework.Logf("Creating StorageClass %s with the EBS CSI Driver provisioner", name)
	if _, err := f.ClientSet.StorageV1().StorageClasses().Create(ctx, sc, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			framework.ExpectNoError(err, "creating the StorageClass for the cache")
		}
		framework.Logf("StorageClass %s already exists, reusing it", name)
	}

	return func(ctx context.Context) {
		framework.Logf("Deleting StorageClass %s", name)
		err := f.ClientSet.StorageV1().StorageClasses().Delete(ctx, name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			framework.ExpectNoError(err, "deleting the StorageClass for the cache")
		}
	}
}
