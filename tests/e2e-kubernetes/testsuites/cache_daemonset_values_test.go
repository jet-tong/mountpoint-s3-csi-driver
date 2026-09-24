package custom_testsuites

import (
	"path/filepath"
	"testing"

	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
)

// chartForTest loads the working-tree chart. helmChartSource is relative to the suite root, which
// is where ginkgo runs; `go test` runs in this package's directory, one level deeper.
func chartForTest(t *testing.T) *chart.Chart {
	t.Helper()
	path := filepath.Join("..", helmChartSource)
	ch, err := loader.Load(path)
	if err != nil {
		t.Fatalf("loading the chart at %s: %v", path, err)
	}
	return ch
}

// The values a cache reconfiguration sends must always carry a *complete* daemonsetMounters element.
// Helm replaces a whole list element rather than merging it, so a partial one renders the mounter with
// empty fields - an absent logLevel becomes `--v=` and the container exits 2 on the unparseable flag,
// with nothing in the failure naming the cause. This was shipped once.
func TestMounterElementIsAlwaysComplete(t *testing.T) {
	ch := chartForTest(t)

	// Keys the chart's own values.yaml defines, and therefore the ones the rendered mounter needs.
	chartElement := ch.Values["daemonsetMounters"].([]any)[0].(map[string]any)
	if _, ok := chartElement["logLevel"]; !ok {
		t.Fatal("the chart's daemonsetMounters[0] has no logLevel; this test's premise is wrong")
	}

	tmpfs := map[string]any{"type": "emptyDir", "emptyDir": map[string]any{"medium": "Memory"}}

	for _, tc := range []struct {
		name     string
		userVals map[string]any
	}{
		{
			// A dev-cluster install: only --set overrides, no daemonsetMounters at all.
			name:     "no user-supplied element",
			userVals: map[string]any{"image": map[string]any{"tag": "latest"}},
		},
		{
			// A CI install: the whole element supplied via --values.
			name:     "complete user-supplied element",
			userVals: map[string]any{"daemonsetMounters": []any{deepCopyMap(chartElement)}},
		},
		{
			// What a previous run of this suite used to leave behind. The overlay must repair it
			// rather than propagate it, or each run strips the element further.
			name: "partial user-supplied element from an earlier run",
			userVals: map[string]any{
				"daemonsetMounters": []any{map[string]any{"cache": tmpfs}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			element := mounterElement(ch, tc.userVals)
			out := withCacheBlock(tc.userVals, element, tmpfs)

			sent := out["daemonsetMounters"].([]any)[0].(map[string]any)
			for k := range chartElement {
				if _, ok := sent[k]; !ok {
					t.Errorf("sent element is missing %q, which the chart's values.yaml defines", k)
				}
			}
			if got := sent["logLevel"]; got == nil || got == "" {
				t.Errorf("logLevel is %v; the mounter renders --v= and exits 2 on it", got)
			}
			if sent["cache"] == nil {
				t.Error("sent element has no cache block")
			}

			// The caller's map must not be mutated - it is also the restore payload.
			if tc.userVals["daemonsetMounters"] != nil {
				orig := tc.userVals["daemonsetMounters"].([]any)[0].(map[string]any)
				if _, leaked := orig["logLevel"]; leaked && len(orig) == 1 {
					t.Error("withCacheBlock mutated the caller's element")
				}
			}
		})
	}
}

// Removing the cache must still send everything else, which is what the no-cache spec relies on.
func TestMounterElementWithoutCacheKeepsTheRest(t *testing.T) {
	ch := chartForTest(t)
	userVals := map[string]any{
		"daemonsetMounters": []any{map[string]any{
			"cache": map[string]any{"type": "ephemeral"},
		}},
	}

	out := withCacheBlock(userVals, mounterElement(ch, userVals), nil)
	sent := out["daemonsetMounters"].([]any)[0].(map[string]any)

	if _, ok := sent["cache"]; ok {
		t.Error("cache should be absent when nil is passed")
	}
	if got := sent["logLevel"]; got == nil || got == "" {
		t.Errorf("logLevel is %v; removing the cache must not strip the rest of the element", got)
	}
}
