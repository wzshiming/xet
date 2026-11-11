package main

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

// validConfig judges group cpu and reports group io, with fewer than ten rounds as only a report-only group may.
const validConfig = `{
	"alpha": 0.05,
	"minSamples": 10,
	"groups": [
		{"name": "cpu", "packages": ["p"], "bench": ".", "rounds": 10, "thresholds": {"sec/op": 0.1}},
		{"name": "io", "packages": ["r"], "bench": ".", "rounds": 3, "reportOnly": true}
	]
}`

// edited writes validConfig as edit changes it and returns its path.
func edited(t *testing.T, edit func(c map[string]any, groups []map[string]any)) string {
	t.Helper()
	var c map[string]any
	if err := json.Unmarshal([]byte(validConfig), &c); err != nil {
		t.Fatal(err)
	}
	var groups []map[string]any
	for _, g := range c["groups"].([]any) {
		groups = append(groups, g.(map[string]any))
	}
	edit(c, groups)
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "gate.json")
	writeFile(t, path, string(data))
	return path
}

func TestConfigErrorsExitTwoNamingTheField(t *testing.T) {
	t.Parallel()
	f := &fake{passes: []map[string]canned{{}}}
	wantCode(t, command(t, f.run, t.TempDir(), t.TempDir(), edited(t, func(map[string]any, []map[string]any) {})), 0)

	for _, c := range []struct {
		name  string
		edit  func(c map[string]any, groups []map[string]any)
		names string // what stderr says
	}{
		{"unknown field", func(_ map[string]any, g []map[string]any) { g[0]["roundz"] = 10 }, `unknown field "roundz"`},
		{"invalid regular expression", func(_ map[string]any, g []map[string]any) { g[0]["bench"] = "(" }, "groups[0].bench:"},
		{"judged group with nine rounds", func(_ map[string]any, g []map[string]any) { g[0]["rounds"] = 9 }, "groups[0].rounds:"},
		{"negative threshold", func(_ map[string]any, g []map[string]any) { g[0]["thresholds"] = map[string]any{"sec/op": -0.1} }, `groups[0].thresholds["sec/op"]:`},
		{"metric the gate cannot judge", func(_ map[string]any, g []map[string]any) { g[0]["thresholds"] = map[string]any{"ns/op": 0.1} }, `groups[0].thresholds["ns/op"]:`},
		{"neither thresholds nor reportOnly", func(_ map[string]any, g []map[string]any) { delete(g[0], "thresholds") }, "groups[0].thresholds:"},
		{"thresholds in a report-only group", func(_ map[string]any, g []map[string]any) { g[1]["thresholds"] = map[string]any{"sec/op": 0.1} }, "groups[1].thresholds:"},
		{"alpha of 0", func(c map[string]any, _ []map[string]any) { c["alpha"] = 0 }, "alpha:"},
		{"alpha of 1", func(c map[string]any, _ []map[string]any) { c["alpha"] = 1 }, "alpha:"},
		{"no minSamples", func(c map[string]any, _ []map[string]any) { delete(c, "minSamples") }, "minSamples:"},
		{"judged group with fewer rounds than minSamples", func(c map[string]any, _ []map[string]any) { c["minSamples"] = 12 }, "groups[0].rounds:"},
		{"no groups", func(c map[string]any, _ []map[string]any) { c["groups"] = []any{} }, "groups:"},
		{"group without packages", func(_ map[string]any, g []map[string]any) { delete(g[0], "packages") }, "groups[0].packages:"},
		{"empty benchmark expression", func(_ map[string]any, g []map[string]any) { g[1]["bench"] = "" }, "groups[1].bench:"},
		{"benchtime neither a duration nor a count", func(_ map[string]any, g []map[string]any) { g[0]["benchtime"] = "soon" }, "groups[0].benchtime:"},
		{"benchtime of no iterations", func(_ map[string]any, g []map[string]any) { g[1]["benchtime"] = "0x" }, "groups[1].benchtime:"},
		{"negative benchtime", func(_ map[string]any, g []map[string]any) { g[1]["benchtime"] = "-1s" }, "groups[1].benchtime:"},
		{"report-only group without rounds", func(_ map[string]any, g []map[string]any) { delete(g[1], "rounds") }, "groups[1].rounds:"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			f := &fake{}
			o := command(t, f.run, t.TempDir(), t.TempDir(), edited(t, c.edit))
			wantCode(t, o, 2)
			if len(f.requests) != 0 {
				t.Errorf("sampled %d times, want no sampling", len(f.requests))
			}
			if !strings.Contains(o.stderr, c.names) {
				t.Errorf("stderr %q does not say %s", o.stderr, c.names)
			}
		})
	}
}

func TestDataAfterTheConfigurationExitsTwo(t *testing.T) {
	t.Parallel()
	for _, trailing := range []string{" this is not JSON", "\n{}", "}"} {
		path := filepath.Join(t.TempDir(), "gate.json")
		writeFile(t, path, validConfig+trailing)
		f := &fake{}
		o := command(t, f.run, t.TempDir(), t.TempDir(), path)
		if o.code != 2 || len(f.requests) != 0 || !strings.Contains(o.stderr, "after the configuration") {
			t.Errorf("with %q after the configuration: exit code %d, sampled %d times, stderr %q; want 2, no sampling and the data named",
				trailing, o.code, len(f.requests), o.stderr)
		}
	}
}
