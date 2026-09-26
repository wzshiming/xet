package hftest

import (
	"embed"
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"testing"
)

//go:embed testdata/*.json
var fixtures embed.FS

// Fixture loads the recorded trace testdata/<name>.json, failing t when it is missing or malformed.
func Fixture(t testing.TB, name string) *Trace {
	t.Helper()
	data, err := fixtures.ReadFile("testdata/" + name + ".json")
	if err != nil {
		t.Fatalf("fixture %s: %v", name, err)
	}
	var tr Trace
	if err := json.Unmarshal(data, &tr); err != nil {
		t.Fatalf("decode fixture %s: %v", name, err)
	}
	return &tr
}

// Hub returns the exchanges with the hub in arrival order.
func (t *Trace) Hub() []Exchange {
	return t.origin("hub")
}

// CAS returns the exchanges with the CAS in arrival order.
func (t *Trace) CAS() []Exchange {
	return t.origin("cas")
}

func (t *Trace) origin(origin string) []Exchange {
	var out []Exchange
	for _, ex := range t.Exchanges {
		if ex.Origin == origin {
			out = append(out, ex)
		}
	}
	return out
}

// Route reduces ex to its method and endpoint kind, such as "POST preupload", "GET xet-read-token" or "HEAD resolve", with repository, revision, path and query stripped.
func Route(ex Exchange) string {
	return route(ex.Method, ex.Path)
}

// Route reduces r the way Route reduces a recorded exchange.
func (r Request) Route() string {
	return route(r.Method, r.Path)
}

func route(method, p string) string {
	p, _, _ = strings.Cut(p, "?")
	seg := strings.Split(strings.Trim(p, "/"), "/")
	kind := p
	switch {
	case seg[0] == "api" && len(seg) >= 5:
		kind = seg[4]
	case seg[0] == "api" && len(seg) == 3 && seg[1] == "repos":
		kind = "repos/" + seg[2]
	case seg[0] == "api" && len(seg) == 2:
		kind = seg[1]
	case (seg[0] == "v1" || seg[0] == "v2") && len(seg) >= 2:
		kind = seg[1]
	case seg[0] == "cdn":
		kind = "cdn"
	case len(seg) >= 3 && seg[2] == "resolve":
		kind = "resolve"
	}
	return method + " " + kind
}

// JSONKeys returns the sorted top-level keys of the JSON object in text (the first line of an NDJSON document), nil when it is not an object.
func JSONKeys(text string) []string {
	line, _, _ := strings.Cut(strings.TrimLeft(text, "\r\n"), "\n")
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &obj); err != nil {
		return nil
	}
	return slices.Sorted(maps.Keys(obj))
}
