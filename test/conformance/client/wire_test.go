package client_test

import (
	"bytes"
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"
)

const (
	testXorbHash  = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	testRustrefID = "01890a5d-ac96-774b-bcce-b302099a8057"
	testGoID      = "0123456789abcdef0123456789abcdef"
)

func TestNormalizePath(t *testing.T) {
	for _, tc := range []struct{ name, path, want string }{
		{"legacy xorb upload", "/v1/xorbs/default/" + testXorbHash, "/v1/xorbs/default/{hash}"},
		{"xorb grant", "/v1/xorb-grants/default/" + testXorbHash, "/v1/xorb-grants/default/{hash}"},
		{"xet-core grant upload with uuid", "/simulation/xorb-uploads/" + testRustrefID, "/simulation/xorb-uploads/{grant}"},
		{"Go grant upload with hex id", "/v1/xorb-uploads/" + testGoID, "/v1/xorb-uploads/{grant}"},
		{"commit with uuid", "/v1/xorb-commits/default/" + testXorbHash + "/grant/" + testRustrefID, "/v1/xorb-commits/default/{hash}/grant/{grant}"},
		{"dedup query unchanged", "/v1/chunks/default:query", "/v1/chunks/default:query"},
		{"shard upload unchanged", "/v2/shards", "/v2/shards"},
		{"telemetry unchanged", "/v1/telemetry", "/v1/telemetry"},
		{"32-hex segment outside a grant unchanged", "/v1/chunks/default/" + testGoID, "/v1/chunks/default/" + testGoID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizePath(tc.path); got != tc.want {
				t.Errorf("normalizePath(%q) = %q, want %q", tc.path, got, tc.want)
			}
		})
	}
}

func TestModelUploads(t *testing.T) {
	legacyBody, legacyXorb := encodeTestXorb(t, "legacy chunk one", "legacy chunk two")
	grantBody, grantXorb := encodeTestXorb(t, "granted chunk")

	legacyPost := func(hash string) RequestRecord {
		return RequestRecord{Method: http.MethodPost, Path: "/v1/xorbs/default/" + hash, Body: legacyBody}
	}
	grant := func(hash string) RequestRecord {
		return RequestRecord{Method: http.MethodPost, Path: "/v1/xorb-grants/default/" + hash, Body: []byte(`{"length":1,"checksum":"AAAAAAAAAAA="}`)}
	}
	commit := func(hash, id string) RequestRecord {
		return RequestRecord{Method: http.MethodPost, Path: "/v1/xorb-commits/default/" + hash + "/grant/" + id}
	}
	rustrefPut := RequestRecord{Method: http.MethodPut, Path: "/simulation/xorb-uploads/" + testRustrefID, Body: grantBody}
	goPut := RequestRecord{Method: http.MethodPut, Path: "/v1/xorb-uploads/" + testGoID, Body: grantBody}

	for _, tc := range []struct {
		name         string
		reqs         []RequestRecord
		wantBodies   []xorbInfo
		wantXorbs    []string
		wantBypassed bool
		wantProblems []string // one substring per problem
		wantNotes    []string // one substring per note
	}{
		{
			name:       "legacy POST and grant PUT bodies are both uploads",
			reqs:       []RequestRecord{legacyPost(legacyXorb.hash), grant(grantXorb.hash), rustrefPut, commit(grantXorb.hash, testRustrefID)},
			wantBodies: []xorbInfo{legacyXorb, grantXorb},
			wantXorbs:  []string{legacyXorb.hash, grantXorb.hash},
		},
		{
			name:       "grant PUT as the only body is an upload",
			reqs:       []RequestRecord{grant(grantXorb.hash), goPut, commit(grantXorb.hash, testGoID)},
			wantBodies: []xorbInfo{grantXorb},
			wantXorbs:  []string{grantXorb.hash},
		},
		{
			name:         "legacy path hash differing from the body hash is reported",
			reqs:         []RequestRecord{legacyPost(testXorbHash)},
			wantBodies:   []xorbInfo{legacyXorb},
			wantXorbs:    []string{testXorbHash},
			wantProblems: []string{legacyXorb.hash},
		},
		{
			name:         "grant and commit without a PUT bypass the proxy",
			reqs:         []RequestRecord{grant(grantXorb.hash), commit(grantXorb.hash, testRustrefID)},
			wantXorbs:    []string{grantXorb.hash},
			wantBypassed: true,
		},
		{
			name: "no requests yield no uploads",
		},
		{
			name:         "PUT body without a grant is reported",
			reqs:         []RequestRecord{rustrefPut},
			wantBodies:   []xorbInfo{grantXorb},
			wantProblems: []string{grantXorb.hash},
		},
		{
			name:         "commit without a grant is reported",
			reqs:         []RequestRecord{commit(grantXorb.hash, testRustrefID)},
			wantXorbs:    []string{grantXorb.hash},
			wantBypassed: true,
			wantProblems: []string{grantXorb.hash},
		},
		{
			name: "commit without a proxied body is reported",
			reqs: []RequestRecord{
				grant(grantXorb.hash), rustrefPut, commit(grantXorb.hash, testRustrefID),
				grant(legacyXorb.hash), commit(legacyXorb.hash, testGoID),
			},
			wantBodies:   []xorbInfo{grantXorb},
			wantXorbs:    []string{legacyXorb.hash, grantXorb.hash},
			wantProblems: []string{legacyXorb.hash},
		},
		{
			name:         "legacy bodies plus commits without a PUT are a bypass, not missing bodies",
			reqs:         []RequestRecord{legacyPost(legacyXorb.hash), grant(grantXorb.hash), commit(grantXorb.hash, testRustrefID)},
			wantBodies:   []xorbInfo{legacyXorb},
			wantXorbs:    []string{legacyXorb.hash, grantXorb.hash},
			wantBypassed: true,
		},
		{
			name:      "grant without a commit is only noted",
			reqs:      []RequestRecord{grant(grantXorb.hash)},
			wantNotes: []string{grantXorb.hash},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := modelUploads(tc.reqs)
			if !slices.EqualFunc(m.bodies, tc.wantBodies, sameXorbInfo) {
				t.Errorf("bodies = %+v, want %+v", m.bodies, tc.wantBodies)
			}
			if got, want := slices.Sorted(maps.Keys(m.xorbHashes)), slices.Sorted(slices.Values(tc.wantXorbs)); !slices.Equal(got, want) {
				t.Errorf("xorbHashes = %v, want %v", got, want)
			}
			if got := m.bypassed(); got != tc.wantBypassed {
				t.Errorf("bypassed() = %v, want %v", got, tc.wantBypassed)
			}
			checkMessages(t, "problems", m.problems, tc.wantProblems)
			checkMessages(t, "notes", m.notes, tc.wantNotes)
		})
	}
}

func TestEmptyFileWithoutReconstruction(t *testing.T) {
	telemetry := RequestRecord{Method: http.MethodPost, Path: "/v1/telemetry"}
	reconstruction := func(version string) RequestRecord {
		return RequestRecord{Method: http.MethodGet, Path: "/" + version + "/reconstructions/" + testXorbHash}
	}

	for _, tc := range []struct {
		name     string
		fileSize int64
		reqs     []RequestRecord
		want     bool
	}{
		{"empty file without a reconstruction query is skipped", 0, []RequestRecord{telemetry}, true},
		{"empty file with a v1 reconstruction query is compared", 0, []RequestRecord{reconstruction("v1")}, false},
		{"empty file with a v2 reconstruction query is compared", 0, []RequestRecord{reconstruction("v2")}, false},
		{"non-empty file without a reconstruction query is compared", 1, []RequestRecord{telemetry}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			byType := groupRequestsByType(tc.reqs)
			if got := emptyFileWithoutReconstruction(tc.fileSize, byType); got != tc.want {
				t.Errorf("emptyFileWithoutReconstruction(%d, %s) = %v, want %v", tc.fileSize, requestHistogram(byType), got, tc.want)
			}
		})
	}
}

// encodeTestXorb serializes chunks as an upload body and returns what decoding it must yield.
func encodeTestXorb(t *testing.T, chunks ...string) ([]byte, xorbInfo) {
	t.Helper()
	var body bytes.Buffer
	enc := xorb.NewEncoder(&body, true)
	var want xorbInfo
	for _, chunk := range chunks {
		if _, err := enc.Write([]byte(chunk)); err != nil {
			t.Fatalf("encode chunk: %v", err)
		}
		want.chunkHashes = append(want.chunkHashes, xet.ComputeChunkHash([]byte(chunk)))
		want.chunkSizes = append(want.chunkSizes, len(chunk))
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("close xorb encoder: %v", err)
	}
	want.hash = enc.SummoryHash().String()
	return body.Bytes(), want
}

func sameXorbInfo(a, b xorbInfo) bool {
	return a.hash == b.hash && slices.Equal(a.chunkHashes, b.chunkHashes) && slices.Equal(a.chunkSizes, b.chunkSizes)
}

func checkMessages(t *testing.T, kind string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s = %q, want %d mentioning %q", kind, got, len(want), want)
		return
	}
	for i := range want {
		if !strings.Contains(got[i], want[i]) {
			t.Errorf("%s[%d] = %q, want it to mention %s", kind, i, got[i], want[i])
		}
	}
}
