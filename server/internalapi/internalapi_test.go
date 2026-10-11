package internalapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/mirror/spool"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/xorb"
)

// putTestFile stores content as one single-chunk xorb plus its shard and
// returns the xet file hash.
func putTestFile(t *testing.T, ctx context.Context, stor storage.Storage, content []byte) xet.FileHash {
	t.Helper()
	var encoded bytes.Buffer
	encoder := xorb.NewEncoder(&encoded, true)
	if _, err := encoder.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	xorbHash := encoder.SummoryHash()
	if _, err := stor.PutXorb(ctx, xorbHash, bytes.NewReader(encoded.Bytes())); err != nil {
		t.Fatal(err)
	}
	chunkHash := xet.ComputeChunkHash(content)
	fileHash := xet.ComputeFileHash([]xet.ChunkHash{chunkHash}, []uint64{uint64(len(content))})
	shardObj := shard.NewShard()
	shardObj.AddCASBlock(shard.CASBlock{
		CASHash: xorbHash,
		Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: chunkHash, UnpackedSegBytes: uint32(len(content))}},
	})
	shardObj.AddFile(shard.FileBlock{
		FileHash: fileHash,
		Entries: []shard.FileDataSequenceEntry{
			{CASHash: xorbHash, UnpackedSegBytes: uint32(len(content)), ChunkIndexEnd: 1},
		},
	})
	if _, err := stor.PutShard(ctx, shardObj); err != nil {
		t.Fatal(err)
	}
	return fileHash
}

// decodeSweep returns the storage pass of a recorded GC sweep response.
func decodeSweep(t *testing.T, rec *httptest.ResponseRecorder) storage.SweepResult {
	t.Helper()
	var resp sweepResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode sweep: %v", err)
	}
	if resp.Storage == nil {
		t.Fatal("sweep response carries no storage pass")
	}
	return *resp.Storage
}

// responseKeys returns the sorted top-level keys of a recorded JSON response without consuming its body.
func responseKeys(t *testing.T, rec *httptest.ResponseRecorder) []string {
	t.Helper()
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode response keys: %v", err)
	}
	return slices.Sorted(maps.Keys(raw))
}

func TestInternalRoutesRequirePermission(t *testing.T) {
	fs, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	fileHash, err := xet.ParseFileHash(strings.Repeat("ab", 32))
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range []struct {
		method string
		url    string
		grants []auth.Grant
	}{
		{http.MethodGet, "/internal/files", []auth.Grant{{Permission: auth.Read}}},
		{http.MethodDelete, "/internal/files/xet/" + strings.Repeat("ab", 32), []auth.Grant{{Permission: auth.Write, File: &fileHash}}},
		{http.MethodDelete, "/internal/files/sha256/" + strings.Repeat("ab", 32), []auth.Grant{{Permission: auth.Write, SHA256: strings.Repeat("ab", 32)}}},
		{http.MethodPost, "/internal/gc?dry_run=true", []auth.Grant{{Permission: auth.Write}}},
		{http.MethodDelete, "/internal/files/xet/not-a-hash", nil},
		{http.MethodDelete, "/internal/files/sha256/not-a-hash", nil},
	} {
		t.Run(route.method+" "+route.url, func(t *testing.T) {
			var calls []auth.Grant
			handler := NewHandler(WithStorage(fs), WithAuthorizer(auth.AuthorizerFunc(func(r *http.Request, grant auth.Grant) error {
				calls = append(calls, grant)
				return nil
			})))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(route.method, route.url, nil))
			if !reflect.DeepEqual(calls, route.grants) {
				t.Fatalf("authorizations = %+v, want %+v", calls, route.grants)
			}
			if route.grants == nil && rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
		})
	}
}

func TestInternalRoutesDenyMapping(t *testing.T) {
	for _, test := range []struct {
		name      string
		err       error
		status    int
		challenge string
	}{
		{"unauthenticated", auth.ErrUnauthenticated, http.StatusUnauthorized, "Bearer"},
		{"forbidden", errors.New("nope"), http.StatusForbidden, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := NewHandler(WithAuthorizer(auth.AuthorizerFunc(func(r *http.Request, grant auth.Grant) error {
				return test.err
			})))
			for _, route := range []struct{ method, url string }{
				{http.MethodGet, "/internal/files"},
				{http.MethodDelete, "/internal/files/xet/" + strings.Repeat("ab", 32)},
				{http.MethodDelete, "/internal/files/sha256/" + strings.Repeat("ab", 32)},
				{http.MethodPost, "/internal/gc?grace=invalid"},
			} {
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, httptest.NewRequest(route.method, route.url, nil))
				if rec.Code != test.status {
					t.Fatalf("%s: status = %d, want %d", route.url, rec.Code, test.status)
				}
				if got := rec.Header().Get("WWW-Authenticate"); got != test.challenge {
					t.Fatalf("WWW-Authenticate = %q, want %q", got, test.challenge)
				}
				if !strings.Contains(rec.Body.String(), test.err.Error()) {
					t.Fatalf("body = %q, want it to contain %q", rec.Body.String(), test.err.Error())
				}
			}
		})
	}
}

func TestInternalDeniedMutations(t *testing.T) {
	ctx := context.Background()
	fs, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("denied mutation content")
	fileHash := putTestFile(t, ctx, fs, content)
	digest := sha256.Sum256(content)
	handler := NewHandler(WithStorage(fs), WithAuthorizer(auth.AuthorizerFunc(func(r *http.Request, grant auth.Grant) error {
		return auth.ErrForbidden
	})))
	for _, route := range []struct{ method, url string }{
		{http.MethodDelete, "/internal/files/xet/" + fileHash.String()},
		{http.MethodDelete, "/internal/files/sha256/" + hex.EncodeToString(digest[:])},
		{http.MethodPost, "/internal/gc?grace=0"},
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(route.method, route.url, nil))
		if rec.Code != http.StatusForbidden {
			t.Fatalf("%s: status = %d", route.url, rec.Code)
		}
		if _, err := fs.GetShard(ctx, fileHash); err != nil {
			t.Fatalf("file entry changed after denied mutation: %v", err)
		}
		if _, err := fs.GetFileHashBySHA256(ctx, digest); err != nil {
			t.Fatalf("SHA256 entry changed after denied mutation: %v", err)
		}
	}
}

func TestBoundTokenCannotUnlinkEmptyFile(t *testing.T) {
	ctx := context.Background()
	stor, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	empty := shard.NewShard()
	empty.AddFile(shard.FileBlock{})
	if _, err := stor.PutShard(ctx, empty); err != nil {
		t.Fatal(err)
	}
	issuer, err := auth.NewIssuer(nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	token, _, err := issuer.Sign(auth.Grant{Permission: auth.Write, File: &xet.FileHash{1}})
	if err != nil {
		t.Fatal(err)
	}
	unbound, _, err := issuer.Sign(auth.Grant{Permission: auth.Write})
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(stor), WithAuthorizer(issuer))
	for _, test := range []struct {
		token  string
		status int
	}{
		{token, http.StatusForbidden},
		{unbound, http.StatusOK},
	} {
		request := httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+(xet.FileHash{}).String(), nil)
		request.Header.Set("Authorization", "Bearer "+test.token)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != test.status {
			t.Fatalf("status = %d, want %d: %s", response.Code, test.status, response.Body)
		}
		_, err := stor.GetShard(ctx, xet.FileHash{})
		if (err == nil) != (test.status == http.StatusForbidden) {
			t.Fatalf("empty file after status %d: %v", test.status, err)
		}
	}
}

func TestListFilesEndpoint(t *testing.T) {
	ctx := context.Background()
	fs, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}

	content := []byte("internal file listing content")
	fileHash := putTestFile(t, ctx, fs, content)

	handler := NewHandler(WithStorage(fs))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/internal/files", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	var entries []storage.FileListEntry
	if err := json.NewDecoder(rec.Body).Decode(&entries); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	digest := sha256.Sum256(content)
	want := []storage.FileListEntry{{
		SHA256:       hex.EncodeToString(digest[:]),
		FileHashes:   []string{fileHash.String()},
		OriginalSize: uint64(len(content)),
	}}
	if len(entries) != 1 || entries[0].SHA256 != want[0].SHA256 ||
		len(entries[0].FileHashes) != 1 || entries[0].FileHashes[0] != want[0].FileHashes[0] ||
		entries[0].OriginalSize != want[0].OriginalSize || entries[0].Missing {
		t.Fatalf("entries = %+v, want %+v", entries, want)
	}
}

func TestHandlerFallsThroughToNext(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	handler := NewHandler(WithNext(next))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/shards", nil))
	if rec.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusTeapot)
	}
}

func TestUnlinkFileEndpoint(t *testing.T) {
	ctx := context.Background()
	fs, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	fileHash := putTestFile(t, ctx, fs, []byte("unlink endpoint content"))
	handler := NewHandler(WithStorage(fs))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["file_hash"] != fileHash.String() || resp["removed"] != true {
		t.Fatalf("response = %v", resp)
	}
	if _, err := fs.GetShard(ctx, fileHash); err == nil {
		t.Fatal("file still resolves after unlink")
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("second unlink status = %d, want %d", rec.Code, http.StatusNotFound)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/not-a-hash", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid hash status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestUnlinkSHA256Endpoint(t *testing.T) {
	ctx := context.Background()
	fs, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("sha256 unlink endpoint content")
	fileHash := putTestFile(t, ctx, fs, content)
	digest := sha256.Sum256(content)
	shaHex := hex.EncodeToString(digest[:])
	handler := NewHandler(WithStorage(fs))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+shaHex, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["sha256"] != shaHex || resp["removed"] != true {
		t.Fatalf("response = %v", resp)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+shaHex, nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("second unlink status = %d, want %d", rec.Code, http.StatusNotFound)
	}

	// The remaining file entry still anchors the shard: a graceless sweep
	// reclaims nothing and the file hash keeps resolving.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=0", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result := decodeSweep(t, rec)
	if len(result.SweptShards) != 0 || len(result.SweptXorbs) != 0 {
		t.Fatalf("sweep after sha256 unlink alone = %+v", result)
	}
	if _, err := fs.GetShard(ctx, fileHash); err != nil {
		t.Fatalf("file hash stopped resolving after sha256 unlink: %v", err)
	}

	// Unlinking the file hash too leaves nothing anchoring the shard.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("file unlink status = %d: %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=0", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("second sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result = decodeSweep(t, rec)
	if len(result.SweptShards) != 1 || len(result.SweptXorbs) != 1 {
		t.Fatalf("sweep after both unlinks = %+v", result)
	}
}

func TestUnlinkSHA256EndpointRejectsBadDigests(t *testing.T) {
	fs, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(fs))

	for _, hash := range []string{
		"abc",                    // too short
		strings.Repeat("ab", 33), // too long
		strings.Repeat("zz", 32), // not hex
		strings.Repeat("00", 32), // all-zero empty-file marker
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+hash, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%q status = %d, want %d", hash, rec.Code, http.StatusBadRequest)
		}
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+strings.Repeat("ab", 32), nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown digest status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestGCSweepEndpoint(t *testing.T) {
	ctx := context.Background()
	fs, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("sweep endpoint content")
	fileHash := putTestFile(t, ctx, fs, content)
	digest := sha256.Sum256(content)
	handler := NewHandler(WithStorage(fs))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("unlink status = %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+hex.EncodeToString(digest[:]), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("sha256 unlink status = %d: %s", rec.Code, rec.Body.String())
	}

	// Dry run reports the orphans but removes nothing.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?dry_run=true&grace=0", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("dry run status = %d: %s", rec.Code, rec.Body.String())
	}
	result := decodeSweep(t, rec)
	if !result.DryRun || len(result.SweptShards) != 1 || len(result.SweptXorbs) != 1 {
		t.Fatalf("dry run result = %+v", result)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=0", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result = decodeSweep(t, rec)
	if result.DryRun || len(result.SweptShards) != 1 || len(result.SweptXorbs) != 1 {
		t.Fatalf("sweep result = %+v", result)
	}

	// Everything is gone: a second sweep finds nothing.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=0", nil))
	result = decodeSweep(t, rec)
	if len(result.SweptShards) != 0 || len(result.SweptXorbs) != 0 {
		t.Fatalf("second sweep result = %+v", result)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=bogus", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bogus grace status = %d, want %d", rec.Code, http.StatusBadRequest)
	}

	// A negative grace is rejected rather than silently disabling the window.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=-5m", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("negative grace status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

// TestGCSweepEndpointStepped drains a two-file store one dead object per
// request. Every request runs an independent pass that re-marks from
// scratch and reports only its own work, so the union of the steps covers
// the whole store; the mirror passes run once, on the step that finishes it.
func TestGCSweepEndpointStepped(t *testing.T) {
	ctx := context.Background()
	fs, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	mir, err := mirror.NewMirror(mirror.WithStorage(fs), mirror.WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	queue, err := spool.NewSpool(t.TempDir(), fs)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(fs), WithMirror(mir), WithSpool(queue))
	for i, content := range [][]byte{[]byte("stepped sweep one"), []byte("stepped sweep two")} {
		fileHash := putTestFile(t, ctx, fs, content)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("unlink %d status = %d", i, rec.Code)
		}
		digest := sha256.Sum256(content)
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+hex.EncodeToString(digest[:]), nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("sha256 unlink %d status = %d", i, rec.Code)
		}
	}

	var result storage.SweepResult
	sweptShards, sweptXorbs := 0, 0
	steps := 0
	for {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=0&max=1", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("step %d status = %d: %s", steps, rec.Code, rec.Body.String())
		}
		keys := responseKeys(t, rec)
		var resp sweepResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		result = decodeSweep(t, rec)
		steps++
		if got := len(result.SweptShards) + len(result.SweptXorbs); got != 1 {
			t.Fatalf("step %d swept %d objects, want exactly 1", steps, got)
		}
		sweptShards += len(result.SweptShards)
		sweptXorbs += len(result.SweptXorbs)
		if result.Done {
			if !slices.Equal(keys, []string{"done", "mirror", "spools", "storage"}) || !resp.Done {
				t.Fatalf("final step keys = %v, done = %v, want done with the storage, spool and mirror passes", keys, resp.Done)
			}
			break
		}
		if !slices.Equal(keys, []string{"done", "storage"}) || resp.Done {
			t.Fatalf("step %d keys = %v, done = %v, want the storage pass alone and not done", steps, keys, resp.Done)
		}
		if steps > 10 {
			t.Fatalf("stepping not done after %d steps: %+v", steps, result)
		}
	}
	// Two shards plus two xorbs, one dead object per step.
	if steps != 4 {
		t.Fatalf("steps = %d, want 4", steps)
	}
	if sweptShards != 2 || sweptXorbs != 2 {
		t.Fatalf("union swept = %d shards, %d xorbs, want 2 and 2", sweptShards, sweptXorbs)
	}
	if result.RemainingShards != 0 || result.RemainingXorbs != 0 {
		t.Fatalf("final step remaining = %d/%d, want 0/0", result.RemainingShards, result.RemainingXorbs)
	}

	// Nothing is left for a full pass.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=0", nil))
	if keys := responseKeys(t, rec); !slices.Equal(keys, []string{"done", "mirror", "spools", "storage"}) {
		t.Fatalf("full pass keys = %v, want done with the storage, spool and mirror passes", keys)
	}
	var full sweepResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &full); err != nil || !full.Done {
		t.Fatalf("full pass done = %v (%v), want true", full.Done, err)
	}
	result = decodeSweep(t, rec)
	if len(result.SweptShards) != 0 || len(result.SweptXorbs) != 0 {
		t.Fatalf("full pass after drain = %+v", result)
	}
}

func TestGCSweepEndpointRejectsInvalidStepParams(t *testing.T) {
	fs, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(fs))
	for _, query := range []string{
		"max=bogus", "max=-1",
		"budget=bogus", "budget=-5s",
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?"+query, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want %d", query, rec.Code, http.StatusBadRequest)
		}
	}
}

// TestGCSweepEndpointRejectsInvalidAnchor: unknown or misspelled anchor
// values are rejected at the boundary before any store access.
func TestGCSweepEndpointRejectsInvalidAnchor(t *testing.T) {
	fs, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(fs))
	for _, v := range []string{"bogus", "Files", "SHA256", "BOTH", "file"} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?anchor="+v, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("anchor=%s status = %d, want %d", v, rec.Code, http.StatusBadRequest)
		}
	}
}

// TestGCSweepEndpointServerGrace: WithGCGrace supplies the window for
// requests omitting grace; an explicit parameter still overrides it.
func TestGCSweepEndpointServerGrace(t *testing.T) {
	ctx := context.Background()

	unlinkBoth := func(t *testing.T, handler *Handler, fileHash xet.FileHash, content []byte) {
		t.Helper()
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("unlink status = %d", rec.Code)
		}
		digest := sha256.Sum256(content)
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+hex.EncodeToString(digest[:]), nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("sha256 unlink status = %d: %s", rec.Code, rec.Body.String())
		}
	}

	// Disabled server default: a plain request reclaims fresh objects.
	fs, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(fs), WithGCGrace(-1))
	content := []byte("server grace content")
	unlinkBoth(t, handler, putTestFile(t, ctx, fs, content), content)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result := decodeSweep(t, rec)
	if len(result.SweptShards) != 1 || len(result.SweptXorbs) != 1 {
		t.Fatalf("sweep with disabled server grace = %+v", result)
	}

	// Unset option: a plain request keeps the default window and shields
	// the fresh objects.
	fs2, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler = NewHandler(WithStorage(fs2))
	content = []byte("server grace default")
	unlinkBoth(t, handler, putTestFile(t, ctx, fs2, content), content)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("default sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result = decodeSweep(t, rec)
	if len(result.SweptShards) != 0 || len(result.SweptXorbs) != 0 {
		t.Fatalf("default-window sweep = %+v", result)
	}

	// An explicit parameter overrides the disabled server default.
	handler = NewHandler(WithStorage(fs2), WithGCGrace(-1))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=1h", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("override sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result = decodeSweep(t, rec)
	if len(result.SweptShards) != 0 || len(result.SweptXorbs) != 0 {
		t.Fatalf("override sweep = %+v", result)
	}
}

// TestGCSweepEndpointNeedsBothUnlinks: full removal of a non-empty file
// takes both unlinks — after the file unlink alone the sha256 entry still
// anchors the shard through a graceless sweep.
func TestGCSweepEndpointNeedsBothUnlinks(t *testing.T) {
	ctx := context.Background()
	fs, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("needs both unlinks content")
	fileHash := putTestFile(t, ctx, fs, content)
	digest := sha256.Sum256(content)
	handler := NewHandler(WithStorage(fs))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("unlink status = %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=0", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result := decodeSweep(t, rec)
	if len(result.SweptShards) != 0 || len(result.SweptXorbs) != 0 {
		t.Fatalf("sweep after file unlink alone = %+v", result)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+hex.EncodeToString(digest[:]), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("sha256 unlink status = %d: %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=0", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("second sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result = decodeSweep(t, rec)
	if len(result.SweptShards) != 1 || len(result.SweptXorbs) != 1 {
		t.Fatalf("sweep after both unlinks = %+v", result)
	}
}

// TestGCSweepEndpointServerAnchor: WithGCAnchor supplies the anchor for
// requests omitting the parameter — under a sha256 server default the
// sha256 unlink alone reclaims — while an explicit anchor=both still
// overrides it back to needing both unlinks.
func TestGCSweepEndpointServerAnchor(t *testing.T) {
	ctx := context.Background()
	fs, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(fs), WithGCAnchor(storage.AnchorSHA256))

	// Server default sha256: the sha256 unlink alone frees the shard for a
	// plain sweep.
	content := []byte("server anchor content one")
	putTestFile(t, ctx, fs, content)
	digest := sha256.Sum256(content)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+hex.EncodeToString(digest[:]), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("sha256 unlink status = %d: %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=0", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result := decodeSweep(t, rec)
	if len(result.SweptShards) != 1 || len(result.SweptXorbs) != 1 {
		t.Fatalf("server-default sha256 sweep = %+v", result)
	}

	// An explicit anchor=both overrides the server default: after the
	// sha256 unlink alone the file entry still anchors the shard.
	content = []byte("server anchor content two")
	fileHash := putTestFile(t, ctx, fs, content)
	digest = sha256.Sum256(content)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+hex.EncodeToString(digest[:]), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("second sha256 unlink status = %d: %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=0&anchor=both", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("anchor=both sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result = decodeSweep(t, rec)
	if len(result.SweptShards) != 0 || len(result.SweptXorbs) != 0 {
		t.Fatalf("anchor=both sweep after sha256 unlink alone = %+v", result)
	}

	// Unlinking the file hash too completes removal under anchor=both,
	// proving the earlier non-reclaim was the anchor override at work.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("file unlink status = %d: %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=0&anchor=both", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("final sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result = decodeSweep(t, rec)
	if len(result.SweptShards) != 1 || len(result.SweptXorbs) != 1 {
		t.Fatalf("anchor=both sweep after both unlinks = %+v", result)
	}
}

// TestGCSweepEndpointSHA256AnchorLFS: the sha256 anchor serves stores
// managed exclusively by SHA-256, e.g. Git-LFS backends — DELETE
// /internal/files/sha256/{hash} alone lets an anchor=sha256 sweep reclaim
// the shard and xorbs, deleting the stale file entry with the shard.
func TestGCSweepEndpointSHA256AnchorLFS(t *testing.T) {
	ctx := context.Background()
	fs, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("sha256 anchor lfs content")
	fileHash := putTestFile(t, ctx, fs, content)
	digest := sha256.Sum256(content)
	handler := NewHandler(WithStorage(fs))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+hex.EncodeToString(digest[:]), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("sha256 unlink status = %d: %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=0&anchor=sha256", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result := decodeSweep(t, rec)
	if len(result.SweptShards) != 1 || len(result.SweptXorbs) != 1 {
		t.Fatalf("anchor=sha256 sweep = %+v", result)
	}
	if result.DeletedFileEntries < 1 {
		t.Fatalf("DeletedFileEntries = %d, want >= 1", result.DeletedFileEntries)
	}

	// The stale file entry went with the shard: nothing lists and the file
	// hash no longer resolves.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/internal/files", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d", rec.Code)
	}
	var entries []storage.FileListEntry
	if err := json.NewDecoder(rec.Body).Decode(&entries); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("listed %d files after sweep, want 0", len(entries))
	}
	if _, err := fs.GetShard(ctx, fileHash); err == nil {
		t.Fatal("file hash still resolves after anchor=sha256 sweep")
	}
}

// blockingGCStorage parks a sweep inside its first mark walk until released
// so a concurrent request can observe the busy GC.
type blockingGCStorage struct {
	*local.Storage
	enter   chan struct{}
	release chan struct{}
}

func (b *blockingGCStorage) WalkFileIndex(ctx context.Context, fn func(fileHash, shardHash string) error) error {
	b.enter <- struct{}{}
	<-b.release
	return b.Storage.WalkFileIndex(ctx, fn)
}

// TestGCSweepEndpointBusy: while one sweep runs, a concurrent request fails
// fast with 409 instead of queueing.
func TestGCSweepEndpointBusy(t *testing.T) {
	fs, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	blocking := &blockingGCStorage{
		Storage: fs,
		enter:   make(chan struct{}),
		release: make(chan struct{}),
	}
	handler := NewHandler(WithStorage(blocking))

	first := make(chan int, 1)
	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=0", nil))
		first <- rec.Code
	}()
	<-blocking.enter // the sweep is parked mid-pass, holding the GC

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=0", nil))
	if rec.Code != http.StatusConflict {
		t.Fatalf("busy sweep status = %d, want %d", rec.Code, http.StatusConflict)
	}

	close(blocking.release)
	if code := <-first; code != http.StatusOK {
		t.Fatalf("first sweep status = %d, want %d", code, http.StatusOK)
	}
}

// sweepRequest posts a GC sweep with query and returns the decoded report.
func sweepRequest(t *testing.T, handler *Handler, query string) sweepResponse {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc"+query, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("sweep %q status = %d: %s", query, rec.Code, rec.Body.String())
	}
	var result sweepResponse
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode sweep %q: %v", query, err)
	}
	return result
}

// TestGCSweepEndpointSpools: without a spool or mirror the sweep report
// carries the storage pass alone; with them, dry_run only reports (and says
// so), the default grace removes only spools idle past it, and grace=0
// removes every idle spool.
func TestGCSweepEndpointSpools(t *testing.T) {
	fs, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	if got := sweepRequest(t, NewHandler(WithStorage(fs)), "?grace=0"); got.Storage == nil || got.Spools != nil || got.Mirror != nil || !got.Done {
		t.Fatalf("sweep without a mirror = %+v, want a done storage pass alone", got)
	}
	rec := httptest.NewRecorder()
	NewHandler(WithStorage(fs)).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=0", nil))
	if keys := responseKeys(t, rec); !slices.Equal(keys, []string{"done", "storage"}) {
		t.Fatalf("sweep keys without a mirror = %v, want storage alone", keys)
	}

	cacheDir := t.TempDir()
	queue, err := spool.NewSpool(filepath.Join(cacheDir, "spool"), fs)
	if err != nil {
		t.Fatal(err)
	}
	mir, err := mirror.NewMirror(mirror.WithStorage(fs), mirror.WithCacheDir(cacheDir), mirror.WithSpool(queue))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(fs), WithMirror(mir), WithSpool(queue))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=0", nil))
	if keys := responseKeys(t, rec); !slices.Equal(keys, []string{"done", "mirror", "spools", "storage"}) {
		t.Fatalf("sweep keys with a mirror = %v, want done, storage, spools and mirror", keys)
	}
	var raw struct {
		Spools map[string]json.RawMessage `json:"spools"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	if keys := slices.Sorted(maps.Keys(raw.Spools)); !slices.Equal(keys, []string{"done", "dry_run", "reclaimed_bytes", "remaining_spools", "swept_spools"}) {
		t.Fatalf("spools report keys = %v, want done, dry_run, reclaimed_bytes, remaining_spools and swept_spools", keys)
	}
	stale := filepath.Join(cacheDir, "spool", "x.spool")
	if err := os.WriteFile(stale, []byte("stale spool content"), 0o644); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(stale, past, past); err != nil {
		t.Fatal(err)
	}
	fresh := filepath.Join(cacheDir, "spool", "y.spool")
	if err := os.WriteFile(fresh, []byte("fresh"), 0o644); err != nil {
		t.Fatal(err)
	}

	want := &spool.SweepResult{DryRun: true, SweptSpools: 2, ReclaimedBytes: int64(len("stale spool content") + len("fresh")), Done: true}
	if got := sweepRequest(t, handler, "?dry_run=true&grace=0"); got.Storage == nil || !got.Storage.DryRun || !reflect.DeepEqual(got.Spools, want) {
		t.Fatalf("dry-run sweep storage = %+v, spools = %+v; want a dry-run storage pass and %+v", got.Storage, got.Spools, want)
	}
	// Dry runs ignore the budget in every pass: one request reports them all.
	if got := sweepRequest(t, handler, "?dry_run=true&grace=0&budget=1ns"); !got.Done || got.Storage == nil || !got.Storage.Done || got.Mirror == nil || !got.Mirror.Done || !reflect.DeepEqual(got.Spools, want) {
		t.Fatalf("budgeted dry run = %+v (spools %+v), want done with every pass and spools %+v", got, got.Spools, want)
	}
	for _, p := range []string{stale, fresh} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("dry run removed %s: %v", filepath.Base(p), err)
		}
	}

	want = &spool.SweepResult{SweptSpools: 1, ReclaimedBytes: int64(len("stale spool content")), Done: true}
	if got := sweepRequest(t, handler, ""); !reflect.DeepEqual(got.Spools, want) {
		t.Fatalf("default-grace spools = %+v, want %+v", got.Spools, want)
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Fatalf("default grace removed the fresh spool: %v", err)
	}

	want = &spool.SweepResult{SweptSpools: 1, ReclaimedBytes: int64(len("fresh")), Done: true}
	if got := sweepRequest(t, handler, "?grace=0"); !reflect.DeepEqual(got.Spools, want) {
		t.Fatalf("grace=0 spools = %+v, want %+v", got.Spools, want)
	}
	if _, err := os.Stat(fresh); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fresh spool after grace=0 sweep: %v, want removed", err)
	}
}

// failingWalkStorage fails the storage pass at its first index walk.
type failingWalkStorage struct {
	storage.Storage
}

func (failingWalkStorage) WalkFileIndex(context.Context, func(fileHash, shardHash string) error) error {
	return errors.New("walk file index: boom")
}

// TestGCSweepEndpointReportsPassErrors: a failed pass keeps the status 200
// and the other passes' reports, appends "<name>: <message>" to errors in
// pass order, does not stop the passes after it and leaves done false.
func TestGCSweepEndpointReportsPassErrors(t *testing.T) {
	// newHandler wires local storage (failing its first index walk when asked), a spool and a mirror under one cache directory.
	newHandler := func(t *testing.T, failStorage bool) (*Handler, string) {
		fs, err := local.NewStorage(local.WithBasePath(t.TempDir()))
		if err != nil {
			t.Fatal(err)
		}
		cacheDir := t.TempDir()
		queue, err := spool.NewSpool(filepath.Join(cacheDir, "spool"), fs)
		if err != nil {
			t.Fatal(err)
		}
		mir, err := mirror.NewMirror(mirror.WithStorage(fs), mirror.WithCacheDir(cacheDir), mirror.WithSpool(queue))
		if err != nil {
			t.Fatal(err)
		}
		st := storage.Storage(fs)
		if failStorage {
			st = failingWalkStorage{Storage: fs}
		}
		return NewHandler(WithStorage(st), WithMirror(mir), WithSpool(queue)), cacheDir
	}
	// replaceWithFile turns dir into a regular file so listing it fails.
	replaceWithFile := func(t *testing.T, dir string) {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dir, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// gc posts grace=0, checks status and top-level keys, and decodes the report.
	gc := func(t *testing.T, handler *Handler, wantKeys []string) sweepResponse {
		t.Helper()
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=0", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("sweep status = %d: %s, want %d", rec.Code, rec.Body.String(), http.StatusOK)
		}
		if keys := responseKeys(t, rec); !slices.Equal(keys, wantKeys) {
			t.Fatalf("sweep keys = %v, want %v", keys, wantKeys)
		}
		var got sweepResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if got.Done {
			t.Fatalf("sweep done = true after a failed pass, want false")
		}
		return got
	}
	// prefixed checks that errors are exactly the given pass names, in order, each with a message.
	prefixed := func(t *testing.T, got []string, passes ...string) {
		t.Helper()
		if len(got) != len(passes) {
			t.Fatalf("sweep errors = %q, want one per %v", got, passes)
		}
		for i, pass := range passes {
			if !strings.HasPrefix(got[i], pass+": ") || len(got[i]) == len(pass)+2 {
				t.Fatalf("sweep errors[%d] = %q, want %q followed by a message", i, got[i], pass+": ")
			}
		}
	}

	t.Run("spools", func(t *testing.T) {
		handler, cacheDir := newHandler(t, false)
		replaceWithFile(t, filepath.Join(cacheDir, "spool"))
		got := gc(t, handler, []string{"done", "errors", "mirror", "storage"})
		prefixed(t, got.Errors, "spools")
		if got.Storage == nil || !got.Storage.Done || got.Mirror == nil || !got.Mirror.Done {
			t.Fatalf("storage = %+v, mirror = %+v; want both passes done around the failed spool pass", got.Storage, got.Mirror)
		}
	})

	t.Run("mirror", func(t *testing.T) {
		handler, cacheDir := newHandler(t, false)
		replaceWithFile(t, filepath.Join(cacheDir, "index"))
		got := gc(t, handler, []string{"done", "errors", "spools", "storage"})
		prefixed(t, got.Errors, "mirror")
		if got.Spools == nil || !got.Spools.Done {
			t.Fatalf("spools report = %+v, want a done spool pass", got.Spools)
		}
	})

	t.Run("spools and mirror", func(t *testing.T) {
		handler, cacheDir := newHandler(t, false)
		replaceWithFile(t, filepath.Join(cacheDir, "spool"))
		replaceWithFile(t, filepath.Join(cacheDir, "index"))
		got := gc(t, handler, []string{"done", "errors", "storage"})
		prefixed(t, got.Errors, "spools", "mirror")
		if got.Storage == nil || !got.Storage.Done {
			t.Fatalf("storage report = %+v, want a done storage pass", got.Storage)
		}
	})

	t.Run("storage", func(t *testing.T) {
		fs, err := local.NewStorage(local.WithBasePath(t.TempDir()))
		if err != nil {
			t.Fatal(err)
		}
		got := gc(t, NewHandler(WithStorage(failingWalkStorage{Storage: fs})), []string{"done", "errors"})
		if want := []string{"storage: walk file index: boom"}; !slices.Equal(got.Errors, want) {
			t.Fatalf("sweep errors = %q, want %q", got.Errors, want)
		}
	})

	t.Run("storage with later passes", func(t *testing.T) {
		handler, _ := newHandler(t, true)
		got := gc(t, handler, []string{"done", "errors", "mirror", "spools"})
		if want := []string{"storage: walk file index: boom"}; !slices.Equal(got.Errors, want) {
			t.Fatalf("sweep errors = %q, want %q", got.Errors, want)
		}
		if got.Spools == nil || !got.Spools.Done || got.Mirror == nil || !got.Mirror.Done {
			t.Fatalf("spools = %+v, mirror = %+v; want both passes done after the failed storage pass", got.Spools, got.Mirror)
		}
	})
}

// TestGCSweepEndpointIndex: with a mirror, the same dry_run drives the index
// pass, which drops the entries of files storage no longer holds and the
// manifests that leaves empty.
func TestGCSweepEndpointIndex(t *testing.T) {
	fs, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	cacheDir := t.TempDir()
	mir, err := mirror.NewMirror(mirror.WithStorage(fs), mirror.WithCacheDir(cacheDir))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(fs), WithMirror(mir))

	// Laid out as mirror/index.go does: index/<sha256(repo)>/commits/<commit>.json, naming a file hash storage never saw.
	repoSum := sha256.Sum256([]byte("org/repo"))
	commit := strings.Repeat("ab", 20)
	manifest := filepath.Join(cacheDir, "index", hex.EncodeToString(repoSum[:]), "commits", commit+".json")
	if err := os.MkdirAll(filepath.Dir(manifest), 0o755); err != nil {
		t.Fatal(err)
	}
	body := `{"repo":"org/repo","commit":"` + commit + `","files":{"f.bin":{"file_hash":"` + strings.Repeat("ef", 32) + `","sha256":"` + strings.Repeat("cd", 32) + `","size":5,"etag":"e","checked_at":"2026-01-01T00:00:00Z"}}}`
	if err := os.WriteFile(manifest, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	want := &mirror.SweepResult{DryRun: true, DroppedEntries: 1, RemovedManifests: 1, Done: true}
	if got := sweepRequest(t, handler, "?dry_run=true&grace=0"); !reflect.DeepEqual(got.Mirror, want) {
		t.Fatalf("dry-run index = %+v, want %+v", got.Mirror, want)
	}
	if _, err := os.Stat(manifest); err != nil {
		t.Fatalf("dry run removed the manifest: %v", err)
	}

	want = &mirror.SweepResult{DroppedEntries: 1, RemovedManifests: 1, Done: true}
	if got := sweepRequest(t, handler, "?grace=0"); !reflect.DeepEqual(got.Mirror, want) {
		t.Fatalf("grace=0 index = %+v, want %+v", got.Mirror, want)
	}
	if _, err := os.Stat(manifest); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("manifest after the sweep: %v, want removed", err)
	}
}

// TestGCSweepEndpointBudgetSpansPasses: one request budget is shared
// storage → spool → mirror. With 1ns every request that swept something
// defers the later passes, while a request that swept nothing still hands
// the next pass the 1ns floor, so each request makes exactly one unit of
// progress until the top-level done.
func TestGCSweepEndpointBudgetSpansPasses(t *testing.T) {
	ctx := context.Background()
	fs, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	cacheDir := t.TempDir()
	queue, err := spool.NewSpool(filepath.Join(cacheDir, "spool"), fs)
	if err != nil {
		t.Fatal(err)
	}
	mir, err := mirror.NewMirror(mirror.WithStorage(fs), mirror.WithCacheDir(cacheDir), mirror.WithSpool(queue))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(fs), WithMirror(mir), WithSpool(queue))

	content := []byte("budget spans passes")
	fileHash := putTestFile(t, ctx, fs, content)
	digest := sha256.Sum256(content)
	for _, url := range []string{"/internal/files/xet/" + fileHash.String(), "/internal/files/sha256/" + hex.EncodeToString(digest[:])} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, url, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d", url, rec.Code)
		}
	}
	past := time.Now().Add(-48 * time.Hour)
	for name, size := range map[string]int{"aa.spool": 10, "bb.spool": 20} {
		p := filepath.Join(cacheDir, "spool", name)
		if err := os.WriteFile(p, make([]byte, size), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, past, past); err != nil {
			t.Fatal(err)
		}
	}

	type step struct {
		keys   []string
		stored int
		spools *spool.SweepResult
		mirror *mirror.SweepResult
		done   bool
	}
	var steps []step
	for len(steps) < 10 {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc?grace=0&budget=1ns", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d status = %d: %s", len(steps)+1, rec.Code, rec.Body.String())
		}
		var resp sweepResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		steps = append(steps, step{
			keys:   responseKeys(t, rec),
			stored: len(resp.Storage.SweptShards) + len(resp.Storage.SweptXorbs),
			spools: resp.Spools,
			mirror: resp.Mirror,
			done:   resp.Done,
		})
		if resp.Done {
			break
		}
	}

	want := []struct {
		keys   []string
		stored int
		spools *spool.SweepResult
		done   bool
	}{
		{[]string{"done", "storage"}, 1, nil, false},
		{[]string{"done", "storage"}, 1, nil, false},
		{[]string{"done", "spools", "storage"}, 0, &spool.SweepResult{SweptSpools: 1, RemainingSpools: 1}, false},
		{[]string{"done", "spools", "storage"}, 0, &spool.SweepResult{SweptSpools: 1, Done: true}, false},
		{[]string{"done", "mirror", "spools", "storage"}, 0, &spool.SweepResult{Done: true}, true},
	}
	if len(steps) != len(want) {
		t.Fatalf("requests = %d, want %d: %+v", len(steps), len(want), steps)
	}
	var reclaimed int64
	for i, w := range want {
		got := steps[i]
		if !slices.Equal(got.keys, w.keys) || got.stored != w.stored || got.done != w.done {
			t.Fatalf("request %d keys = %v, storage swept %d, done %v; want %v, %d, %v", i+1, got.keys, got.stored, got.done, w.keys, w.stored, w.done)
		}
		if got.spools != nil {
			reclaimed += got.spools.ReclaimedBytes
			spools := *got.spools
			spools.ReclaimedBytes = 0
			got.spools = &spools
		}
		if !reflect.DeepEqual(got.spools, w.spools) {
			t.Fatalf("request %d spools = %+v, want %+v (bytes aside)", i+1, got.spools, w.spools)
		}
		if (got.mirror != nil) != (i == len(want)-1) {
			t.Fatalf("request %d mirror = %+v, want it on the last request only", i+1, got.mirror)
		}
	}
	if m := steps[len(steps)-1].mirror; !m.Done {
		t.Fatalf("final mirror pass = %+v, want done", m)
	}
	if reclaimed != 30 {
		t.Fatalf("reclaimed spool bytes = %d, want 30", reclaimed)
	}
	if left, err := filepath.Glob(filepath.Join(cacheDir, "spool", "*.spool")); err != nil || len(left) != 0 {
		t.Fatalf("spools left = %v (%v), want none", left, err)
	}
}
