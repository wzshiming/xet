package server_test

import (
	"bytes"
	"fmt"
	"io"
	"maps"
	"mime"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/storage/storagetest"
)

// bridgeChunks are the chunk sizes of the bridged file's one xorb: file bytes
// [0, 1024), [1024, 103424), [103424, 108544) and [108544, 174081).
var bridgeChunks = []int{1 << 10, 100 << 10, 5 << 10, 64<<10 + 1}

// storeBridgedFile stores a one-xorb file of LZ4 chunks sized by bridgeChunks
// on a fresh local store.
func storeBridgedFile(t *testing.T) (storage.Storage, storagetest.File) {
	t.Helper()
	st, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	class := storagetest.ChunkClasses[1]
	chunks := make([][]byte, len(bridgeChunks))
	for i, n := range bridgeChunks {
		chunks[i] = make([]byte, n)
		class.Chunk(0, i, chunks[i])
	}
	return st, storagetest.PutXorbFile(t, t.Context(), st, [][][]byte{chunks})
}

// writeSizes is an http.ResponseWriter without ReadFrom that records the
// length of each body Write.
type writeSizes struct {
	*httptest.ResponseRecorder
	sizes []int
}

func (w *writeSizes) Write(p []byte) (int, error) {
	w.sizes = append(w.sizes, len(p))
	return w.ResponseRecorder.Write(p)
}

// The bridge writes a reconstructed file to its response one decoded chunk
// per Write, a range from inside its first chunk on and cut at its end.
func TestBridgeWritesDecodedChunks(t *testing.T) {
	st, f := storeBridgedFile(t)
	h := server.NewHandler(server.WithStorage(st))
	for _, c := range []struct {
		name, rng string
		status    int
		body      []byte
		sizes     []int
	}{
		{"whole file", "", http.StatusOK, f.Content, bridgeChunks},
		{"range", "bytes=2000-105000", http.StatusPartialContent, f.Content[2000:105001], []int{103424 - 2000, 105001 - 103424}},
	} {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/xet-bridge/"+f.SHA256Hex, nil)
			if c.rng != "" {
				req.Header.Set("Range", c.rng)
			}
			w := &writeSizes{ResponseRecorder: httptest.NewRecorder()}
			h.ServeHTTP(w, req)
			if w.Code != c.status || !bytes.Equal(w.Body.Bytes(), c.body) {
				t.Fatalf("GET with Range %q = %d with %d bytes; want %d with the file's %d", c.rng, w.Code, w.Body.Len(), c.status, len(c.body))
			}
			if !slices.Equal(w.sizes, c.sizes) {
				t.Errorf("the bridge made Writes of %v bytes; want one per decoded chunk, %v", w.sizes, c.sizes)
			}
		})
	}
}

// exchanged is what TestBridgeResponsesMatchServeContent compares of a response.
type exchanged struct {
	status int
	header http.Header
	body   []byte
}

// exchange sends srv a request and reads its response, dropping Date and
// renaming the multipart boundary, which ServeContent draws at random, BOUNDARY.
func exchange(t *testing.T, srv *httptest.Server, method, path string, header http.Header) exchanged {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	maps.Copy(req.Header, header)
	res, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	h := res.Header.Clone()
	h.Del("Date")
	ctype := h.Get("Content-Type")
	if mt, params, err := mime.ParseMediaType(ctype); err == nil && mt == "multipart/byteranges" && params["boundary"] != "" {
		boundary := params["boundary"]
		h.Set("Content-Type", strings.ReplaceAll(ctype, boundary, "BOUNDARY"))
		body = bytes.ReplaceAll(body, []byte(boundary), []byte("BOUNDARY"))
	}
	return exchanged{res.StatusCode, h, body}
}

// Over TCP the bridge answers each request as http.ServeContent of the same
// bytes from a bytes.Reader under the same headers does: same status, headers
// but Date, and body.
func TestBridgeResponsesMatchServeContent(t *testing.T) {
	st, f := storeBridgedFile(t)
	etag := `"` + f.SHA256Hex + `"`
	mux := http.NewServeMux()
	mux.Handle("/xet-bridge/", server.NewHandler(server.WithStorage(st)))
	mux.HandleFunc("/baseline/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("ETag", etag)
		http.ServeContent(w, r, f.SHA256Hex, time.Time{}, bytes.NewReader(f.Content))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	for _, c := range []struct {
		name, method string
		header       http.Header
	}{
		{"GET", http.MethodGet, nil},
		{"single range", http.MethodGet, http.Header{"Range": {"bytes=2000-105000"}}},
		{"multi-range", http.MethodGet, http.Header{"Range": {"bytes=0-99,103000-110000"}}},
		{"HEAD", http.MethodHead, nil},
		{"If-None-Match", http.MethodGet, http.Header{"If-None-Match": {etag}}},
		{"unsatisfiable", http.MethodGet, http.Header{"Range": {fmt.Sprintf("bytes=%d-", len(f.Content))}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := exchange(t, srv, c.method, "/xet-bridge/"+f.SHA256Hex, c.header)
			want := exchange(t, srv, c.method, "/baseline/"+f.SHA256Hex, c.header)
			if got.status != want.status || !maps.EqualFunc(got.header, want.header, slices.Equal) || !bytes.Equal(got.body, want.body) {
				t.Errorf("bridge answered %d %v with %d bytes; want http.ServeContent's %d %v with %d bytes", got.status, got.header, len(got.body), want.status, want.header, len(want.body))
			}
		})
	}
}

// Over TCP the bridge answers a GET of the all-zero digest, under which an
// empty file is stored, as http.ServeContent of an empty bytes.Reader does:
// 200 with Content-Length 0 and no body.
func TestBridgeZeroDigestMatchesServeContent(t *testing.T) {
	st, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	storagetest.PutFile(t, t.Context(), st, nil)
	digest := strings.Repeat("0", 64)
	mux := http.NewServeMux()
	mux.Handle("/xet-bridge/", server.NewHandler(server.WithStorage(st)))
	mux.HandleFunc("/baseline/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("ETag", `"`+digest+`"`)
		http.ServeContent(w, r, digest, time.Time{}, bytes.NewReader(nil))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	got := exchange(t, srv, http.MethodGet, "/xet-bridge/"+digest, nil)
	want := exchange(t, srv, http.MethodGet, "/baseline/"+digest, nil)
	if got.status != http.StatusOK || got.header.Get("Content-Length") != "0" || len(got.body) != 0 {
		t.Errorf("GET of the zero digest = %d with Content-Length %q and %d bytes; want 200 with 0 and none", got.status, got.header.Get("Content-Length"), len(got.body))
	}
	if got.status != want.status || !maps.EqualFunc(got.header, want.header, slices.Equal) || !bytes.Equal(got.body, want.body) {
		t.Errorf("bridge answered %d %v with %d bytes; want http.ServeContent's %d %v with %d bytes", got.status, got.header, len(got.body), want.status, want.header, len(want.body))
	}
}
