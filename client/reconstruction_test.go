package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/xorb"
)

func TestGetReconstruction(t *testing.T) {
	// Create a test hash
	testHash := xet.FileHash([32]byte{0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6, 0xa7, 0xb8, 0xc9, 0xd0, 0xe1, 0xf2, 0xa3, 0xb4, 0xc5, 0xd6, 0xe7, 0xf8, 0xa9, 0xb0, 0xc1, 0xd2, 0xe3, 0xf4, 0xa5, 0xb6, 0xc7, 0xd8, 0xe9, 0xf0, 0xa1, 0xb2})
	expectedPath := "/v1/reconstructions/" + testHash.String()

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != expectedPath {
			t.Errorf("Unexpected path: %s, expected: %s", r.URL.Path, expectedPath)
		}
		if r.Method != http.MethodGet {
			t.Errorf("Expected GET method, got %s", r.Method)
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("Expected Authorization header 'Bearer test-token', got '%s'", r.Header.Get("Authorization"))
		}

		resp := download.ReconstructionResponseV1{
			OffsetIntoFirstRange: 0,
			Terms: []download.Term{
				{
					Hash:           "chunk1",
					UnpackedLength: 1024,
					Range:          download.ChunkRange{Start: 0, End: 1},
				},
			},
			FetchInfo: map[string][]download.FetchInfoEntry{
				"xorb1": {
					{
						Range:    download.ChunkRange{Start: 0, End: 1},
						URL:      "https://example.com/xorb1",
						URLRange: download.ByteRange{Start: 0, End: 1023},
					},
				},
			},
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(WithBaseURL(server.URL), WithToken("test-token"))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	reconstruction, err := client.GetReconstructionV1(context.Background(), testHash, nil)
	if err != nil {
		t.Fatalf("GetReconstruction failed: %v", err)
	}

	if reconstruction.OffsetIntoFirstRange != 0 {
		t.Errorf("Expected OffsetIntoFirstRange 0, got %d", reconstruction.OffsetIntoFirstRange)
	}
	if len(reconstruction.Terms) != 1 {
		t.Errorf("Expected 1 term, got %d", len(reconstruction.Terms))
	}
	if reconstruction.Terms[0].Hash != "chunk1" {
		t.Errorf("Expected term hash 'chunk1', got '%s'", reconstruction.Terms[0].Hash)
	}
}

func TestGetReconstructionV1RetriesOnServer5xx(t *testing.T) {
	testHash := xet.FileHash([32]byte{0x11})
	var calls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/reconstructions/"+testHash.String() {
			t.Fatalf("Unexpected path: %s", r.URL.Path)
		}

		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			if _, err := w.Write([]byte("temporary backend error")); err != nil {
				t.Fatalf("write error body: %v", err)
			}
			return
		}

		resp := download.ReconstructionResponseV1{
			OffsetIntoFirstRange: 0,
			Terms:                []download.Term{},
			FetchInfo:            map[string][]download.FetchInfoEntry{},
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	c, err := NewClient(
		WithBaseURL(server.URL),
		WithRetries(2),
	)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	if _, err := c.GetReconstructionV1(context.Background(), testHash, nil); err != nil {
		t.Fatalf("GetReconstructionV1 failed: %v", err)
	}

	if got := calls.Load(); got != 2 {
		t.Fatalf("expected 2 attempts, got %d", got)
	}
}

func TestGetReconstructionError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		if _, err := w.Write([]byte("file not found")); err != nil {
			t.Fatalf("write error body: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(WithBaseURL(server.URL))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	hash := xet.FileHash{}
	_, err = client.GetReconstructionV1(context.Background(), hash, nil)
	if err == nil {
		t.Fatal("Expected error for 404 response")
	}
}

func TestGetReconstructionRange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "bytes=1000-2000" {
			t.Errorf("Expected Range header 'bytes=1000-2000', got '%s'", rangeHeader)
		}

		w.WriteHeader(http.StatusPartialContent)
		resp := download.ReconstructionResponseV1{
			OffsetIntoFirstRange: 1000,
			Terms:                []download.Term{},
			FetchInfo:            map[string][]download.FetchInfoEntry{},
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(WithBaseURL(server.URL))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	hash := xet.FileHash{}
	reconstruction, err := client.GetReconstructionV1(context.Background(), hash, http.Header{"Range": []string{"bytes=1000-2000"}})
	if err != nil {
		t.Fatalf("GetReconstructionRange failed: %v", err)
	}

	if reconstruction.OffsetIntoFirstRange != 1000 {
		t.Errorf("Expected OffsetIntoFirstRange 1000, got %d", reconstruction.OffsetIntoFirstRange)
	}
}

func TestGetReconstructionV2(t *testing.T) {
	testHash := xet.FileHash([32]byte{0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6, 0xa7, 0xb8, 0xc9, 0xd0, 0xe1, 0xf2, 0xa3, 0xb4, 0xc5, 0xd6, 0xe7, 0xf8, 0xa9, 0xb0, 0xc1, 0xd2, 0xe3, 0xf4, 0xa5, 0xb6, 0xc7, 0xd8, 0xe9, 0xf0, 0xa1, 0xb2})
	expectedPath := "/v2/reconstructions/" + testHash.String()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != expectedPath {
			t.Errorf("Unexpected path: %s, expected: %s", r.URL.Path, expectedPath)
		}
		if r.Method != http.MethodGet {
			t.Errorf("Expected GET method, got %s", r.Method)
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("Expected Authorization header 'Bearer test-token', got '%s'", r.Header.Get("Authorization"))
		}

		resp := download.ReconstructionResponseV2{
			OffsetIntoFirstRange: 0,
			Terms: []download.Term{
				{
					Hash:           "xorb1",
					UnpackedLength: 1024,
					Range:          download.ChunkRange{Start: 0, End: 1},
				},
			},
			Xorbs: map[string][]download.XorbMultiRangeFetch{
				"xorb1": {
					{
						URL: "https://example.com/xorb1",
						Ranges: []download.XorbRangeDescriptor{
							{
								Chunks: download.ChunkRange{Start: 0, End: 1},
								Bytes:  download.ByteRange{Start: 0, End: 1023},
							},
						},
					},
				},
			},
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	c, err := NewClient(WithBaseURL(server.URL), WithToken("test-token"))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	reconstruction, err := c.GetReconstructionV2(context.Background(), testHash, nil)
	if err != nil {
		t.Fatalf("GetReconstructionV2 failed: %v", err)
	}

	if reconstruction.OffsetIntoFirstRange != 0 {
		t.Errorf("Expected OffsetIntoFirstRange 0, got %d", reconstruction.OffsetIntoFirstRange)
	}
	if len(reconstruction.Terms) != 1 {
		t.Errorf("Expected 1 term, got %d", len(reconstruction.Terms))
	}
	if reconstruction.Terms[0].Hash != "xorb1" {
		t.Errorf("Expected term hash 'xorb1', got '%s'", reconstruction.Terms[0].Hash)
	}
	if len(reconstruction.Xorbs) != 1 {
		t.Errorf("Expected 1 xorb entry, got %d", len(reconstruction.Xorbs))
	}
}

func TestGetReconstructionV2Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		if _, err := w.Write([]byte("file not found")); err != nil {
			t.Fatalf("write error body: %v", err)
		}
	}))
	defer server.Close()

	c, err := NewClient(WithBaseURL(server.URL))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	hash := xet.FileHash{}
	_, err = c.GetReconstructionV2(context.Background(), hash, nil)
	if err == nil {
		t.Fatal("Expected error for 404 response")
	}
}

func TestGetReconstructionRangeV2(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "bytes=1000-2000" {
			t.Errorf("Expected Range header 'bytes=1000-2000', got '%s'", rangeHeader)
		}

		w.WriteHeader(http.StatusPartialContent)
		resp := download.ReconstructionResponseV2{
			OffsetIntoFirstRange: 1000,
			Terms:                []download.Term{},
			Xorbs:                map[string][]download.XorbMultiRangeFetch{},
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	c, err := NewClient(WithBaseURL(server.URL))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	hash := xet.FileHash{}
	reconstruction, err := c.GetReconstructionV2(context.Background(), hash, http.Header{"Range": []string{"bytes=1000-2000"}})
	if err != nil {
		t.Fatalf("GetReconstructionRangeV2 failed: %v", err)
	}

	if reconstruction.OffsetIntoFirstRange != 1000 {
		t.Errorf("Expected OffsetIntoFirstRange 1000, got %d", reconstruction.OffsetIntoFirstRange)
	}
}

// Servers answer a ranged reconstruction query with 200: the JSON body carries
// offset_into_first_range instead of a 206 status.
func TestGetReconstructionRangeStatusOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got != "bytes=1000-" {
			t.Errorf("Expected Range header 'bytes=1000-', got '%s'", got)
		}
		resp := download.ReconstructionResponseV1{
			OffsetIntoFirstRange: 1000,
			Terms:                []download.Term{},
			FetchInfo:            map[string][]download.FetchInfoEntry{},
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(WithBaseURL(server.URL))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	reconstruction, err := client.GetReconstructionV1(context.Background(), xet.FileHash{}, http.Header{"Range": []string{"bytes=1000-"}})
	if err != nil {
		t.Fatalf("GetReconstructionV1 rejected a 200 ranged response: %v", err)
	}
	if reconstruction.OffsetIntoFirstRange != 1000 {
		t.Errorf("Expected OffsetIntoFirstRange 1000, got %d", reconstruction.OffsetIntoFirstRange)
	}
}

func TestGetReconstructionRangeV2StatusOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got != "bytes=1000-" {
			t.Errorf("Expected Range header 'bytes=1000-', got '%s'", got)
		}
		resp := download.ReconstructionResponseV2{
			OffsetIntoFirstRange: 1000,
			Terms:                []download.Term{},
			Xorbs:                map[string][]download.XorbMultiRangeFetch{},
		}
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}))
	defer server.Close()

	c, err := NewClient(WithBaseURL(server.URL))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	reconstruction, err := c.GetReconstructionV2(context.Background(), xet.FileHash{}, http.Header{"Range": []string{"bytes=1000-"}})
	if err != nil {
		t.Fatalf("GetReconstructionV2 rejected a 200 ranged response: %v", err)
	}
	if reconstruction.OffsetIntoFirstRange != 1000 {
		t.Errorf("Expected OffsetIntoFirstRange 1000, got %d", reconstruction.OffsetIntoFirstRange)
	}
}

func TestGetReconstructionRangeErrorStatus(t *testing.T) {
	var status atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "denied", int(status.Load()))
	}))
	defer server.Close()

	c, err := NewClient(WithBaseURL(server.URL))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	header := http.Header{"Range": []string{"bytes=1000-"}}
	for _, tc := range []struct {
		status       int
		wantNotFound bool
	}{
		{status: http.StatusNotFound, wantNotFound: true},
		{status: http.StatusForbidden, wantNotFound: false},
	} {
		status.Store(int32(tc.status))
		_, errV1 := c.GetReconstructionV1(context.Background(), xet.FileHash{}, header)
		_, errV2 := c.GetReconstructionV2(context.Background(), xet.FileHash{}, header)
		for _, err := range []error{errV1, errV2} {
			if err == nil {
				t.Fatalf("status %d: expected error", tc.status)
			}
			if errors.Is(err, errNotFound) != tc.wantNotFound {
				t.Fatalf("status %d: errNotFound match = %v, want %v: %v", tc.status, !tc.wantNotFound, tc.wantNotFound, err)
			}
		}
	}
}

// downloadFixture is a CAS stub holding one single-term xorb per file, served
// through the V1, V2, and batch reconstruction APIs.
type downloadFixture struct {
	srv          *httptest.Server
	hashes       []xet.FileHash
	xorbHashes   []string
	data         [][]byte // original file contents
	served       [][]byte // encoded xorb bytes returned for each file
	rejectRange  bool     // refuse Range reconstruction requests
	offsetOnFull int64    // OffsetIntoFirstRange reported without a Range header
}

const fixtureChunkSize = 1000

// encodeFixtureXorb encodes two incompressible chunks so a substituted xorb
// has the same length as the original.
func encodeFixtureXorb(t *testing.T, seed int64) (encoded, data []byte, xorbHash xet.XorbHash, fileHash xet.FileHash) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	var buf bytes.Buffer
	enc := xorb.NewEncoder(&buf, false)
	var hashes []xet.ChunkHash
	var sizes []uint64
	for range 2 {
		chunk := make([]byte, fixtureChunkSize)
		rng.Read(chunk)
		if _, err := enc.Write(chunk); err != nil {
			t.Fatal(err)
		}
		data = append(data, chunk...)
		hashes = append(hashes, xet.ComputeChunkHash(chunk))
		sizes = append(sizes, fixtureChunkSize)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes(), data, enc.SummoryHash(), xet.ComputeFileHash(hashes, sizes)
}

func newDownloadFixture(t *testing.T, seeds ...int64) *downloadFixture {
	t.Helper()
	f := &downloadFixture{}
	for _, seed := range seeds {
		encoded, data, xorbHash, fileHash := encodeFixtureXorb(t, seed)
		f.hashes = append(f.hashes, fileHash)
		f.xorbHashes = append(f.xorbHashes, xorbHash.String())
		f.data = append(f.data, data)
		f.served = append(f.served, encoded)
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(f.srv.Close)
	return f
}

// substitute serves different bytes of the same length for file i.
func (f *downloadFixture) substitute(t *testing.T, i int) {
	t.Helper()
	encoded, _, _, _ := encodeFixtureXorb(t, int64(100+i))
	if len(encoded) != len(f.served[i]) {
		t.Fatalf("substituted xorb is %d bytes, want %d", len(encoded), len(f.served[i]))
	}
	f.served[i] = encoded
}

func (f *downloadFixture) index(fileHash string) int {
	for i, h := range f.hashes {
		if h.String() == fileHash {
			return i
		}
	}
	return -1
}

func (f *downloadFixture) term(i int) download.Term {
	return download.Term{Hash: f.xorbHashes[i], UnpackedLength: uint64(len(f.data[i])), Range: download.ChunkRange{Start: 0, End: 2}}
}

func (f *downloadFixture) fetchInfo(i int) download.FetchInfoEntry {
	return download.FetchInfoEntry{
		Range:    download.ChunkRange{Start: 0, End: 2},
		URL:      f.srv.URL + "/xorbs/" + strconv.Itoa(i),
		URLRange: download.ByteRange{Start: 0, End: int64(len(f.served[i]) - 1)},
	}
}

func (f *downloadFixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if name, ok := strings.CutPrefix(r.URL.Path, "/xorbs/"); ok {
		i, err := strconv.Atoi(name)
		if err != nil || i < 0 || i >= len(f.served) {
			http.NotFound(w, r)
			return
		}
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(f.served[i]))
		return
	}
	if r.URL.Path == "/reconstructions" {
		resp := download.BatchReconstructionResponse{Files: map[string][]download.Term{}, FetchInfo: map[string][]download.FetchInfoEntry{}}
		for _, id := range r.URL.Query()["file_id"] {
			if i := f.index(id); i >= 0 {
				resp.Files[id] = []download.Term{f.term(i)}
				resp.FetchInfo[f.xorbHashes[i]] = []download.FetchInfoEntry{f.fetchInfo(i)}
			}
		}
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	version, id, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/reconstructions/")
	i := f.index(id)
	if i < 0 {
		http.NotFound(w, r)
		return
	}
	offset := f.offsetOnFull
	if rg := r.Header.Get("Range"); rg != "" {
		if f.rejectRange {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if _, err := fmt.Sscanf(rg, "bytes=%d-", &offset); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
	}
	var resp any
	switch version {
	case "v1":
		resp = download.ReconstructionResponseV1{
			OffsetIntoFirstRange: offset,
			Terms:                []download.Term{f.term(i)},
			FetchInfo:            map[string][]download.FetchInfoEntry{f.xorbHashes[i]: {f.fetchInfo(i)}},
		}
	case "v2":
		info := f.fetchInfo(i)
		resp = download.ReconstructionResponseV2{
			OffsetIntoFirstRange: offset,
			Terms:                []download.Term{f.term(i)},
			Xorbs: map[string][]download.XorbMultiRangeFetch{f.xorbHashes[i]: {{
				URL:    info.URL,
				Ranges: []download.XorbRangeDescriptor{{Chunks: info.Range, Bytes: info.URLRange}},
			}}},
		}
	default:
		http.NotFound(w, r)
		return
	}
	_ = json.NewEncoder(w).Encode(resp)
}

// downloadInto runs one client download entry point into a temp file that
// already holds prefix, and returns the file contents.
func downloadInto(t *testing.T, prefix []byte, do func(w io.WriteSeeker) error) ([]byte, error) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "out")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(prefix); err != nil {
		t.Fatal(err)
	}
	doErr := do(f)
	got, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	return got, doErr
}

func wantHashMismatch(t *testing.T, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "file hash mismatch") {
		t.Fatalf("err = %v, want file hash mismatch", err)
	}
}

// TestDownloadFileVerifiesFileHash pins that whole downloads (including a
// restart after a rejected resume) are verified against the requested hash,
// while a true resume keeps appending the suffix unverified.
func TestDownloadFileVerifiesFileHash(t *testing.T) {
	entryPoints := map[string]func(c *Client, ctx context.Context, fileHash xet.FileHash, w io.WriteSeeker) error{
		"v1": (*Client).DownloadFileV1,
		"v2": (*Client).DownloadFileV2,
	}
	ctx := context.Background()
	for name, downloadFile := range entryPoints {
		t.Run(name, func(t *testing.T) {
			run := func(t *testing.T, fx *downloadFixture, prefix []byte) ([]byte, error) {
				t.Helper()
				c, err := NewClient(WithBaseURL(fx.srv.URL), WithCacheDir(t.TempDir()))
				if err != nil {
					t.Fatal(err)
				}
				return downloadInto(t, prefix, func(w io.WriteSeeker) error {
					return downloadFile(c, ctx, fx.hashes[0], w)
				})
			}

			t.Run("full", func(t *testing.T) {
				fx := newDownloadFixture(t, 1)
				if got, err := run(t, fx, nil); err != nil || !bytes.Equal(got, fx.data[0]) {
					t.Fatalf("got %d bytes, err %v; want %d bytes", len(got), err, len(fx.data[0]))
				}
			})

			t.Run("full substituted", func(t *testing.T) {
				fx := newDownloadFixture(t, 1)
				fx.substitute(t, 0)
				_, err := run(t, fx, nil)
				wantHashMismatch(t, err)
			})

			t.Run("resume", func(t *testing.T) {
				fx := newDownloadFixture(t, 1)
				prefix := fx.data[0][:fixtureChunkSize+1]
				if got, err := run(t, fx, prefix); err != nil || !bytes.Equal(got, fx.data[0]) {
					t.Fatalf("got %d bytes, err %v; want %d bytes", len(got), err, len(fx.data[0]))
				}
			})

			t.Run("restart after rejected resume", func(t *testing.T) {
				fx := newDownloadFixture(t, 1)
				fx.rejectRange = true
				prefix := fx.data[0][:fixtureChunkSize+1]
				if got, err := run(t, fx, prefix); err != nil || !bytes.Equal(got, fx.data[0]) {
					t.Fatalf("got %d bytes, err %v; want %d bytes", len(got), err, len(fx.data[0]))
				}
				fx.substitute(t, 0)
				_, err := run(t, fx, prefix)
				wantHashMismatch(t, err)
			})

			t.Run("offset in full response", func(t *testing.T) {
				fx := newDownloadFixture(t, 1)
				fx.offsetOnFull = 1
				if _, err := run(t, fx, nil); err == nil {
					t.Fatal("expected an error: a full download cannot verify a partial reconstruction")
				}
			})
		})
	}
}

// TestDownloadFilesVerifiesFileHash pins per-file verification of batch
// downloads: a substituted xorb fails only the reader of that file.
func TestDownloadFilesVerifiesFileHash(t *testing.T) {
	fx := newDownloadFixture(t, 1, 2)
	fx.substitute(t, 1)
	c, err := NewClient(WithBaseURL(fx.srv.URL), WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	readers, sizes, err := c.DownloadFiles(context.Background(), fx.hashes)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range readers {
		defer r.Close()
	}
	got, err := io.ReadAll(readers[0])
	if err != nil || !bytes.Equal(got, fx.data[0]) || sizes[0] != int64(len(fx.data[0])) {
		t.Fatalf("file 0: got %d bytes (size %d), err %v", len(got), sizes[0], err)
	}
	_, err = io.ReadAll(readers[1])
	wantHashMismatch(t, err)
}

// reply writes one reconstruction response given the healthy status and body.
type reply func(w http.ResponseWriter, code int, full []byte)

var (
	healthy   reply = func(w http.ResponseWriter, code int, full []byte) { w.WriteHeader(code); _, _ = w.Write(full) }
	empty     reply = func(w http.ResponseWriter, code int, _ []byte) { w.WriteHeader(code) }
	truncated reply = func(w http.ResponseWriter, code int, full []byte) {
		w.Header().Set("Content-Length", strconv.Itoa(len(full)))
		w.WriteHeader(code)
		_, _ = w.Write(full[:len(full)/2])
	}
	malformed reply = func(w http.ResponseWriter, code int, _ []byte) {
		w.WriteHeader(code)
		_, _ = io.WriteString(w, "<html>")
	}
	unavailable reply = func(w http.ResponseWriter, _ int, _ []byte) { http.Error(w, "busy", http.StatusServiceUnavailable) }
)

// replayFixture serves fx's reconstruction answers through replies in request
// order, repeating the last, while checking that every request carries the
// client token and wantRange.
func replayFixture(t *testing.T, fx *downloadFixture, wantRange string, replies ...reply) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	calls := new(atomic.Int32)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(calls.Add(1))
		if r.Header.Get("Range") != wantRange || r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("request %d: Range %q Authorization %q, want %q and the client token", n, r.Header.Get("Range"), r.Header.Get("Authorization"), wantRange)
		}
		rec := httptest.NewRecorder()
		fx.serveHTTP(rec, r)
		replies[min(n, len(replies))-1](w, rec.Code, rec.Body.Bytes())
	}))
	t.Cleanup(srv.Close)
	return srv, calls
}

// TestGetReconstructionRetriesCutBody ends the first metadata answer before
// its JSON is complete and expects one fresh GET with the same headers.
func TestGetReconstructionRetriesCutBody(t *testing.T) {
	fx := newDownloadFixture(t, 1)
	const resume = "bytes=1-"
	apis := []struct {
		name  string
		rng   string
		terms func(c *Client, ctx context.Context) ([]download.Term, error)
	}{
		{"v1", resume, func(c *Client, ctx context.Context) ([]download.Term, error) {
			resp, err := c.GetReconstructionV1(ctx, fx.hashes[0], http.Header{"Range": {resume}})
			if err != nil {
				return nil, err
			}
			return resp.Terms, nil
		}},
		{"v2", resume, func(c *Client, ctx context.Context) ([]download.Term, error) {
			resp, err := c.GetReconstructionV2(ctx, fx.hashes[0], http.Header{"Range": {resume}})
			if err != nil {
				return nil, err
			}
			return resp.Terms, nil
		}},
		{"batch", "", func(c *Client, ctx context.Context) ([]download.Term, error) {
			resp, err := c.GetBatchReconstruction(ctx, fx.hashes)
			if err != nil {
				return nil, err
			}
			return resp.Files[fx.hashes[0].String()], nil
		}},
	}
	cuts := []struct {
		name string
		reply
	}{{"empty", empty}, {"truncated", truncated}}
	for _, api := range apis {
		for _, cut := range cuts {
			t.Run(api.name+"/"+cut.name, func(t *testing.T) {
				srv, calls := replayFixture(t, fx, api.rng, cut.reply, healthy)
				c, err := NewClient(WithBaseURL(srv.URL), WithToken("test-token"), WithRetryBackoff(0))
				if err != nil {
					t.Fatal(err)
				}
				terms, err := api.terms(c, context.Background())
				if err != nil {
					t.Fatalf("%s after a %s first answer: %v", api.name, cut.name, err)
				}
				if got := calls.Load(); got != 2 {
					t.Fatalf("%d requests, want the cut answer and one retry", got)
				}
				if len(terms) != 1 || terms[0].Hash != fx.xorbHashes[0] {
					t.Fatalf("terms = %+v, want the fixture's single term", terms)
				}
			})
		}
	}
}

// TestGetReconstructionDecodeBudget pins that cut metadata bodies spend the
// same retries+1 budget as statuses, while malformed JSON is not retried.
func TestGetReconstructionDecodeBudget(t *testing.T) {
	fx := newDownloadFixture(t, 1)
	const retries = 2
	for _, tc := range []struct {
		name      string
		replies   []reply
		wantCalls int32
		wantErr   func(error) bool // nil: the call must succeed
	}{
		{"status then cut then healthy", []reply{unavailable, truncated, healthy}, 3, nil},
		{"permanent cut", []reply{truncated}, retries + 1, func(err error) bool { return errors.Is(err, io.ErrUnexpectedEOF) }},
		{"malformed", []reply{malformed}, 1, func(err error) bool {
			var syntaxErr *json.SyntaxError
			return errors.As(err, &syntaxErr)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, calls := replayFixture(t, fx, "", tc.replies...)
			c, err := NewClient(WithBaseURL(srv.URL), WithToken("test-token"), WithRetries(retries), WithRetryBackoff(0))
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.GetReconstructionV1(context.Background(), fx.hashes[0], nil)
			if tc.wantErr == nil && err != nil {
				t.Fatalf("GetReconstructionV1: %v", err)
			}
			if tc.wantErr != nil && (err == nil || !tc.wantErr(err)) {
				t.Fatalf("GetReconstructionV1 error = %v, want the original failure", err)
			}
			if got := calls.Load(); got != tc.wantCalls {
				t.Fatalf("%d requests, want %d", got, tc.wantCalls)
			}
		})
	}
}

// TestGetReconstructionCancelDuringBody cancels while the metadata body is
// held open: the call reports context.Canceled without another GET.
func TestGetReconstructionCancelDuringBody(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, `{"offset_into_first_range":0,`)
		_ = http.NewResponseController(w).Flush()
		cancel()
		<-r.Context().Done()
	}))
	defer srv.Close()

	c, err := NewClient(WithBaseURL(srv.URL), WithRetryBackoff(0))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.GetReconstructionV2(ctx, xet.FileHash{}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GetReconstructionV2 error = %v, want context.Canceled", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("%d requests, want no retry after cancellation", got)
	}
}
