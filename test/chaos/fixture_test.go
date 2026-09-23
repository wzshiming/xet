package chaos_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime/pprof"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage/local"
)

const (
	idleTimeout = 200 * time.Millisecond // stall and trickle tests opt in with client.WithIdleTimeout(idleTimeout)
	backoffBase = 2 * time.Millisecond   // keeps retries quick; TestRetryBackoffSpacesAttempts raises it
	testTimeout = 15 * time.Second
)

// fixture is a real CAS backend whose xorb URLs point at the fault proxy. file2
// spans two xorbs because its first chunk dedups against an earlier upload;
// small rides along in batch downloads.
type fixture struct {
	proxy   *faultProxy
	storage *local.Storage

	file2, small     []byte
	hash2, hashSmall xet.FileHash
	terms            []download.Term
	fetch            map[string][]download.FetchInfoEntry
	first, big       task // term 0 (one chunk) and term 1 (several chunks of another xorb)
}

// task is one xorb byte range the client fetches for a term.
type task struct {
	hash       string
	chunks     download.ChunkRange
	start, end int64
}

func (tk task) rangeHeader() string { return fmt.Sprintf("bytes=%d-%d", tk.start, tk.end) }

// match reports whether r is a GET for exactly this range.
func (tk task) match(r record) bool {
	return r.xorbGet() && strings.Contains(r.Path, tk.hash) && r.Range == tk.rangeHeader()
}

// within matches GETs of this range that start at most prefix bytes in: the
// original request and any resume of it.
func (tk task) within(prefix int64) func(record) bool {
	return func(r record) bool {
		var start, end int64
		if !r.xorbGet() || !strings.Contains(r.Path, tk.hash) {
			return false
		}
		if _, err := fmt.Sscanf(r.Range, "bytes=%d-%d", &start, &end); err != nil {
			return false
		}
		return end == tk.end && start >= tk.start && start <= tk.start+prefix
	}
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	proxy := newFaultProxy()
	stor, err := local.NewStorage(local.WithBasePath(t.TempDir()), local.WithBaseURL(proxy.srv.URL))
	if err != nil {
		proxy.Close()
		t.Fatal(err)
	}
	backend := httptest.NewServer(server.NewHandler(server.WithStorage(stor)))
	// Stalled proxy handlers hold backend responses open, so the proxy drains first.
	t.Cleanup(func() {
		proxy.Close()
		backend.Close()
	})
	proxy.setTarget(backend.URL)

	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	uploader := newClient(t, backend.URL, t.TempDir())
	file1 := deterministicData(1 << 20)
	file2 := append(append([]byte{}, file1[:512<<10]...), bytes.Repeat([]byte{0xC7}, 512<<10)...)
	small := bytes.Repeat([]byte("chaos"), 4096)
	if _, err := uploader.UploadFile(ctx, bytes.NewReader(file1)); err != nil {
		t.Fatal(err)
	}
	hash2, err := uploader.UploadFile(ctx, bytes.NewReader(file2))
	if err != nil {
		t.Fatal(err)
	}
	hashSmall, err := uploader.UploadFile(ctx, bytes.NewReader(small))
	if err != nil {
		t.Fatal(err)
	}
	layout, err := uploader.GetReconstructionV1(ctx, hash2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(layout.Terms) < 2 || layout.Terms[0].Hash == layout.Terms[1].Hash {
		t.Fatalf("fixture: terms %+v, want two terms from different xorbs", layout.Terms)
	}
	if n := layout.Terms[1].Range.End - layout.Terms[1].Range.Start; n < 2 {
		t.Fatalf("fixture: second term spans %d chunk, want several: %+v", n, layout.Terms)
	}
	fx := &fixture{
		proxy:     proxy,
		storage:   stor,
		file2:     file2,
		small:     small,
		hash2:     hash2,
		hashSmall: hashSmall,
		terms:     layout.Terms,
		fetch:     layout.FetchInfo,
	}
	fx.first, fx.big = fx.taskFor(t, 0), fx.taskFor(t, 1)
	return fx
}

func deterministicData(size int) []byte {
	data := make([]byte, size)
	var state uint32 = 1
	for i := range data {
		state = state*1664525 + 1013904223
		data[i] = byte(state >> 24)
	}
	return data
}

// newClient returns a client on an owned transport that is drained at cleanup.
// The idle timeout is generous so only tests that stall opt in to a short one.
func newClient(t *testing.T, baseURL, cacheDir string, opts ...client.Options) *client.Client {
	t.Helper()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(transport.CloseIdleConnections)
	opts = append([]client.Options{
		client.WithBaseURL(baseURL),
		client.WithCacheDir(cacheDir),
		client.WithHTTPClient(&http.Client{Transport: transport}),
		client.WithIdleTimeout(testTimeout),
		client.WithRetryBackoff(backoffBase),
	}, opts...)
	c, err := client.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// proxyClient downloads through the fault proxy using cacheDir.
func (fx *fixture) proxyClient(t *testing.T, cacheDir string, opts ...client.Options) *client.Client {
	t.Helper()
	return newClient(t, fx.proxy.srv.URL, cacheDir, opts...)
}

// taskFor returns the byte range the server advertises for file2's term.
func (fx *fixture) taskFor(t *testing.T, term int) task {
	t.Helper()
	for _, entry := range fx.fetch[fx.terms[term].Hash] {
		if entry.Range == fx.terms[term].Range {
			return task{hash: fx.terms[term].Hash, chunks: entry.Range, start: entry.URLRange.Start, end: entry.URLRange.End}
		}
	}
	t.Fatalf("fixture: no fetch info covers term %d %+v", term, fx.terms[term])
	return task{}
}

// chunkBounds returns the absolute end offset of every packed chunk in the task.
func (fx *fixture) chunkBounds(t *testing.T, tk task) []int64 {
	t.Helper()
	hash, err := xet.ParseXorbHash(tk.hash)
	if err != nil {
		t.Fatal(err)
	}
	offsets, err := fx.storage.GetXorbChunkOffsets(context.Background(), "default", hash)
	if err != nil {
		t.Fatal(err)
	}
	var bounds []int64
	for idx := tk.chunks.Start; idx < tk.chunks.End; idx++ {
		bounds = append(bounds, int64(offsets[idx]))
	}
	if bounds[len(bounds)-1] != tk.end+1 {
		t.Fatalf("fixture: task %+v ends at %d, chunk bounds %v", tk, tk.end, bounds)
	}
	return bounds
}

// midChunkPrefix returns a wire prefix ending strictly inside the task's second chunk.
func (fx *fixture) midChunkPrefix(t *testing.T, tk task) int64 {
	t.Helper()
	bounds := fx.chunkBounds(t, tk)
	if len(bounds) < 2 || bounds[1]-bounds[0] < 2 {
		t.Fatalf("fixture: chunk bounds %v leave no room inside the second chunk", bounds)
	}
	prefix := (bounds[0]+bounds[1])/2 - tk.start
	if tk.start+prefix <= bounds[0] || slices.Contains(bounds, tk.start+prefix) {
		t.Fatalf("fixture: prefix %d from %d lands on a chunk boundary %v", prefix, tk.start, bounds)
	}
	return prefix
}

// maxChunk returns the largest packed chunk size in the task.
func (fx *fixture) maxChunk(t *testing.T, tk task) int64 {
	t.Helper()
	var largest int64
	prev := tk.start
	for _, bound := range fx.chunkBounds(t, tk) {
		largest = max(largest, bound-prev)
		prev = bound
	}
	return largest
}

type apiVersion int

const (
	apiAuto apiVersion = iota
	apiV1
	apiV2
	apiBatch
)

var apiNames = [...]string{"auto", "v1", "v2", "batch"}

func (v apiVersion) String() string { return apiNames[v] }

// output hides os.File's ReadFrom so every byte passes through Write and rewinds are visible.
type output struct {
	f       *os.File
	written int64
	rewound bool
}

func newOutput(t *testing.T, seed []byte) *output {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "out")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	if _, err := f.Write(seed); err != nil {
		t.Fatal(err)
	}
	return &output{f: f}
}

func (o *output) Write(p []byte) (int, error) {
	n, err := o.f.Write(p)
	o.written += int64(n)
	return n, err
}

func (o *output) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekStart && offset == 0 {
		o.rewound = true
	}
	return o.f.Seek(offset, whence)
}

func (o *output) bytes(t *testing.T) []byte {
	t.Helper()
	got, err := os.ReadFile(o.f.Name())
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// download fetches file2 through api into out; batch mode also drains small and checks it.
func (fx *fixture) download(ctx context.Context, c *client.Client, api apiVersion, out *output) error {
	switch api {
	case apiAuto:
		return c.DownloadFile(ctx, fx.hash2, out)
	case apiV1:
		return c.DownloadFileV1(ctx, fx.hash2, out)
	case apiV2:
		return c.DownloadFileV2(ctx, fx.hash2, out)
	}
	readers, sizes, err := c.DownloadFiles(ctx, []xet.FileHash{fx.hash2, fx.hashSmall})
	if err != nil {
		return err
	}
	for _, r := range readers {
		if r != nil {
			defer r.Close()
		}
	}
	if readers[0] == nil || readers[1] == nil || sizes[0] != int64(len(fx.file2)) || sizes[1] != int64(len(fx.small)) {
		return fmt.Errorf("batch: readers %v sizes %v", readers, sizes)
	}
	if _, err := io.Copy(out, readers[0]); err != nil {
		return err
	}
	small, err := io.ReadAll(readers[1])
	if err != nil {
		return err
	}
	if !bytes.Equal(small, fx.small) {
		return fmt.Errorf("batch: small file has %d bytes and differs from the %d uploaded", len(small), len(fx.small))
	}
	return nil
}

// mustDownload downloads file2 into a fresh output and requires exact bytes.
func (fx *fixture) mustDownload(t *testing.T, ctx context.Context, c *client.Client, api apiVersion) *output {
	t.Helper()
	out := newOutput(t, nil)
	if err := fx.download(ctx, c, api, out); err != nil {
		t.Fatalf("%s download: %v", api, err)
	}
	fx.requireFile2(t, out)
	return out
}

func (fx *fixture) requireFile2(t *testing.T, out *output) {
	t.Helper()
	if got := out.bytes(t); !bytes.Equal(got, fx.file2) {
		t.Fatalf("output has %d bytes and differs from file2 (%d bytes)", len(got), len(fx.file2))
	}
}

func denyXorbs(r record) fault {
	if r.xorbGet() {
		return fault{kind: injectStatus, status: http.StatusForbidden}
	}
	return fault{}
}

// assertCached proves cacheDir holds all of file2: a fresh client succeeds while every xorb GET is denied.
func (fx *fixture) assertCached(t *testing.T, ctx context.Context, cacheDir string) {
	t.Helper()
	fx.proxy.arm(denyXorbs)
	fx.mustDownload(t, ctx, fx.proxyClient(t, cacheDir), apiV1)
	if gets := filter(fx.proxy.settle(t), record.xorbGet); len(gets) != 0 {
		t.Fatalf("cached rerun issued %d xorb GETs: %+v", len(gets), gets)
	}
}

// heal completes file2 on cacheDir with no faults, proves the cache is whole,
// and returns the xorb GETs the completion needed.
func (fx *fixture) heal(t *testing.T, ctx context.Context, cacheDir string) []record {
	t.Helper()
	fx.proxy.arm(nil)
	fx.mustDownload(t, ctx, fx.proxyClient(t, cacheDir), apiAuto)
	gets := filter(fx.proxy.settle(t), record.xorbGet)
	fx.assertCached(t, ctx, cacheDir)
	return gets
}

func filter(recs []record, match func(record) bool) []record {
	var out []record
	for _, r := range recs {
		if match(r) {
			out = append(out, r)
		}
	}
	return out
}

func xorbOf(hash string) func(record) bool {
	return func(r record) bool { return r.xorbGet() && strings.Contains(r.Path, hash) }
}

// parseRange returns the inclusive bounds of a bytes=start-end header.
func parseRange(t *testing.T, header string) (start, end int64) {
	t.Helper()
	if _, err := fmt.Sscanf(header, "bytes=%d-%d", &start, &end); err != nil {
		t.Fatalf("Range %q: %v", header, err)
	}
	return start, end
}

// requireAccounting checks that gets fetched every file2 xorb, each byte range
// once, and nothing outside the layout except extra ranges when allowed.
func (fx *fixture) requireAccounting(t *testing.T, gets []record, extra int) {
	t.Helper()
	seen := make(map[string]bool)
	foreign := 0
	for _, g := range gets {
		key := g.Path + " " + g.Range
		if seen[key] {
			t.Fatalf("range fetched twice: %+v", g)
		}
		seen[key] = true
		known := false
		for _, term := range fx.terms {
			known = known || strings.Contains(g.Path, term.Hash)
		}
		if !known {
			foreign++
		}
	}
	for _, term := range fx.terms {
		if len(filter(gets, xorbOf(term.Hash))) == 0 {
			t.Fatalf("xorb %s never fetched: %+v", term.Hash, gets)
		}
	}
	if foreign != extra {
		t.Fatalf("%d GETs outside file2's layout, want %d: %+v", foreign, extra, gets)
	}
}

// assertNoPrefetchWorkers waits for every prefetch worker goroutine to exit.
func assertNoPrefetchWorkers(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for {
		var buf bytes.Buffer
		_ = pprof.Lookup("goroutine").WriteTo(&buf, 2)
		if !strings.Contains(buf.String(), "download.(*prefetcher).runJob") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("prefetch workers still running after %v:\n%s", waitTimeout, buf.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
