package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage/local"
)

// TestClientCacheEndToEnd verifies the client disk cache end to end:
// correctness, warm-download cache hits, and background compaction of
// overlapping same-xorb entries created by dedup'd downloads.
func TestClientCacheEndToEnd(t *testing.T) {
	uploadStorage, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	var xorbGets atomic.Int64
	handler := server.NewHandler(server.WithStorage(uploadStorage))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/xorbs/") {
			xorbGets.Add(1)
		}
		handler.ServeHTTP(w, r)
	}))
	defer srv.Close()

	cacheDir := t.TempDir()
	c1, err := client.NewClient(client.WithCache(client.NewCache(cacheDir, 0, 0)), client.WithUpstreamProvider(client.StaticUpstreamProvider(srv.URL, "")))
	if err != nil {
		t.Fatal(err)
	}

	// file2 shares file1's prefix so chunk boundaries align from byte 0 and
	// its upload dedups against file1's xorb; downloading file2 then file1
	// caches overlapping ranges of that xorb.
	file1 := deterministicData(10 * 1024 * 1024)
	file2 := append(append([]byte{}, file1[:5*1024*1024]...), bytes.Repeat([]byte{0xC7}, 3*1024*1024)...)

	ctx := context.Background()
	hash1, err := c1.UploadFile(ctx, bytes.NewReader(file1))
	if err != nil {
		t.Fatal(err)
	}
	hash2, err := c1.UploadFile(ctx, bytes.NewReader(file2))
	if err != nil {
		t.Fatal(err)
	}

	download := func(t *testing.T, c *client.Client, hash xet.FileHash, want []byte) {
		t.Helper()
		f, err := os.CreateTemp(t.TempDir(), "out")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := c.DownloadFile(ctx, hash, f); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(f.Name())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("downloaded %d bytes, mismatch with original %d bytes", len(got), len(want))
		}
	}

	// Cold downloads: file2 first caches a sub-range of the shared xorb,
	// file1 then caches the covering range.
	download(t, c1, hash2, file2)
	download(t, c1, hash1, file1)
	coldGets := xorbGets.Load()
	if coldGets == 0 {
		t.Fatal("cold downloads issued no xorb fetches")
	}

	multiObserved := countMultiEntryXorbDirs(t, filepath.Join(cacheDir, "download"))
	t.Logf("cold xorb GETs: %d, xorb dirs with multiple entries before merge: %d", coldGets, multiObserved)
	if multiObserved == 0 {
		t.Fatal("dedup downloads did not produce overlapping entries of one xorb")
	}

	// Warm downloads on the same client must be fully cache-served.
	download(t, c1, hash1, file1)
	download(t, c1, hash2, file2)
	if got := xorbGets.Load(); got != coldGets {
		t.Fatalf("warm downloads issued %d extra xorb fetches", got-coldGets)
	}

	// Background merge (2s quiet period) compacts each xorb to one entry.
	deadline := time.Now().Add(15 * time.Second)
	for countMultiEntryXorbDirs(t, filepath.Join(cacheDir, "download")) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("cache entries were not merged within the deadline")
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Log("overlapping entries were merged to one entry per xorb")

	// A fresh client on the same directory re-verifies checksums and must
	// serve both files from the merged entries without any fetch.
	c2, err := client.NewClient(client.WithCache(client.NewCache(cacheDir, 0, 0)), client.WithUpstreamProvider(client.StaticUpstreamProvider(srv.URL, "")))
	if err != nil {
		t.Fatal(err)
	}
	download(t, c2, hash1, file1)
	download(t, c2, hash2, file2)
	if got := xorbGets.Load(); got != coldGets {
		t.Fatalf("fresh client issued %d xorb fetches, want all cache hits", got-coldGets)
	}
}

// TestClientUploadCacheEndToEnd verifies the upload dedup cache end to end:
// every upload uses a fresh client, so a hit can only come from disk. Known
// bytes go up again with no dedup query and no xorb traffic, an extended file
// settles its prefix locally, checks one shard for the tail and sends only the
// new tail, and a second CAS sharing the cache root is never answered from the
// first one's locations.
func TestClientUploadCacheEndToEnd(t *testing.T) {
	ctx := context.Background()
	cacheDir := t.TempDir()
	upload := func(t *testing.T, baseURL string, data []byte) xet.FileHash {
		t.Helper()
		c, err := client.NewClient(client.WithCache(client.NewCache(cacheDir, 0, 0)), client.WithUpstreamProvider(client.StaticUpstreamProvider(baseURL, "")))
		if err != nil {
			t.Fatal(err)
		}
		hash, err := c.UploadFile(ctx, bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		return hash
	}

	srvA, a := newCountingCAS(t)
	file1 := deterministicData(10 * 1024 * 1024)
	hash1 := upload(t, srvA.URL, file1)
	if a.xorbPost.Load() == 0 || a.shardPost.Load() != 1 {
		t.Fatalf("cold upload: %s; want xorb uploads and one shard", a)
	}
	cached := uploadCacheEntries(t, cacheDir)
	if len(cached) == 0 {
		t.Fatal("cache after the cold upload holds no chunk locations")
	}
	t.Logf("cold upload: %s; %d chunk locations cached", a, len(cached))

	// The same bytes again: the cached locations settle every chunk without a request.
	a.reset()
	if got := upload(t, srvA.URL, file1); got != hash1 {
		t.Fatalf("warm upload hash = %s, want %s", got, hash1)
	}
	if a.dedup.Load() != 0 || a.xorbHead.Load() != 0 || a.xorbPost.Load() != 0 || a.shardPost.Load() != 1 {
		t.Fatalf("warm upload: %s; want the shard upload only", a)
	}

	// file1 plus a new tail: every prefix chunk hits the cache; one fetch of the shard behind the cached probe asks whether the tail is known, then only the tail is encoded and sent.
	file2 := append(append([]byte{}, file1...), invertedData(3*1024*1024)...)
	a.reset()
	upload(t, srvA.URL, file2)
	if a.dedup.Load() != 1 || a.xorbPost.Load() != 1 || a.xorbBytes.Load() >= int64(len(file1)) || a.shardPost.Load() != 1 {
		t.Fatalf("extended upload: %s; want one shard fetch and one xorb smaller than the %d-byte prefix", a, len(file1))
	}
	t.Logf("extended upload: %s", a)

	// Another CAS through the same root starts cold and then hits its own entries.
	srvB, b := newCountingCAS(t)
	if got := upload(t, srvB.URL, file1); got != hash1 {
		t.Fatalf("upload to the second CAS hash = %s, want %s", got, hash1)
	}
	if b.dedup.Load() == 0 || b.xorbPost.Load() == 0 || b.shardPost.Load() != 1 {
		t.Fatalf("cold upload to the second CAS: %s; want its own dedup query and xorb uploads", b)
	}
	b.reset()
	upload(t, srvB.URL, file1)
	if b.dedup.Load() != 0 || b.xorbHead.Load() != 0 || b.xorbPost.Load() != 0 || b.shardPost.Load() != 1 {
		t.Fatalf("warm upload to the second CAS: %s; want the shard upload only", b)
	}
	endpoints := map[string]bool{}
	for _, entry := range uploadCacheEntries(t, cacheDir) {
		endpoint, _, _ := strings.Cut(entry, "/")
		endpoints[endpoint] = true
	}
	if len(endpoints) != 2 {
		t.Fatalf("cached chunk locations under %d endpoint directories; want one per CAS", len(endpoints))
	}
}

// Chunks shared with an earlier upload dedup locally even when none of the new file's probes is known: a file wrapping A in new data sends far less than A again.
func TestClientUploadCacheDedupsSharedChunksWithoutProbeHit(t *testing.T) {
	ctx := context.Background()
	cacheDir := t.TempDir()
	srv, counters := newCountingCAS(t)
	upload := func(data []byte) {
		t.Helper()
		c, err := client.NewClient(client.WithCache(client.NewCache(cacheDir, 0, 0)), client.WithUpstreamProvider(client.StaticUpstreamProvider(srv.URL, "")))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.UploadFile(ctx, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
	}
	a := deterministicData(4 * 1024 * 1024)
	upload(a)
	counters.reset()
	b := append(append(invertedData(1024*1024), a...), invertedData(2 * 1024 * 1024)[1024*1024:]...)
	upload(b)
	if counters.xorbBytes.Load() >= 3*1024*1024 {
		t.Fatalf("upload wrapping a known %d-byte file: %s; want xorb bytes well below the known part", len(a), counters)
	}
	t.Logf("upload wrapping a known %d-byte file: %s", len(a), counters)
}

// casCounters tallies the CAS requests an upload makes.
type casCounters struct {
	dedup, xorbHead, xorbPost, xorbBytes, shardPost atomic.Int64
}

func (c *casCounters) reset() {
	for _, n := range []*atomic.Int64{&c.dedup, &c.xorbHead, &c.xorbPost, &c.xorbBytes, &c.shardPost} {
		n.Store(0)
	}
}

func (c *casCounters) String() string {
	return fmt.Sprintf("%d /v1/chunks requests, %d xorb HEAD, %d xorb POST (%d bytes), %d shard POST", c.dedup.Load(), c.xorbHead.Load(), c.xorbPost.Load(), c.xorbBytes.Load(), c.shardPost.Load())
}

// newCountingCAS serves a fresh local store behind counters for the dedup, xorb and shard routes.
func newCountingCAS(t *testing.T) (*httptest.Server, *casCounters) {
	t.Helper()
	stor, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := server.NewHandler(server.WithStorage(stor))
	c := &casCounters{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/v1/chunks/"):
			c.dedup.Add(1)
		case strings.HasPrefix(r.URL.Path, "/v1/xorbs/") && r.Method == http.MethodHead:
			c.xorbHead.Add(1)
		case strings.HasPrefix(r.URL.Path, "/v1/xorbs/") && r.Method == http.MethodPost:
			c.xorbPost.Add(1)
			c.xorbBytes.Add(r.ContentLength)
		case r.URL.Path == "/v1/shards" && r.Method == http.MethodPost:
			c.shardPost.Add(1)
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, c
}

// uploadCacheEntries lists the chunk location entries of the upload cache as
// upload/chunks-relative paths <endpoint>/<h[:2]>/<h[2:4]>/<h[4:]>.
func uploadCacheEntries(t *testing.T, cacheDir string) []string {
	t.Helper()
	root := filepath.Join(cacheDir, "upload", "chunks")
	var entries []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if strings.Count(rel, "/") != 3 {
			return fmt.Errorf("chunk entry at unexpected depth: %s", rel)
		}
		entries = append(entries, rel)
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return entries
}

// countMultiEntryXorbDirs counts xorb hash directories holding more than one
// cache entry file.
func countMultiEntryXorbDirs(t *testing.T, cacheDir string) int {
	t.Helper()
	count := 0
	prefixes, err := os.ReadDir(cacheDir)
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range prefixes {
		if !p.IsDir() {
			continue
		}
		secondPrefixes, err := os.ReadDir(filepath.Join(cacheDir, p.Name()))
		if err != nil {
			continue
		}
		for _, secondPrefix := range secondPrefixes {
			hashDirs, err := os.ReadDir(filepath.Join(cacheDir, p.Name(), secondPrefix.Name()))
			if err != nil {
				continue
			}
			for _, hashDir := range hashDirs {
				entries, err := os.ReadDir(filepath.Join(cacheDir, p.Name(), secondPrefix.Name(), hashDir.Name()))
				if err != nil {
					continue
				}
				if len(entries) > 1 {
					count++
				}
			}
		}
	}
	return count
}
