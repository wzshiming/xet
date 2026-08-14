package e2e_test

import (
	"bytes"
	"context"
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
	"github.com/wzshiming/xet/storage"
)

// TestClientCacheEndToEnd verifies the client disk cache end to end:
// correctness, warm-download cache hits, and background compaction of
// overlapping same-xorb entries created by dedup'd downloads.
func TestClientCacheEndToEnd(t *testing.T) {
	uploadStorage, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
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
	c1, err := client.NewClient(client.WithBaseURL(srv.URL), client.WithCacheDir(cacheDir))
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

	multiObserved := countMultiEntryXorbDirs(t, cacheDir)
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
	for countMultiEntryXorbDirs(t, cacheDir) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("cache entries were not merged within the deadline")
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Log("overlapping entries were merged to one entry per xorb")

	// A fresh client on the same directory re-verifies checksums and must
	// serve both files from the merged entries without any fetch.
	c2, err := client.NewClient(client.WithBaseURL(srv.URL), client.WithCacheDir(cacheDir))
	if err != nil {
		t.Fatal(err)
	}
	download(t, c2, hash1, file1)
	download(t, c2, hash2, file2)
	if got := xorbGets.Load(); got != coldGets {
		t.Fatalf("fresh client issued %d xorb fetches, want all cache hits", got-coldGets)
	}
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
		hashDirs, err := os.ReadDir(filepath.Join(cacheDir, p.Name()))
		if err != nil {
			continue
		}
		for _, hd := range hashDirs {
			entries, err := os.ReadDir(filepath.Join(cacheDir, p.Name(), hd.Name()))
			if err != nil {
				continue
			}
			if len(entries) > 1 {
				count++
			}
		}
	}
	return count
}
