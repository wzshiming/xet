package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/storagetest"
	"github.com/wzshiming/xet/xorb"
)

type recordedRequest struct {
	method string
	path   string
	ranged bool
}

// countingTransport records the S3 client's requests and can fail or truncate chosen ones.
type countingTransport struct {
	mu       sync.Mutex
	requests []recordedRequest
	fail     func(*http.Request) bool
	truncate func(*http.Request) bool
}

func (ct *countingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ct.mu.Lock()
	ct.requests = append(ct.requests, recordedRequest{req.Method, req.URL.Path, req.Header.Get("Range") != ""})
	fail, truncate := ct.fail, ct.truncate
	ct.mu.Unlock()
	if fail != nil && fail(req) {
		return nil, fmt.Errorf("injected failure: %s %s", req.Method, req.URL.Path)
	}
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err == nil && truncate != nil && truncate(req) {
		resp.Body = &truncatedBody{ReadCloser: resp.Body}
	}
	return resp, err
}

// count returns how many method requests went to a path containing pathContains.
func (ct *countingTransport) count(method, pathContains string) int {
	return ct.matching(method, pathContains, false)
}

// ranged is count restricted to requests that carried a Range header.
func (ct *countingTransport) ranged(method, pathContains string) int {
	return ct.matching(method, pathContains, true)
}

func (ct *countingTransport) matching(method, pathContains string, rangedOnly bool) int {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	n := 0
	for _, r := range ct.requests {
		if r.method == method && strings.Contains(r.path, pathContains) && (r.ranged || !rangedOnly) {
			n++
		}
	}
	return n
}

func (ct *countingTransport) setFail(fail func(*http.Request) bool) {
	ct.mu.Lock()
	ct.fail = fail
	ct.mu.Unlock()
}

func (ct *countingTransport) setTruncate(truncate func(*http.Request) bool) {
	ct.mu.Lock()
	ct.truncate = truncate
	ct.mu.Unlock()
}

// reset forgets the recorded requests and stops injecting faults.
func (ct *countingTransport) reset() {
	ct.mu.Lock()
	ct.requests, ct.fail, ct.truncate = nil, nil, nil
	ct.mu.Unlock()
}

// truncatedBody passes at most one byte of the response through, then fails.
type truncatedBody struct {
	io.ReadCloser
	done bool
}

func (b *truncatedBody) Read(p []byte) (int, error) {
	if b.done {
		return 0, errors.New("injected truncation")
	}
	b.done = true
	return b.ReadCloser.Read(p[:min(len(p), 1)])
}

// newCountingS3Storage is newTestS3Storage with requests recorded and SDK retries off.
func newCountingS3Storage(t *testing.T, opts ...Option) (*Storage, *countingTransport) {
	t.Helper()
	backend := s3mem.New()
	if err := backend.CreateBucket("test-bucket"); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(srv.Close)

	ct := &countingTransport{}
	client := awss3.New(awss3.Options{
		BaseEndpoint:               aws.String(srv.URL),
		Region:                     "us-east-1",
		Credentials:                credentials.NewStaticCredentialsProvider("test", "test", ""),
		UsePathStyle:               true,
		HTTPClient:                 &http.Client{Transport: ct},
		Retryer:                    aws.NopRetryer{},
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	ss, err := NewStorage(context.Background(), append([]Option{
		WithCacheDir(t.TempDir()),
		WithS3Client(client),
		WithBucket("test-bucket"),
	}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return ss, ct
}

// putRandomXorb stores an xorb of four incompressible 4 KiB chunks drawn from seed.
func putRandomXorb(t *testing.T, ss *Storage, seed byte) ([]byte, xet.XorbHash) {
	t.Helper()
	rng := rand.NewChaCha8([32]byte{seed})
	chunks := make([][]byte, 4)
	for i := range chunks {
		chunks[i] = make([]byte, 4096)
		rng.Read(chunks[i])
	}
	encoded, xorbHash := storagetest.EncodeXorb(t, true, chunks...)
	if _, err := ss.PutXorb(context.Background(), xorbHash, bytes.NewReader(encoded)); err != nil {
		t.Fatal(err)
	}
	return encoded, xorbHash
}

func mustOpenXorb(t *testing.T, ss *Storage, xorbHash xet.XorbHash) io.ReadSeekCloser {
	t.Helper()
	rc, err := ss.GetXorbReadSeekCloser(context.Background(), xorbHash)
	if err != nil {
		t.Fatalf("GetXorbReadSeekCloser: %v", err)
	}
	return rc
}

// drained is what a reader returned up to EOF, or its first error.
type drained struct {
	data []byte
	err  error
}

// drainXorb opens, reads and closes the whole xorb; it takes no *testing.T so goroutines can call it.
func drainXorb(ss *Storage, xorbHash xet.XorbHash) drained {
	rc, err := ss.GetXorbReadSeekCloser(context.Background(), xorbHash)
	if err != nil {
		return drained{err: err}
	}
	data, err := io.ReadAll(rc)
	if closeErr := rc.Close(); err == nil {
		err = closeErr
	}
	return drained{data, err}
}

func assertDrained(t *testing.T, what string, got drained, want []byte) {
	t.Helper()
	if got.err != nil {
		t.Fatalf("%s: %v", what, got.err)
	}
	if !bytes.Equal(got.data, want) {
		t.Fatalf("%s read %d bytes that differ from the %d stored bytes", what, len(got.data), len(want))
	}
}

// cachedXorbFiles counts the regular files, temporary ones included, under the cache's xorbs directory.
func cachedXorbFiles(t *testing.T, ss *Storage) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(filepath.Join(ss.cacheDir, "xorbs"), func(_ string, d fs.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() {
			n++
		}
		return err
	})
	if err != nil {
		t.Fatalf("list the xorb cache: %v", err)
	}
	return n
}

func TestS3XorbSecondReadHitsCache(t *testing.T) {
	ss, ct := newCountingS3Storage(t)
	encoded, x := putRandomXorb(t, ss, 1)
	if cached := ss.xorbCache.Len(); cached != 0 {
		t.Fatalf("xorbCache.Len() after PutXorb = %d, want 0", cached)
	}
	ct.reset()
	key := ss.objectKey("xorbs", x.String())

	assertDrained(t, "first read", drainXorb(ss, x), encoded)
	if gets, heads := ct.count(http.MethodGet, key), ct.count(http.MethodHead, key); gets != 1 || heads != 0 {
		t.Fatalf("first read sent %d GET and %d HEAD requests for %s; want 1 and 0", gets, heads, key)
	}
	assertDrained(t, "second read", drainXorb(ss, x), encoded)
	if gets := ct.count(http.MethodGet, key); gets != 1 {
		t.Fatalf("GET requests for %s after a second read = %d, want still 1", key, gets)
	}
}

func TestS3XorbRangesAndOffsetsOneGet(t *testing.T) {
	ctx := context.Background()
	ss, ct := newCountingS3Storage(t)
	encoded, x := putRandomXorb(t, ss, 1)
	ct.reset()

	offsets, err := ss.GetXorbChunkOffsets(ctx, x)
	if err != nil {
		t.Fatal(err)
	}
	want, err := storage.ReadXorbChunkOffsets(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(offsets, want) {
		t.Fatalf("GetXorbChunkOffsets = %v, want %v", offsets, want)
	}
	rc := mustOpenXorb(t, ss, x)
	defer rc.Close()
	reads := 0
	for i := range uint32(len(offsets)) {
		for j := i + 1; j <= uint32(len(offsets)); j++ {
			start, end, err := xorb.ChunkDataRangeFromOffsets(offsets, i, j)
			if err != nil {
				t.Fatal(err)
			}
			got := make([]byte, end-start+1)
			if _, err = rc.Seek(start, io.SeekStart); err == nil {
				_, err = io.ReadFull(rc, got)
			}
			if err != nil || !bytes.Equal(got, encoded[start:end+1]) {
				t.Fatalf("chunks [%d, %d): Seek(%d) and a read of %d bytes = %v; want the encoded bytes", i, j, start, len(got), err)
			}
			reads++
		}
	}

	key := ss.objectKey("xorbs", x.String())
	if gets, ranged := ct.count(http.MethodGet, key), ct.ranged(http.MethodGet, key); gets != 1 || ranged != 0 {
		t.Fatalf("chunk offsets and %d ranged reads sent %d GET requests for %s, %d with a Range header; want 1, 0", reads, gets, key, ranged)
	}
}

func TestS3XorbEvictionRedownloads(t *testing.T) {
	ss, ct := newCountingS3Storage(t, WithXorbCacheSize(1))
	encodedX, x := putRandomXorb(t, ss, 1)
	encodedY, y := putRandomXorb(t, ss, 2)
	ct.reset()

	assertDrained(t, "first read of X", drainXorb(ss, x), encodedX)
	assertDrained(t, "read of Y evicting X", drainXorb(ss, y), encodedY)
	assertDrained(t, "second read of X", drainXorb(ss, x), encodedX)
	keyX := ss.objectKey("xorbs", x.String())
	if gets, cached := ct.count(http.MethodGet, keyX), ss.xorbCache.Len(); gets != 2 || cached != 1 {
		t.Fatalf("reading X, Y, X through a 1-entry cache: %d GET requests for X, xorbCache.Len() = %d; want 2, 1", gets, cached)
	}
}

func TestS3XorbCacheDirBounded(t *testing.T) {
	ss := newTestS3Storage(t, WithXorbCacheSize(2))
	for seed := range byte(5) {
		encoded, x := putRandomXorb(t, ss, seed+1)
		assertDrained(t, fmt.Sprintf("read of xorb %d", seed+1), drainXorb(ss, x), encoded)
		if _, err := ss.GetXorbChunkOffsets(context.Background(), x); err != nil {
			t.Fatalf("GetXorbChunkOffsets of xorb %d: %v", seed+1, err)
		}
	}
	files, cached, mapped := cachedXorbFiles(t, ss), ss.xorbCache.Len(), ss.xorbCache.Mapped()
	if files != 2 || cached != 2 || mapped != 2 {
		t.Fatalf("after reading 5 xorbs and their chunk offsets through a 2-entry cache: %d cache files, xorbCache.Len() = %d, Mapped() = %d; want 2, 2, 2", files, cached, mapped)
	}
}

func TestS3XorbEvictedFileRemovedAfterReaderClose(t *testing.T) {
	ss := newTestS3Storage(t, WithXorbCacheSize(1))
	encodedX, x := putRandomXorb(t, ss, 1)
	encodedY, y := putRandomXorb(t, ss, 2)
	a := mustOpenXorb(t, ss, x)
	defer a.Close()
	assertDrained(t, "read of Y evicting X", drainXorb(ss, y), encodedY)

	path := ss.xorbCachePath(x)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("evicted X's cache file while reader A holds X: %v", err)
	}
	if mapped := ss.xorbCache.Mapped(); mapped != 2 {
		t.Fatalf("xorbCache.Mapped() with evicted X held by reader A = %d, want 2", mapped)
	}
	rest, err := io.ReadAll(a)
	assertDrained(t, "reader A of evicted X", drained{rest, err}, encodedX)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("X's cache file after its last reader closed: stat error = %v, want fs.ErrNotExist", err)
	}
	if mapped := ss.xorbCache.Mapped(); mapped != 1 {
		t.Fatalf("xorbCache.Mapped() after evicted X's last reader closed = %d, want 1", mapped)
	}
}

func TestS3XorbRefillKeepsHeldOldMappingSeparate(t *testing.T) {
	ss, ct := newCountingS3Storage(t, WithXorbCacheSize(1))
	encodedX, x := putRandomXorb(t, ss, 1)
	encodedY, y := putRandomXorb(t, ss, 2)
	ct.reset()
	keyX, path := ss.objectKey("xorbs", x.String()), ss.xorbCachePath(x)

	a := mustOpenXorb(t, ss, x)
	defer a.Close()
	data, err := io.ReadAll(a)
	assertDrained(t, "reader A of X", drained{data, err}, encodedX)
	assertDrained(t, "read of Y evicting X", drainXorb(ss, y), encodedY)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("evicted X's cache file while reader A holds X: %v", err)
	}
	assertDrained(t, "refill read of X", drainXorb(ss, x), encodedX)
	if gets := ct.count(http.MethodGet, keyX); gets != 2 {
		t.Fatalf("GET requests for X after reading X, Y, X through a 1-entry cache = %d, want 2", gets)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("X's cache file after the refill while reader A holds the old mapping: %v", err)
	}

	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("X's cache file after reader A released the old mapping while the refilled one is cached: %v, want it kept", err)
	}
	assertDrained(t, "read of the refilled X after reader A closed", drainXorb(ss, x), encodedX)
	if gets := ct.count(http.MethodGet, keyX); gets != 2 {
		t.Fatalf("GET requests for X after reader A closed and X was read again = %d, want still 2", gets)
	}

	assertDrained(t, "second read of Y evicting the refilled X", drainXorb(ss, y), encodedY)
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("X's cache file after the refilled X was evicted with no reader: stat error = %v, want fs.ErrNotExist", err)
	}
}

func TestS3XorbFailedDownloadLeavesNoFile(t *testing.T) {
	for _, tc := range []struct {
		name   string
		inject func(*countingTransport, func(*http.Request) bool)
	}{
		{"request fails", (*countingTransport).setFail},
		{"body truncated", (*countingTransport).setTruncate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ss, ct := newCountingS3Storage(t)
			encoded, x := putRandomXorb(t, ss, 1)
			key := ss.objectKey("xorbs", x.String())
			tc.inject(ct, func(req *http.Request) bool {
				return req.Method == http.MethodGet && strings.Contains(req.URL.Path, key)
			})

			if rc, err := ss.GetXorbReadSeekCloser(context.Background(), x); err == nil {
				rc.Close()
				t.Fatal("GetXorbReadSeekCloser succeeded while the download failed")
			} else if errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("GetXorbReadSeekCloser with the download failing = %v, want an error other than fs.ErrNotExist", err)
			}
			if files, cached := cachedXorbFiles(t, ss), ss.xorbCache.Len(); files != 0 || cached != 0 {
				t.Fatalf("after a failed download: %d cache files, xorbCache.Len() = %d; want 0, 0", files, cached)
			}

			ct.reset()
			assertDrained(t, "read after the fault cleared", drainXorb(ss, x), encoded)
		})
	}
}

func TestS3XorbMissingIsNotExistAtOpen(t *testing.T) {
	ctx := context.Background()
	ss, ct := newCountingS3Storage(t)
	x := xet.XorbHash{9}
	key := ss.objectKey("xorbs", x.String())
	if rc, err := ss.GetXorbReadSeekCloser(ctx, x); !errors.Is(err, fs.ErrNotExist) {
		if err == nil {
			rc.Close()
		}
		t.Fatalf("GetXorbReadSeekCloser(missing xorb) error = %v, want fs.ErrNotExist", err)
	}
	if gets := ct.count(http.MethodGet, key); gets != 1 {
		t.Fatalf("GetXorbReadSeekCloser(missing xorb) sent %d GET requests for %s, want 1", gets, key)
	}
	if _, err := ss.GetXorbChunkOffsets(ctx, x); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("GetXorbChunkOffsets(missing xorb) error = %v, want fs.ErrNotExist", err)
	}
	if gets := ct.count(http.MethodGet, key); gets != 2 {
		t.Fatalf("GET requests for %s after GetXorbReadSeekCloser and GetXorbChunkOffsets of a missing xorb = %d, want 2 (a failed fill is not cached)", key, gets)
	}
	if heads := ct.count(http.MethodHead, key); heads != 0 {
		t.Fatalf("opening a missing xorb sent %d HEAD requests, want 0", heads)
	}
	if files := cachedXorbFiles(t, ss); files != 0 {
		t.Fatalf("a missing xorb left %d cache files, want 0", files)
	}
}

func TestS3XorbCachedSurvivesVanishedObject(t *testing.T) {
	ctx := context.Background()
	ss, ct := newCountingS3Storage(t)
	encoded, x := putRandomXorb(t, ss, 1)
	key := ss.objectKey("xorbs", x.String())
	ct.reset()

	if ok, err := ss.HasXorb(ctx, x); err != nil || !ok {
		t.Fatalf("HasXorb of a stored, uncached xorb = %v, %v; want true, nil", ok, err)
	}
	if heads, gets := ct.count(http.MethodHead, key), ct.count(http.MethodGet, key); heads != 1 || gets != 0 {
		t.Fatalf("HasXorb of an uncached xorb sent %d HEAD and %d GET requests; want 1 and 0", heads, gets)
	}
	assertDrained(t, "first read", drainXorb(ss, x), encoded)
	if err := ss.deleteObject(ctx, key); err != nil {
		t.Fatal(err)
	}
	ct.reset()

	assertDrained(t, "read after the object vanished", drainXorb(ss, x), encoded)
	if ok, err := ss.HasXorb(ctx, x); err != nil || !ok {
		t.Fatalf("HasXorb of a cached xorb whose object vanished = %v, %v; want true, nil", ok, err)
	}
	if gets, heads := ct.count(http.MethodGet, key), ct.count(http.MethodHead, key); gets != 0 || heads != 0 {
		t.Fatalf("re-reading and HasXorb of a cached xorb sent %d GET and %d HEAD requests; want 0 and 0", gets, heads)
	}
}

func TestS3XorbConcurrentFirstReadersShareOneGet(t *testing.T) {
	ss, ct := newCountingS3Storage(t)
	encoded, x := putRandomXorb(t, ss, 1)
	ct.reset()

	results := make([]drained, 8)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range results {
		wg.Go(func() {
			<-start
			results[i] = drainXorb(ss, x)
		})
	}
	close(start)
	wg.Wait()
	for i, got := range results {
		assertDrained(t, fmt.Sprintf("reader %d", i), got, encoded)
	}
	key := ss.objectKey("xorbs", x.String())
	if gets := ct.count(http.MethodGet, key); gets != 1 {
		t.Fatalf("%d concurrent first readers sent %d GET requests for %s, want 1", len(results), gets, key)
	}
}

// residentBytes sums the Rss that /proc/self/smaps reports for this process's mappings of path.
func residentBytes(t *testing.T, path string) int64 {
	t.Helper()
	path, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	smaps, err := os.ReadFile("/proc/self/smaps")
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	inPath := false
	for line := range strings.Lines(string(smaps)) {
		fields := strings.Fields(line)
		switch {
		case len(fields) >= 5 && strings.Contains(fields[0], "-"):
			// A mapping's header: addresses, perms, offset, device, inode, then the path of a file mapping.
			inPath = len(fields) == 6 && fields[5] == path
		case inPath && len(fields) == 3 && fields[0] == "Rss:":
			kb, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			total += kb << 10
		}
	}
	return total
}

// PutFile's shard is verified through the cached copy's file: the xorb downloads
// once for its CAS block, its term and its chunk offsets alike and stays cached,
// but no page of it is resident in the process until a read through the mapping.
func TestS3PutFileLeavesCopyCachedNotResident(t *testing.T) {
	ctx := context.Background()
	ss, ct := newCountingS3Storage(t)
	data := make([]byte, 1<<20)
	rand.NewChaCha8([32]byte{1}).Read(data)
	fileHash, err := storage.PutFile(ctx, ss, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	sh, err := ss.GetShard(ctx, fileHash)
	if err != nil {
		t.Fatal(err)
	}
	x := sh.CASInfos[0].CASHash
	key := ss.objectKey("xorbs", x.String())
	if gets, cached := ct.count(http.MethodGet, key), ss.xorbCache.Has(x); gets != 1 || !cached {
		t.Fatalf("after PutFile: %d GET requests for its xorb, cached %v; want 1, true", gets, cached)
	}
	onLinux := runtime.GOOS == "linux"
	if onLinux {
		if rss := residentBytes(t, ss.xorbCachePath(x)); rss != 0 {
			t.Fatalf("after PutFile: %d bytes of its xorb's copy resident, want 0", rss)
		}
	}

	if got := drainXorb(ss, x); got.err != nil {
		t.Fatal(got.err)
	}
	if gets := ct.count(http.MethodGet, key); gets != 1 {
		t.Fatalf("GET requests for the xorb after PutFile and a read = %d, want still 1", gets)
	}
	// The 0 above means something only if a read through the mapping shows up here.
	if onLinux && residentBytes(t, ss.xorbCachePath(x)) == 0 {
		t.Fatal("after a read through the mapping: no byte of the copy resident, want some")
	}
}

func TestS3XorbZeroByteObject(t *testing.T) {
	ctx := context.Background()
	ss := newTestS3Storage(t)
	x := xet.XorbHash{0xe}
	if err := ss.putObject(ctx, ss.objectKey("xorbs", x.String()), bytes.NewReader(nil)); err != nil {
		t.Fatal(err)
	}

	rc := mustOpenXorb(t, ss, x)
	defer rc.Close()
	if cached, mapped := ss.xorbCache.Len(), ss.xorbCache.Mapped(); cached != 1 || mapped != 0 {
		t.Fatalf("cached 0-byte xorb: xorbCache.Len() = %d, Mapped() = %d; want 1, 0", cached, mapped)
	}
	if n, err := rc.Read(make([]byte, 16)); n != 0 || err != io.EOF {
		t.Fatalf("Read of a 0-byte xorb = %d, %v; want 0, io.EOF", n, err)
	}
	if end, err := rc.Seek(0, io.SeekEnd); end != 0 || err != nil {
		t.Fatalf("Seek(0, io.SeekEnd) of a 0-byte xorb = %d, %v; want 0, nil", end, err)
	}

	if offsets, err := ss.GetXorbChunkOffsets(ctx, x); err != nil || len(offsets) != 0 {
		t.Fatalf("GetXorbChunkOffsets of a 0-byte xorb = %v, %v; want no offsets, nil", offsets, err)
	}
}

func TestS3NewStorageWipesOnlyXorbSubtree(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "xorbs", "stale")
	keep := filepath.Join(dir, "keep")
	if err := os.MkdirAll(filepath.Dir(stale), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{stale, keep} {
		if err := os.WriteFile(path, []byte("left by an earlier run"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	newTestS3Storage(t, WithCacheDir(dir))
	if _, err := os.Stat(stale); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stale file under the xorbs subtree after NewStorage: stat error = %v, want fs.ErrNotExist", err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("file beside the xorbs subtree after NewStorage: %v, want it kept", err)
	}
}

func TestS3CacheDirDefault(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	client := awss3.New(awss3.Options{Region: "us-east-1"})
	ss, err := NewStorage(context.Background(), WithS3Client(client), WithBucket("test-bucket"))
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(os.TempDir(), "xet-s3-xorbs"); ss.cacheDir != want {
		t.Fatalf("cacheDir without WithCacheDir = %q, want %q", ss.cacheDir, want)
	}
}

func TestS3CacheDirUnwritableFailsNewStorage(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(file, "sub")
	client := awss3.New(awss3.Options{Region: "us-east-1"})
	ss, err := NewStorage(context.Background(), WithCacheDir(dir), WithS3Client(client), WithBucket("test-bucket"))
	if err == nil || ss != nil {
		t.Fatalf("NewStorage(WithCacheDir(%q)) = %p, %v; want nil and an error", dir, ss, err)
	}
}
