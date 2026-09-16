package mirror

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/upload"
)

// testIdleTimeout keeps stall detection within a test's patience.
const testIdleTimeout = 300 * time.Millisecond

// ingestGuard bounds a wait that a silent upstream used to hold forever.
const ingestGuard = 15 * time.Second

// stallingUpstream answers data GETs per plan entry (prefix, silent, headless, trickle), then normally.
type stallingUpstream struct {
	mu      sync.Mutex
	data    []byte
	plan    []string
	sendMax int
	piece   int
	gap     time.Duration
	offsets []int // Range start of each data GET
	release chan struct{}
}

func newStallingUpstream(data []byte, plan ...string) *stallingUpstream {
	return &stallingUpstream{data: data, plan: plan, sendMax: len(data) / 3, release: make(chan struct{})}
}

func (u *stallingUpstream) rangeOffsets() []int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return slices.Clone(u.offsets)
}

// wait ends when the client is gone or release is closed, so a failed test can still shut down.
func (u *stallingUpstream) wait(r *http.Request) {
	select {
	case <-r.Context().Done():
	case <-u.release:
	}
}

func (u *stallingUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/cdn") {
		w.Header().Set("ETag", `"etag-1"`)
		w.Header().Set("X-Linked-Etag", `"etag-1"`)
		w.Header().Set("X-Linked-Size", fmt.Sprint(len(u.data)))
		w.Header().Set("X-Repo-Commit", "commit-1")
		http.Redirect(w, r, "/cdn"+r.URL.Path, http.StatusFound)
		return
	}
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", fmt.Sprint(len(u.data)))
		return
	}
	offset := 0
	if rg := r.Header.Get("Range"); strings.HasPrefix(rg, "bytes=") {
		fmt.Sscanf(rg, "bytes=%d-", &offset)
	}
	u.mu.Lock()
	mode := "serve"
	if n := len(u.offsets); n < len(u.plan) {
		mode = u.plan[n]
	}
	u.offsets = append(u.offsets, offset)
	u.mu.Unlock()

	if mode == "headless" {
		u.wait(r)
		return
	}
	body := u.data[offset:]
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	if offset > 0 {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, len(u.data)-1, len(u.data)))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	flusher := w.(http.Flusher)
	switch mode {
	case "silent":
		flusher.Flush()
		u.wait(r)
	case "prefix":
		_, _ = w.Write(body[:min(u.sendMax, len(body))])
		flusher.Flush()
		u.wait(r)
	case "trickle":
		for len(body) > 0 {
			n := min(u.piece, len(body))
			if _, err := w.Write(body[:n]); err != nil {
				return
			}
			flusher.Flush()
			body = body[n:]
			time.Sleep(u.gap)
		}
	default:
		_, _ = w.Write(body)
	}
}

func newStallTestMirror(t *testing.T, upstream string, opts ...Option) (*Mirror, storage.Storage) {
	t.Helper()
	m, stor := newTestMirror(t, upstream, t.TempDir(), t.TempDir(), opts...)
	m.idleTimeout = testIdleTimeout
	return m, stor
}

// waitIngest releases the silent handlers on timeout so the servers can close before failing.
func waitIngest(t *testing.T, in *Ingestion, release chan struct{}) (*Entry, error) {
	t.Helper()
	select {
	case <-in.Done():
	case <-time.After(ingestGuard):
		close(release)
		t.Fatalf("ingest still running after %s: the silent upstream held the fetch", ingestGuard)
	}
	return in.Entry()
}

func randomData(t *testing.T, n int) []byte {
	t.Helper()
	data := make([]byte, n)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	return data
}

// A silent body, then a headless retry: both are cut and the next attempt resumes from the spool.
func TestFetchStalledUpstreamResumesOnRetry(t *testing.T) {
	data := randomData(t, 96*1024)
	up := newStallingUpstream(data, "prefix", "headless")
	srv := httptest.NewServer(up)
	defer srv.Close()
	m, stor := newStallTestMirror(t, srv.URL)

	in, err := m.Ingest("org/repo", "main", "stall.bin")
	if err != nil {
		t.Fatal(err)
	}
	entry, err := waitIngest(t, in, up.release)
	if err != nil {
		t.Fatal(err)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, data) {
		t.Fatalf("stored bytes mismatch: got %d bytes, want %d", len(got), len(data))
	}
	if got, want := up.rangeOffsets(), []int{0, up.sendMax, up.sendMax}; !slices.Equal(got, want) {
		t.Fatalf("range offsets = %v, want %v", got, want)
	}
}

// A never-resuming upstream fails the task in bounded time and keeps the spool for the next task.
func TestFetchStalledUpstreamFailsTask(t *testing.T) {
	data := randomData(t, 96*1024)
	up := newStallingUpstream(data, "prefix", "silent", "silent", "silent", "silent")
	srv := httptest.NewServer(up)
	defer srv.Close()
	m, stor := newStallTestMirror(t, srv.URL)

	in, err := m.Ingest("org/repo", "main", "dead.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := waitIngest(t, in, up.release); !errors.Is(err, errFetchStalled) {
		t.Fatalf("ingest err = %v, want errFetchStalled", err)
	}
	p := up.sendMax
	if got, want := up.rangeOffsets(), []int{0, p, p, p, p}; !slices.Equal(got, want) {
		t.Fatalf("range offsets = %v, want %v", got, want)
	}
	m.mu.Lock()
	tasks := len(m.tasks)
	m.mu.Unlock()
	if tasks != 0 {
		t.Fatalf("%d tasks still registered after failure", tasks)
	}
	spools, _ := filepath.Glob(filepath.Join(m.spoolDir, "*.spool"))
	if len(spools) != 1 {
		t.Fatalf("spool files after failure = %v, want one", spools)
	}
	if st, err := os.Stat(spools[0]); err != nil || st.Size() != int64(p) {
		t.Fatalf("partial spool not retained: size %d, err %v, want %d bytes", st.Size(), err, p)
	}

	clearBackoff(m, "/org/repo/resolve/main/dead.bin")
	in, err = m.Ingest("org/repo", "main", "dead.bin")
	if err != nil {
		t.Fatal(err)
	}
	entry, err := waitIngest(t, in, up.release)
	if err != nil {
		t.Fatal(err)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, data) {
		t.Fatal("stored bytes mismatch after resume")
	}
	if offsets := up.rangeOffsets(); len(offsets) != 6 || offsets[5] != p {
		t.Fatalf("healed task did not resume from %d: offsets %v", p, offsets)
	}
}

// Slow but flowing bytes over several idle timeouts must complete in a single attempt.
func TestFetchSlowProgressCompletes(t *testing.T) {
	data := randomData(t, 64*1024)
	up := newStallingUpstream(data, "trickle")
	up.piece, up.gap = 4*1024, testIdleTimeout/4 // 16 pieces, 4 idle timeouts in total
	srv := httptest.NewServer(up)
	defer srv.Close()
	m, stor := newStallTestMirror(t, srv.URL)

	in, err := m.Ingest("org/repo", "main", "slow.bin")
	if err != nil {
		t.Fatal(err)
	}
	entry, err := waitIngest(t, in, up.release)
	if err != nil {
		t.Fatal(err)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, data) {
		t.Fatal("stored bytes mismatch")
	}
	if got := up.rangeOffsets(); !slices.Equal(got, []int{0}) {
		t.Fatalf("range offsets = %v, want a single attempt", got)
	}
}

// fetchAttempt cuts only silence and releases the attempt context however the fetch ends.
func TestFetchAttempt(t *testing.T) {
	const idle = 100 * time.Millisecond
	block := func(ctx context.Context, _ io.Writer) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return errors.New("attempt context never canceled")
		}
	}

	t.Run("silence is cut and reported", func(t *testing.T) {
		start := time.Now()
		err := fetchAttempt(context.Background(), idle, block)
		if !errors.Is(err, errFetchStalled) {
			t.Fatalf("err = %v, want errFetchStalled", err)
		}
		if d := time.Since(start); d < idle {
			t.Fatalf("cut after %s, before the idle timeout %s", d, idle)
		}
	})

	t.Run("progress re-arms the timer", func(t *testing.T) {
		err := fetchAttempt(context.Background(), idle, func(ctx context.Context, progress io.Writer) error {
			for range 12 { // three idle timeouts in total, gaps of a quarter each
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(idle / 4):
				}
				_, _ = progress.Write([]byte{0})
			}
			return nil
		})
		if err != nil {
			t.Fatalf("flowing attempt cut: %v", err)
		}
	})

	t.Run("parent cancel is not a stall", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		err := fetchAttempt(ctx, time.Hour, func(ctx context.Context, w io.Writer) error {
			cancel()
			return block(ctx, w)
		})
		if !errors.Is(err, context.Canceled) || errors.Is(err, errFetchStalled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	})

	t.Run("outcome passes through and the attempt context is released", func(t *testing.T) {
		for _, want := range []error{nil, errors.New("boom")} {
			var attempt context.Context
			err := fetchAttempt(context.Background(), time.Hour, func(ctx context.Context, _ io.Writer) error {
				attempt = ctx
				return want
			})
			if err != want {
				t.Fatalf("err = %v, want %v", err, want)
			}
			if attempt.Err() == nil {
				t.Fatal("attempt context still live after return")
			}
		}
	})
}

// xetStallUpstream is a xet hub over a real CAS whose first xorb GET goes silent after its headers.
type xetStallUpstream struct {
	casURL   string
	fileHash string
	sha      string
	size     int
	xorbGETs atomic.Int64
	release  chan struct{}
}

func newXetStallUpstream(t *testing.T, data []byte) (*xetStallUpstream, string) {
	t.Helper()
	u := &xetStallUpstream{release: make(chan struct{})}

	var cas atomic.Pointer[server.Handler]
	casSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/v1/xorbs/") || u.xorbGETs.Add(1) != 1 {
			cas.Load().ServeHTTP(w, r)
			return
		}
		rec := httptest.NewRecorder()
		cas.Load().ServeHTTP(rec, r)
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.Header().Set("Content-Length", fmt.Sprint(rec.Body.Len()))
		w.WriteHeader(rec.Code)
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-u.release:
		}
	}))
	t.Cleanup(casSrv.Close)

	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()), storage.WithBaseURL(casSrv.URL))
	if err != nil {
		t.Fatal(err)
	}
	cas.Store(server.NewHandler(server.WithStorage(stor)))
	fileHash, err := upload.UploadFile(context.Background(), &localCAS{storage: stor, namespace: "default"},
		bytes.NewReader(data), upload.WithEnableSHA256(true))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	u.casURL, u.fileHash, u.sha, u.size = casSrv.URL, fileHash.String(), hex.EncodeToString(sum[:]), len(data)

	hubSrv := httptest.NewServer(http.HandlerFunc(u.serveHub))
	t.Cleanup(hubSrv.Close)
	return u, hubSrv.URL
}

func (u *xetStallUpstream) serveHub(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/xet-read-token" {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"casUrl":      u.casURL,
			"accessToken": "upstream-cas-token",
			"exp":         time.Now().Add(time.Hour).Unix(),
		})
		return
	}
	w.Header().Set("ETag", `"`+u.sha+`"`)
	w.Header().Set("X-Linked-Etag", `"`+u.sha+`"`)
	w.Header().Set("X-Linked-Size", fmt.Sprint(u.size))
	w.Header().Set("X-Repo-Commit", "commit-1")
	w.Header().Add("Link", fmt.Sprintf("<%s/v1/reconstructions/%s>; rel=\"xet-reconstruction-info\"", u.casURL, u.fileHash))
	w.Header().Add("Link", fmt.Sprintf("<http://%s/api/xet-read-token>; rel=\"xet-auth\"", r.Host))
}

// The xet path runs under the same watchdog: a silent xorb GET is cut and the retry completes.
func TestFetchXetStalledUpstreamRetries(t *testing.T) {
	data := randomData(t, 256*1024)
	up, hubURL := newXetStallUpstream(t, data)
	xc, err := client.NewClient(client.WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	m, stor := newStallTestMirror(t, hubURL, WithClient(xc))

	in, err := m.Ingest("org/repo", "main", "weights.bin")
	if err != nil {
		t.Fatal(err)
	}
	entry, err := waitIngest(t, in, up.release)
	if err != nil {
		t.Fatal(err)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, data) {
		t.Fatalf("stored bytes mismatch: got %d bytes, want %d", len(got), len(data))
	}
	if n := up.xorbGETs.Load(); n < 2 {
		t.Fatalf("xorb GETs = %d, want the stalled attempt retried", n)
	}
}
