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
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage/local"
)

// A follower fails with its leader's error; the next task under either key
// starts over from offset 0.
func TestIngestFollowerSharesFailure(t *testing.T) {
	data := make([]byte, 96*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	commit := strings.Repeat("ab", 20)
	up := &flakyUpstream{data: data, commit: commit, etag: hashHex(string(data)), failLeft: 1000, sendMax: 48 * 1024} // past the 32 KiB memory tier, so the shared partial reaches disk
	srv := httptest.NewServer(up)
	t.Cleanup(srv.Close)
	m, stor := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir())

	a, err := m.Resolve(context.Background(), "org/repo", "main", "a.bin")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		m.mu.Lock()
		leader := m.tasks[resolveKey{repo: "org/repo", rev: commit, path: "a.bin"}]
		m.mu.Unlock()
		if leader != nil && leader.item.Writer() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a.bin never led the download of its content")
		}
		time.Sleep(10 * time.Millisecond)
	}
	b, err := m.Resolve(context.Background(), "org/repo", "main", "b.bin")
	if err != nil {
		t.Fatal(err)
	}
	awaitClosed(t, doneOf(a), "leader ingest")
	awaitClosed(t, doneOf(b), "follower ingest")
	_, errA := entryOf(a)
	_, errB := entryOf(b)
	if errA == nil || errB == nil || errA.Error() != errB.Error() {
		t.Fatalf("leader err = %v, follower err = %v; want the follower to fail with the leader's error", errA, errB)
	}

	up.heal()
	for _, p := range []string{"a.bin", "b.bin"} {
		clearBackoff(m, "/org/repo/resolve/"+commit+"/"+p)
	}
	before := len(up.rangeOffsets())
	entry, err := ingestWait(t, m, "org/repo", "main", "b.bin")
	if err != nil {
		t.Fatal(err)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, data) {
		t.Fatalf("stored bytes mismatch after the refetch: got %d bytes, want %d", len(got), len(data))
	}
	offsets := up.rangeOffsets()
	if len(offsets) <= before {
		t.Fatal("the task under the other key issued no upstream fetch")
	}
	if first := offsets[before]; first != 0 {
		t.Fatalf("the task under the other key resumed at %d instead of starting over: offsets %v", first, offsets)
	}
}

// A key whose probe reports a different size for the same content hash does
// not join the download: its Resolve fails at once, in either direction, with
// no task registered, while the leader is unaffected.
func TestIngestFollowerRejectsSizeDisagreement(t *testing.T) {
	for _, delta := range []int{-1, 1} {
		t.Run(fmt.Sprintf("delta%+d", delta), func(t *testing.T) {
			upstream := newPlainUpstream()
			upstream.gate = make(chan struct{})
			upstream.gateHit = make(chan struct{})
			upstream.commit = strings.Repeat("ab", 20)
			data := make([]byte, 64*1024)
			if _, err := rand.Read(data); err != nil {
				t.Fatal(err)
			}
			upstream.set("/org/repo/resolve/main/a.bin", data)
			// b.bin advertises a.bin's content hash with a size off by delta.
			lying := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/b.bin") && !strings.HasPrefix(r.URL.Path, "/cdn") {
					etag := `"` + hashHex(string(data)) + `"`
					w.Header().Set("ETag", etag)
					w.Header().Set("X-Linked-Etag", etag)
					w.Header().Set("X-Linked-Size", fmt.Sprint(len(data)+delta))
					w.Header().Set("X-Repo-Commit", upstream.commit)
					http.Redirect(w, r, "/cdn"+strings.Replace(r.URL.Path, "/b.bin", "/a.bin", 1), http.StatusFound)
					return
				}
				upstream.ServeHTTP(w, r)
			})
			srv := httptest.NewServer(lying)
			t.Cleanup(srv.Close)
			release := sync.OnceFunc(func() { close(upstream.gate) })
			t.Cleanup(release)
			m, _ := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir())
			ctx := context.Background()

			a, err := m.Resolve(ctx, "org/repo", "main", "a.bin")
			if err != nil || a.Stream == nil {
				t.Fatalf("resolve a.bin = %+v, %v; want an in-flight stream", a, err)
			}
			awaitClosed(t, upstream.gateHit, "leader download")
			if _, err := m.Resolve(ctx, "org/repo", "main", "b.bin"); err == nil || !strings.Contains(err.Error(), "disagrees") {
				t.Fatalf("resolve b.bin err = %v; want the size disagreement", err)
			}
			m.mu.Lock()
			rejected, tasks := m.tasks[resolveKey{repo: "org/repo", rev: upstream.commit, path: "b.bin"}], len(m.tasks)
			m.mu.Unlock()
			if rejected != nil || tasks != 1 {
				t.Fatalf("b.bin task = %v among %d tasks; want none beside the leader", rejected, tasks)
			}
			if _, err := m.Resolve(ctx, "org/repo", "main", "b.bin"); err == nil || !strings.Contains(err.Error(), "disagrees") {
				t.Fatalf("b.bin after the rejection: err = %v; want the recorded size disagreement", err)
			}
			release()
			entry, err := ingestWait(t, m, "org/repo", "main", "a.bin")
			if err != nil || entry.Size != int64(len(data)) {
				t.Fatalf("leader after the follower's rejection = %+v, %v; want %d bytes", entry, err, len(data))
			}
			if got := upstream.dataGETs.Load(); got != 1 {
				t.Fatalf("upstream GETs = %d, want 1", got)
			}
		})
	}
}

// A leader whose probe carried no size learns it from its first response;
// a follower advertising another size is refused against that at Resolve,
// before it holds anything, while one advertising the true size shares the
// spool.
func TestIngestFollowerRejectsSizeLearnedFromLeader(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.noSize = true
	upstream.gate = make(chan struct{})
	upstream.gateHit = make(chan struct{})
	upstream.commit = strings.Repeat("ab", 20)
	data := make([]byte, 64*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"a.bin", "b.bin", "c.bin"} {
		upstream.set("/org/repo/resolve/main/"+p, data) // one content hash under three keys
	}
	// The followers' probes carry a size; only b.bin's is wrong.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/cdn") {
			switch {
			case strings.HasSuffix(r.URL.Path, "/b.bin"):
				w.Header().Set("X-Linked-Size", fmt.Sprint(len(data)+1))
			case strings.HasSuffix(r.URL.Path, "/c.bin"):
				w.Header().Set("X-Linked-Size", fmt.Sprint(len(data)))
			}
		}
		upstream.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	release := sync.OnceFunc(func() { close(upstream.gate) })
	t.Cleanup(release)
	m, _ := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir())
	ctx := context.Background()

	a, err := m.Resolve(ctx, "org/repo", "main", "a.bin")
	if err != nil || a.Stream == nil {
		t.Fatalf("resolve a.bin = %+v, %v; want an in-flight stream", a, err)
	}
	awaitClosed(t, upstream.gateHit, "leader download")
	if _, err := m.Resolve(ctx, "org/repo", "main", "b.bin"); err == nil || !strings.Contains(err.Error(), "disagrees with the shared download's") {
		t.Fatalf("lying follower err = %v; want the size disagreement", err)
	}

	c, err := m.Resolve(ctx, "org/repo", "main", "c.bin")
	if err != nil || c.Stream == nil {
		t.Fatalf("resolve c.bin = %+v, %v; want an in-flight stream", c, err)
	}
	if size, ok := c.Stream.WaitSize(ctx); !ok || size != int64(len(data)) {
		t.Fatalf("honest follower WaitSize = %d, %v; want %d, true", size, ok, len(data))
	}
	rc, err := c.Stream.NewReader(0)
	if err != nil {
		t.Fatalf("honest follower NewReader: %v", err)
	}
	defer rc.Close()
	half := make([]byte, len(data)/2)
	if _, err := io.ReadFull(rc, half); err != nil || !bytes.Equal(half, data[:len(half)]) {
		t.Fatalf("honest follower's first half while gated: %v", err)
	}
	release()
	if tail, err := io.ReadAll(rc); err != nil || !bytes.Equal(tail, data[len(half):]) {
		t.Fatalf("honest follower's second half: %v", err)
	}
	awaitClosed(t, c.Stream.t.done, "honest follower")
	leader, err := m.Resolve(ctx, "org/repo", "main", "a.bin")
	if err != nil || leader.Entry == nil {
		t.Fatalf("a.bin after the ingest = %+v, %v; want the ready entry", leader, err)
	}
	res, err := m.Resolve(ctx, "org/repo", "main", "c.bin")
	if err != nil || res.Entry == nil || res.Entry.FileHash != leader.Entry.FileHash || res.Entry.Size != int64(len(data)) {
		t.Fatalf("c.bin after the ingest = %+v, %v; want the leader's entry %+v", res, err, leader.Entry)
	}
	if got := upstream.dataGETs.Load(); got != 1 {
		t.Fatalf("upstream GETs = %d, want 1", got)
	}
}

// blobAdvert is what blobServer advertises for a file instead of the sha256 of its bytes.
type blobAdvert struct {
	blob string // a git-blob sha1: a content id storage cannot be asked about
	size int64
}

// blobServer serves upstream, advertising the files in adverts (by base name)
// with a git-blob etag and the given size: such a content id names no digest
// held content could be published from, so the follow decision alone decides
// what a task holds.
func blobServer(t *testing.T, upstream *plainUpstream, adverts map[string]blobAdvert) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ad, ok := adverts[path.Base(r.URL.Path)]; ok && !strings.HasPrefix(r.URL.Path, "/cdn") {
			if _, ok := upstream.get(r.URL.Path); ok {
				etag := `"` + ad.blob + `"`
				w.Header().Set("ETag", etag)
				w.Header().Set("X-Linked-Etag", etag)
				w.Header().Set("X-Linked-Size", fmt.Sprint(ad.size))
				w.Header().Set("X-Repo-Commit", upstream.commit)
				http.Redirect(w, r, "/cdn"+r.URL.Path, http.StatusFound)
				return
			}
		}
		upstream.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// flakyUpstream serves one partial body (sendMax bytes, then a dropped
// connection) and answers every following data request with 500 while
// failLeft > 0, so the whole ingest task fails with partial progress. It
// records the Range offsets of incoming data requests.
type flakyUpstream struct {
	mu          sync.Mutex
	data        []byte
	commit      string
	etag        string
	failLeft    int   // remaining forced failures
	sendMax     int   // bytes to send on the first failing request
	partialSent bool  // the one partial body has been served
	offsets     []int // Range start of each data GET
	hangOnce    bool
	canceled    int
	abort       chan struct{}
}

func (u *flakyUpstream) heal() {
	u.mu.Lock()
	u.failLeft = 0
	u.mu.Unlock()
}

func (u *flakyUpstream) rangeOffsets() []int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]int(nil), u.offsets...)
}

func (u *flakyUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/cdn") {
		u.mu.Lock()
		data := u.data
		offset := 0
		if rg := r.Header.Get("Range"); strings.HasPrefix(rg, "bytes=") {
			fmt.Sscanf(rg, "bytes=%d-", &offset)
		}
		mode := "serve"
		if r.Method == http.MethodGet {
			u.offsets = append(u.offsets, offset)
			if u.hangOnce && !u.partialSent {
				u.partialSent = true
				mode = "hang"
			} else if u.failLeft > 0 {
				u.failLeft--
				if u.partialSent {
					mode = "error"
				} else {
					u.partialSent = true
					mode = "partial"
				}
			}
		}
		sendMax := u.sendMax
		u.mu.Unlock()

		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			return
		}
		if mode == "error" {
			http.Error(w, "upstream flake", http.StatusInternalServerError)
			return
		}
		if offset > 0 {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, len(data)-1, len(data)))
			w.Header().Set("Content-Length", fmt.Sprint(len(data)-offset))
			w.WriteHeader(http.StatusPartialContent)
		} else {
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			w.WriteHeader(http.StatusOK)
		}
		body := data[offset:]
		if mode == "hang" {
			_, _ = w.Write(body[:sendMax])
			w.(http.Flusher).Flush()
			select {
			case <-r.Context().Done():
				u.mu.Lock()
				u.canceled++
				u.mu.Unlock()
			case <-u.abort:
			}
			return
		}
		if mode == "partial" && len(body) > sendMax {
			_, _ = w.Write(body[:sendMax])
			w.(http.Flusher).Flush()
			// Drop the connection mid-body.
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					conn.Close()
					return
				}
			}
			panic(http.ErrAbortHandler)
		}
		_, _ = w.Write(body)
		return
	}

	u.mu.Lock()
	etag, commit, size := u.etag, u.commit, len(u.data)
	u.mu.Unlock()
	w.Header().Set("ETag", `"`+etag+`"`)
	w.Header().Set("X-Linked-Etag", `"`+etag+`"`)
	w.Header().Set("X-Linked-Size", fmt.Sprint(size))
	w.Header().Set("X-Repo-Commit", commit)
	http.Redirect(w, r, "/cdn"+r.URL.Path, http.StatusFound)
}

// clearBackoff lets the next ingest start a fresh task immediately.
func clearBackoff(m *testMirror, key string) {
	k, _ := parseResolveKey(key)
	m.mu.Lock()
	if e := m.entries[k]; e != nil && e.State == stateFailed {
		e.nextRetry = time.Time{}
	}
	m.mu.Unlock()
}

// Retries within a task resume from the spooled prefix; after the task fails,
// the next task fetches from zero.
func TestMirrorFailedTaskDropsPartial(t *testing.T) {
	data := make([]byte, 96*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	up := &flakyUpstream{data: data, commit: "commit-1", etag: "etag-1", failLeft: 1000, sendMax: 48 * 1024} // past the 32 KiB memory tier, so the partial reaches disk
	upstreamSrv := httptest.NewServer(up)
	defer upstreamSrv.Close()

	const resolvePath = "/org/repo/resolve/main/flaky.bin"
	m, stor := newTestMirror(t, upstreamSrv.URL, t.TempDir(), t.TempDir())

	// First ingest: the upstream serves one partial body then fails hard, so
	// the task fails with partial progress, and each retry must have resumed
	// from the previous offset.
	if _, err := ingestWait(t, m, "org/repo", "main", "flaky.bin"); err == nil {
		t.Fatal("ingest against a failing upstream unexpectedly succeeded")
	}

	offsets := up.rangeOffsets()
	if len(offsets) < 2 {
		t.Fatalf("expected multiple fetch attempts, got offsets %v", offsets)
	}
	for _, off := range offsets[1:] {
		if off == 0 {
			t.Fatalf("a retry restarted from 0 instead of resuming: offsets %v", offsets)
		}
	}

	// Second ingest (upstream healed, backoff cleared): the new task has no
	// partial left to resume and fetches from zero.
	up.heal()
	clearBackoff(m, resolvePath)
	before := len(offsets)
	entry, err := ingestWait(t, m, "org/repo", "main", "flaky.bin")
	if err != nil {
		t.Fatal(err)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, data) {
		t.Fatalf("stored bytes mismatch after the refetch: got %d bytes, want %d", len(got), len(data))
	}
	offsets = up.rangeOffsets()
	if len(offsets) <= before {
		t.Fatal("second task issued no upstream fetch")
	}
	if first := offsets[before]; first != 0 {
		t.Fatalf("second task resumed at %d although the failed task's partial was dropped: offsets %v", first, offsets)
	}
}

// A process that dies mid-transfer leaves a partial spool nobody finished or
// swept; the restarted mirror must resume from exactly those bytes.
func TestMirrorResumeAfterCrashMidTransfer(t *testing.T) {
	data := make([]byte, 96*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	up := &flakyUpstream{data: data, commit: "commit-1", etag: "etag-1", hangOnce: true, sendMax: 48 * 1024, abort: make(chan struct{})}
	srv := httptest.NewServer(up)
	t.Cleanup(srv.Close)

	storageDir, cacheDir := t.TempDir(), t.TempDir()
	m, _ := newTestMirror(t, srv.URL, storageDir, cacheDir)
	t.Cleanup(sync.OnceFunc(func() { close(up.abort) })) // frees the hung handler before the fixture waits for m's task
	ctx := context.Background()
	res, err := m.Resolve(ctx, "org/repo", "main", "crash.bin")
	if err != nil || res.Stream == nil {
		t.Fatalf("Resolve = %+v, %v; want an in-flight stream", res, err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for res.Stream.t.item.Written() < int64(up.sendMax) {
		if time.Now().After(deadline) {
			t.Fatal("the hung upstream delivered no prefix")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The crash: the first engine is abandoned with its download parked on the hung body.
	before := len(up.rangeOffsets())

	m2, stor2 := newTestMirror(t, srv.URL, storageDir, cacheDir)
	entry, err := ingestWait(t, m2, "org/repo", "main", "crash.bin")
	if err != nil {
		t.Fatal(err)
	}
	if got := readStored(t, stor2, entry.SHA256); !bytes.Equal(got, data) {
		t.Fatalf("stored bytes mismatch after the crash: got %d bytes, want %d", len(got), len(data))
	}
	offsets := up.rangeOffsets()
	if len(offsets) != before+1 || offsets[before] != up.sendMax {
		t.Fatalf("data GET offsets after the restart = %v, want one fetch resuming at %d", offsets[before:], up.sendMax)
	}
}

// A changed upstream etag must invalidate the partial spool instead of
// resuming into mismatched content.
func TestMirrorStalePartialDiscarded(t *testing.T) {
	data := make([]byte, 64*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	up := &flakyUpstream{data: data, commit: "commit-1", etag: "etag-1", failLeft: 1000, sendMax: 16 * 1024}
	upstreamSrv := httptest.NewServer(up)
	defer upstreamSrv.Close()

	const resolvePath = "/org/repo/resolve/main/stale.bin"
	m, stor := newTestMirror(t, upstreamSrv.URL, t.TempDir(), t.TempDir())

	if _, err := ingestWait(t, m, "org/repo", "main", "stale.bin"); err == nil {
		t.Fatal("ingest against a failing upstream unexpectedly succeeded")
	}
	before := len(up.rangeOffsets())

	// Upstream content changed: new etag, new bytes.
	data2 := make([]byte, 64*1024)
	if _, err := rand.Read(data2); err != nil {
		t.Fatal(err)
	}
	up.mu.Lock()
	up.data = data2
	up.etag = "etag-2"
	up.commit = "commit-2"
	up.failLeft = 0
	up.mu.Unlock()

	clearBackoff(m, resolvePath)
	entry, err := ingestWait(t, m, "org/repo", "main", "stale.bin")
	if err != nil {
		t.Fatal(err)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, data2) {
		t.Fatal("stored bytes mismatch after etag change")
	}
	offsets := up.rangeOffsets()
	if len(offsets) <= before {
		t.Fatal("no upstream fetch after etag change")
	}
	if first := offsets[before]; first != 0 {
		t.Fatalf("stale partial was resumed (offset %d) instead of discarded", first)
	}
}

func TestMirrorStalledPlainFetchResumes(t *testing.T) {
	data := make([]byte, 96*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	up := &flakyUpstream{data: data, commit: "commit-1", etag: "etag-1", hangOnce: true, sendMax: 32 * 1024, abort: make(chan struct{})}
	srv := httptest.NewServer(up)
	t.Cleanup(srv.Close)

	m, stor := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir())
	t.Cleanup(func() { close(up.abort) })
	// Shorten the upstream read-idle guard below the shared auth injector.
	m.probeClient.Transport.(*authInjector).inner = client.NewIdleTimeoutTransport(http.DefaultTransport.(*http.Transport).Clone(), 200*time.Millisecond)

	res, err := m.Resolve(context.Background(), "org/repo", "main", "stall.bin")
	if err != nil {
		t.Fatal(err)
	}
	awaitClosed(t, doneOf(res), "stalled ingest")
	entry, err := entryOf(res)
	if err != nil {
		t.Fatal(err)
	}
	up.mu.Lock()
	canceled := up.canceled
	up.mu.Unlock()
	if canceled != 1 {
		t.Fatalf("upstream saw %d canceled requests, want the stalled one", canceled)
	}
	if offsets := up.rangeOffsets(); !slices.Equal(offsets, []int{0, 32 * 1024}) {
		t.Fatalf("data GET offsets = %v, want [0 32768]: resume from the exact prefix", offsets)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, data) {
		t.Fatal("stored bytes mismatch after stall resume")
	}
}

// The second xorb is paced or stalled before its first chunk completes.
type xetStallUpstream struct {
	hubURL, casURL string
	fileHash       string
	sha256         string
	size           int
	xorbB          string // hash of the stalled xorb
	slowFeed       bool
	stalls         atomic.Int32
	canceled       atomic.Int32
	xorbGETs       atomic.Int32
	mu             sync.Mutex
	reconRanges    []string // Range header of each reconstruction request
	xorbBRanges    []string // Range header of each GET for the stalled xorb
	abort          chan struct{}
}

type stallWriter struct {
	http.ResponseWriter
	r *http.Request
	u *xetStallUpstream
}

func (w *stallWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p[:min(len(p), 4096)])
	w.ResponseWriter.(http.Flusher).Flush()
	select {
	case <-w.r.Context().Done():
		w.u.canceled.Add(1)
	case <-w.u.abort:
	}
	return n, errors.Join(err, errors.New("stalled"))
}

// Partial chunks must count as progress before they can reach the spool.
type slowWriter struct {
	http.ResponseWriter
	r             *http.Request
	u             *xetStallUpstream
	sent, slowEnd int
}

func (w *slowWriter) Write(p []byte) (int, error) {
	if w.slowEnd == 0 {
		w.slowEnd = 8 + (int(p[1]) | int(p[2])<<8 | int(p[3])<<16) - 1 // chunk header + compressed size
	}
	written := 0
	for len(p) > 0 {
		n, paced := len(p), w.sent < w.slowEnd
		if paced {
			n = min(n, (w.slowEnd+29)/30, w.slowEnd-w.sent)
		}
		k, err := w.ResponseWriter.Write(p[:n])
		w.sent, written, p = w.sent+k, written+k, p[n:]
		if err != nil {
			return written, err
		}
		if !paced {
			continue
		}
		w.ResponseWriter.(http.Flusher).Flush()
		select {
		case <-time.After(20 * time.Millisecond):
		case <-w.r.Context().Done():
			w.u.canceled.Add(1)
			return written, w.r.Context().Err()
		case <-w.u.abort:
			return written, errors.New("aborted")
		}
	}
	return written, nil
}

func newXetStallUpstream(t *testing.T, resolvePath string, head, tail []byte) *xetStallUpstream {
	t.Helper()
	u := &xetStallUpstream{abort: make(chan struct{})}
	data := append(append([]byte(nil), head...), tail...)

	var cas http.Handler
	casSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/reconstructions/"):
			u.mu.Lock()
			u.reconRanges = append(u.reconRanges, r.Header.Get("Range"))
			u.mu.Unlock()
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/xorbs/"):
			u.xorbGETs.Add(1)
			if strings.Contains(r.URL.Path, u.xorbB) {
				u.mu.Lock()
				u.xorbBRanges = append(u.xorbBRanges, r.Header.Get("Range"))
				u.mu.Unlock()
				if u.slowFeed {
					w = &slowWriter{ResponseWriter: w, r: r, u: u}
				} else if u.stalls.Add(1) == 1 {
					w = &stallWriter{ResponseWriter: w, r: r, u: u}
				}
			}
		}
		cas.ServeHTTP(w, r)
	}))
	t.Cleanup(casSrv.Close)
	stor, err := local.NewStorage(local.WithBasePath(t.TempDir()), local.WithBaseURL(casSrv.URL))
	if err != nil {
		t.Fatal(err)
	}
	cas = server.NewHandler(server.WithStorage(stor))
	ctx := context.Background()
	// Seeding the prefix makes the combined upload span two xorbs.
	seedStorage(t, stor, head)
	fileHash := seedStorage(t, stor, data)
	sh, err := stor.GetShard(ctx, fileHash)
	if err != nil {
		t.Fatal(err)
	}
	entries := sh.Files[0].Entries
	if len(entries) != 2 || entries[0].CASHash == entries[1].CASHash {
		t.Fatalf("fixture file spans %d terms, want two xorbs", len(entries))
	}
	u.xorbB = entries[1].CASHash.String()
	sum := sha256.Sum256(data)
	u.fileHash, u.sha256, u.size = fileHash.String(), hex.EncodeToString(sum[:]), len(data)

	hubSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/xet-read-token/") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"casUrl": u.casURL, "accessToken": "upstream-cas-token", "exp": time.Now().Add(time.Hour).Unix()})
			return
		}
		if r.URL.Path != resolvePath {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("ETag", `"`+u.sha256+`"`)
		w.Header().Set("X-Linked-Size", fmt.Sprint(u.size))
		w.Header().Set("X-Repo-Commit", "xet-commit-1")
		w.Header().Set("X-Xet-Hash", u.fileHash)
		w.Header().Add("Link", fmt.Sprintf("<%s/v1/reconstructions/%s>; rel=\"xet-reconstruction-info\"", u.casURL, u.fileHash))
		w.Header().Add("Link", fmt.Sprintf("<%s/api/xet-read-token>; rel=\"xet-auth\"", u.hubURL))
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(hubSrv.Close)
	u.casURL, u.hubURL = casSrv.URL, hubSrv.URL
	return u
}

// A stalled term body is resumed by the client at the compressed offset: no new attempt, no second reconstruction query.
func TestMirrorStalledXetFetchResumes(t *testing.T) {
	head, tail := make([]byte, 256*1024), make([]byte, 256*1024)
	if _, err := rand.Read(head); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(tail); err != nil {
		t.Fatal(err)
	}
	const resolvePath = "/org/repo/resolve/main/stall.bin"
	up := newXetStallUpstream(t, resolvePath, head, tail)
	m, stor := newTestMirror(t, up.hubURL, t.TempDir(), t.TempDir(), WithClientOptions(client.WithIdleTimeout(200*time.Millisecond)))
	t.Cleanup(func() { close(up.abort) }) // runs first: unblocks a still-stalled handler before servers close

	res, err := m.Resolve(context.Background(), "org/repo", "main", "stall.bin")
	if err != nil {
		t.Fatal(err)
	}
	awaitClosed(t, doneOf(res), "stalled xet ingest")
	entry, err := entryOf(res)
	if err != nil {
		t.Fatal(err)
	}
	if got := up.canceled.Load(); got != 1 {
		t.Fatalf("upstream CAS saw %d canceled xorb requests, want the stalled one", got)
	}
	if got := up.xorbGETs.Load(); got != 3 {
		t.Fatalf("upstream xorb GETs = %d, want 3 (first xorb, stalled second, resumed second)", got)
	}
	up.mu.Lock()
	reconRanges := append([]string(nil), up.reconRanges...)
	xorbBRanges := append([]string(nil), up.xorbBRanges...)
	up.mu.Unlock()
	if !slices.Equal(reconRanges, []string{""}) {
		t.Fatalf("reconstruction Range headers = %q, want one unranged query", reconRanges)
	}
	var start, end int64
	if len(xorbBRanges) != 2 {
		t.Fatalf("stalled xorb Range headers = %q, want the stalled and the resumed request", xorbBRanges)
	}
	if _, err := fmt.Sscanf(xorbBRanges[0], "bytes=%d-%d", &start, &end); err != nil {
		t.Fatalf("stalled xorb Range %q: %v", xorbBRanges[0], err)
	}
	if want := fmt.Sprintf("bytes=%d-%d", start+4096, end); xorbBRanges[1] != want {
		t.Fatalf("resumed xorb Range = %q, want %q (the bytes the stalled response delivered)", xorbBRanges[1], want)
	}
	if entry.SHA256 != up.sha256 {
		t.Fatalf("entry sha256 = %s, want %s", entry.SHA256, up.sha256)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, append(head, tail...)) {
		t.Fatal("stored bytes mismatch after xet stall resume")
	}
}

// A term body that keeps moving without completing a chunk for longer than the idle timeout is live: one attempt, no cancel, exact bytes.
func TestMirrorSlowXetFetchNotStalled(t *testing.T) {
	head, tail := make([]byte, 256*1024), make([]byte, 256*1024)
	if _, err := rand.Read(head); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(tail); err != nil {
		t.Fatal(err)
	}
	const resolvePath = "/org/repo/resolve/main/slow.bin"
	up := newXetStallUpstream(t, resolvePath, head, tail)
	up.slowFeed = true
	own := client.NewCache(t.TempDir(), 0, 0)
	cacheDir := t.TempDir()
	m, stor := newTestMirror(t, up.hubURL, t.TempDir(), cacheDir, WithClientOptions(client.WithIdleTimeout(200*time.Millisecond), client.WithCache(own)))
	t.Cleanup(func() { close(up.abort) })

	res, err := m.Resolve(context.Background(), "org/repo", "main", "slow.bin")
	if err != nil {
		t.Fatal(err)
	}
	awaitClosed(t, doneOf(res), "slow xet ingest")
	entry, err := entryOf(res)
	if err != nil {
		t.Fatalf("continuously progressing transfer failed: %v", err)
	}
	if got := up.canceled.Load(); got != 0 {
		t.Fatalf("upstream CAS saw %d canceled xorb requests, want 0", got)
	}
	if got := up.xorbGETs.Load(); got != 2 {
		t.Fatalf("upstream xorb GETs = %d, want 2 (one per xorb, single attempt)", got)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, append(head, tail...)) {
		t.Fatal("stored bytes mismatch")
	}
	usage, err := own.Usage(context.Background())
	if err != nil || usage.Download.Count == 0 {
		t.Fatalf("supplied chunk cache: %+v, %v; want entries", usage, err)
	}
	if _, err := os.Stat(filepath.Join(cacheDir, "download")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("mirror used its own chunk cache despite WithCache: %v", err)
	}
}
