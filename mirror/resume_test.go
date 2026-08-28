package mirror

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// The spool file name must embed the content identity (etag + size), so a
// reopen with the same validators resumes from the file length and different
// validators land in a different, swept-clean file.
func TestSpoolNamedByValidatorsResume(t *testing.T) {
	dir := t.TempDir()
	const key = "/org/repo/resolve/main/a.bin"
	etag := strings.Repeat("ab", 32) // hex sha256-style etag

	sp, err := openSpool(dir, key, etag, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sp.Write(make([]byte, 40)); err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(sp.f.Name())
	if want := spoolKeyPrefix(key) + etag + "-100.spool"; name != want {
		t.Fatalf("spool name = %q, want %q", name, want)
	}
	sp.finish(fmt.Errorf("interrupted"))

	// Same validators: resume from the partial length.
	sp2, err := openSpool(dir, key, etag, 100)
	if err != nil {
		t.Fatal(err)
	}
	if got := sp2.size(); got != 40 {
		t.Fatalf("resumed size = %d, want 40", got)
	}
	sp2.finish(fmt.Errorf("interrupted again"))

	// Changed etag: new file from zero, stale sibling swept.
	etag2 := strings.Repeat("cd", 32)
	sp3, err := openSpool(dir, key, etag2, 100)
	if err != nil {
		t.Fatal(err)
	}
	if got := sp3.size(); got != 0 {
		t.Fatalf("size after etag change = %d, want 0", got)
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var spools []string
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".spool") {
			spools = append(spools, e.Name())
		}
	}
	if len(spools) != 1 || spools[0] != spoolKeyPrefix(key)+etag2+"-100.spool" {
		t.Fatalf("stale spool not swept: %v", spools)
	}
	sp3.finish(nil)

	// No etag: nothing trustworthy in the name, always start fresh.
	sp4, err := openSpool(dir, key, "", -1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sp4.Write(make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	sp4.finish(fmt.Errorf("interrupted"))
	sp5, err := openSpool(dir, key, "", -1)
	if err != nil {
		t.Fatal(err)
	}
	if got := sp5.size(); got != 0 {
		t.Fatalf("etag-less spool resumed (%d bytes), want truncation", got)
	}
	sp5.finish(nil)
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
			if u.failLeft > 0 {
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
func clearBackoff(m *Mirror, key string) {
	k, _ := parseResolveKey(key)
	m.mu.Lock()
	if e := m.entries[k]; e != nil && e.State == stateFailed {
		e.nextRetry = time.Time{}
	}
	m.mu.Unlock()
}

// A task that dies mid-download must leave its partial spool behind, and the
// next task must resume from that offset instead of refetching from zero.
func TestMirrorResumeAfterTaskFailure(t *testing.T) {
	data := make([]byte, 96*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	up := &flakyUpstream{data: data, commit: "commit-1", etag: "etag-1", failLeft: 1000, sendMax: 32 * 1024}
	upstreamSrv := httptest.NewServer(up)
	defer upstreamSrv.Close()

	const resolvePath = "/org/repo/resolve/main/flaky.bin"
	m, stor := newTestMirror(t, upstreamSrv.URL, t.TempDir(), t.TempDir())

	// First ingest: the upstream serves one partial body then fails hard, so
	// the task fails with partial progress, and each retry must have resumed
	// from the previous offset.
	in, err := m.Ingest("org/repo", "main", "flaky.bin")
	if err != nil {
		t.Fatal(err)
	}
	<-in.Done()
	if _, err := in.Entry(); err == nil {
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

	// Second ingest (upstream healed, backoff cleared): the new task must
	// resume from the partial spool, not from zero.
	up.heal()
	clearBackoff(m, resolvePath)
	before := len(offsets)
	in, err = m.Ingest("org/repo", "main", "flaky.bin")
	if err != nil {
		t.Fatal(err)
	}
	<-in.Done()
	entry, err := in.Entry()
	if err != nil {
		t.Fatal(err)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, data) {
		t.Fatalf("stored bytes mismatch after resume: got %d bytes, want %d", len(got), len(data))
	}
	offsets = up.rangeOffsets()
	if len(offsets) <= before {
		t.Fatal("second task issued no upstream fetch")
	}
	if first := offsets[before]; first == 0 {
		t.Fatalf("second task restarted from 0 instead of resuming: offsets %v", offsets)
	}
}

// A restart of the mirror process must also resume from the partial spool.
func TestMirrorResumeAcrossRestart(t *testing.T) {
	data := make([]byte, 96*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	up := &flakyUpstream{data: data, commit: "commit-1", etag: "etag-1", failLeft: 1000, sendMax: 24 * 1024}
	upstreamSrv := httptest.NewServer(up)
	defer upstreamSrv.Close()

	storageDir, cacheDir := t.TempDir(), t.TempDir()
	m, _ := newTestMirror(t, upstreamSrv.URL, storageDir, cacheDir)

	in, err := m.Ingest("org/repo", "main", "restart.bin")
	if err != nil {
		t.Fatal(err)
	}
	<-in.Done()
	if _, err := in.Entry(); err == nil {
		t.Fatal("ingest against a failing upstream unexpectedly succeeded")
	}
	up.heal()
	before := len(up.rangeOffsets())

	// "Restart": a fresh engine over the same cache dir.
	m2, stor2 := newTestMirror(t, upstreamSrv.URL, storageDir, cacheDir)
	in, err = m2.Ingest("org/repo", "main", "restart.bin")
	if err != nil {
		t.Fatal(err)
	}
	<-in.Done()
	entry, err := in.Entry()
	if err != nil {
		t.Fatal(err)
	}
	if got := readStored(t, stor2, entry.SHA256); !bytes.Equal(got, data) {
		t.Fatalf("stored bytes mismatch after restart: got %d bytes, want %d", len(got), len(data))
	}
	offsets := up.rangeOffsets()
	if len(offsets) <= before {
		t.Fatal("restarted mirror issued no upstream fetch")
	}
	if first := offsets[before]; first == 0 {
		t.Fatalf("restarted mirror refetched from 0 instead of resuming: offsets %v", offsets)
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

	in, err := m.Ingest("org/repo", "main", "stale.bin")
	if err != nil {
		t.Fatal(err)
	}
	<-in.Done()
	if _, err := in.Entry(); err == nil {
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
	in, err = m.Ingest("org/repo", "main", "stale.bin")
	if err != nil {
		t.Fatal(err)
	}
	<-in.Done()
	entry, err := in.Entry()
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
