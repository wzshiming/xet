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
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/upload"
)

// The spool file name must embed the content identity (etag + size), so a
// reopen with the same validators resumes from the file length and different
// validators land in a different, swept-clean file.
func TestSpoolNamedByValidatorsResume(t *testing.T) {
	dir := t.TempDir()
	const origin, key = "https://hub.example", "/org/repo/resolve/main/a.bin"
	etag := strings.Repeat("ab", 16) // md5-style hex etag: not a content hash, so the name stays key-prefixed

	sp, err := openSpool(dir, origin, key, etag, 100, true)
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
	sp2, err := openSpool(dir, origin, key, etag, 100, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := sp2.size(); got != 40 {
		t.Fatalf("resumed size = %d, want 40", got)
	}
	sp2.finish(fmt.Errorf("interrupted again"))

	// Changed etag: new file from zero, stale sibling swept.
	etag2 := strings.Repeat("cd", 16)
	sp3, err := openSpool(dir, origin, key, etag2, 100, true)
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
	sp4, err := openSpool(dir, origin, key, "", -1, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sp4.Write(make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	sp4.finish(fmt.Errorf("interrupted"))
	sp5, err := openSpool(dir, origin, key, "", -1, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := sp5.size(); got != 0 {
		t.Fatalf("etag-less spool resumed (%d bytes), want truncation", got)
	}
	sp5.finish(nil)
}

// A content-hash etag names the shared spool by the origin and the hash alone,
// so every key of that origin with that content opens (and resumes) the same
// file, another origin's never does, and other etags — or a private spool —
// keep the key-prefixed name.
func TestSpoolNamedByContentHash(t *testing.T) {
	dir := t.TempDir()
	const origin, other = "https://hub.example", "https://other.example"
	const keyA, keyB = "/org/repo/resolve/main/a.bin", "/other/repo/resolve/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/b.bin"
	sha, blob := strings.Repeat("Ab", 32), strings.Repeat("Cd", 20)
	for _, tc := range []struct{ etag, want string }{
		{sha, spoolOriginPrefix(origin) + strings.ToLower(sha) + ".spool"},
		{blob, spoolOriginPrefix(origin) + strings.ToLower(blob) + ".spool"},
		{"etag1", spoolKeyPrefix(keyA) + spoolETagToken("etag1") + "-100.spool"},
		{"", spoolKeyPrefix(keyA) + "noetag-100.spool"},
	} {
		if got := spoolFileName(origin, keyA, tc.etag, 100, true); got != tc.want {
			t.Fatalf("spoolFileName(%q) = %q, want %q", tc.etag, got, tc.want)
		}
	}
	if got, want := spoolFileName(origin, keyA, sha, 100, false), spoolKeyPrefix(keyA)+strings.ToLower(sha)+"-100.spool"; got != want {
		t.Fatalf("private spoolFileName(%q) = %q, want the key-prefixed %q", sha, got, want)
	}
	if a, b := spoolFileName(origin, keyA, sha, 100, true), spoolFileName(origin, keyB, sha, -1, true); a != b {
		t.Fatalf("content spool names differ by key: %q vs %q", a, b)
	}
	if a, b := spoolFileName(origin, keyA, sha, 100, true), spoolFileName(other, keyA, sha, 100, true); a == b {
		t.Fatalf("content spool names collide across origins: %q", a)
	}
	if spoolOriginPrefix(keyA) == spoolKeyPrefix(keyA) {
		t.Fatal("origin and key prefixes share a hash domain")
	}
	if a, b := spoolFileName(origin, keyA, "etag1", 100, true), spoolFileName(origin, keyB, "etag1", 100, true); a == b {
		t.Fatalf("key-prefixed spool names collide across keys: %q", a)
	}

	// Opening the content spool for a key sheds the key's stale key-prefixed spool.
	stale, err := openSpool(dir, origin, keyA, "etag1", 100, true)
	if err != nil {
		t.Fatal(err)
	}
	stale.finish(errors.New("interrupted"))
	sp, err := openSpool(dir, origin, keyA, sha, 100, true)
	if err != nil {
		t.Fatal(err)
	}
	if files := spoolFiles(t, dir); !slices.Equal(files, []string{spoolOriginPrefix(origin) + strings.ToLower(sha) + ".spool"}) {
		t.Fatalf("spool files after opening the content spool = %v, want it alone", files)
	}
	if _, err := sp.Write(make([]byte, 40)); err != nil {
		t.Fatal(err)
	}
	sp.finish(errors.New("interrupted"))

	// Another key of the same origin with the same content resumes the bytes the first wrote; another origin starts its own file.
	sp2, err := openSpool(dir, origin, keyB, sha, 100, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := sp2.size(); got != 40 {
		t.Fatalf("resumed size under another key = %d, want 40", got)
	}
	sp2.finish(nil)
	sp3, err := openSpool(dir, other, keyB, sha, 100, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := sp3.size(); got != 0 {
		t.Fatalf("size under another origin = %d, want 0", got)
	}
	sp3.finish(nil)
	if files := spoolFiles(t, dir); len(files) != 2 {
		t.Fatalf("spool files after two origins = %v, want one per origin", files)
	}
}

// markRemove unlinks the file once: a spool opened later at the same content
// name is not removed when the first one retires.
func TestSpoolMarkRemoveUnlinksOnce(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("open files cannot be unlinked on windows")
	}
	dir := t.TempDir()
	etag := strings.Repeat("ab", 32)
	a, err := openSpool(dir, "https://hub.example", "/org/repo/resolve/main/a.bin", etag, 40, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Write(make([]byte, 40)); err != nil {
		t.Fatal(err)
	}
	rc := a.newReader(context.Background(), 0)
	if rc == nil {
		t.Fatal("newReader returned nil on a live spool")
	}
	a.finish(nil)
	a.markRemove()
	if _, err := os.Stat(a.f.Name()); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("spool after markRemove: %v, want unlinked", err)
	}

	b, err := openSpool(dir, "https://hub.example", "/other/repo/resolve/main/b.bin", etag, 40, true)
	if err != nil {
		t.Fatal(err)
	}
	if b.f.Name() != a.f.Name() {
		t.Fatalf("second spool at %q, want the content name %q", b.f.Name(), a.f.Name())
	}
	if _, err := b.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if got, err := io.ReadAll(rc); err != nil || len(got) != 40 {
		t.Fatalf("attached reader after unlink read %d bytes, %v; want 40", len(got), err)
	}
	_ = rc.Close() // a retires
	if _, err := os.Stat(b.f.Name()); err != nil {
		t.Fatalf("spool reopened at the same name after the first one retired: %v, want kept", err)
	}
	b.finish(nil)
}

// xorbRejectingStorage refuses every xorb write, so an ingest fails only after its fetch completed.
type xorbRejectingStorage struct {
	storage.Storage
}

func (xorbRejectingStorage) PutXorb(context.Context, string, xet.XorbHash, io.Reader) (bool, error) {
	return false, errors.New("xorb write refused")
}

// An etag-less spool that was fully fetched but failed to land in storage is
// as unresumable as a partial one: nothing names its bytes, so it must go.
func TestSpoolEtaglessIngestFailureRemoved(t *testing.T) {
	data := make([]byte, 64*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	var gets atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			gets.Add(1)
		}
		w.Header().Set("X-Repo-Commit", "commit-1")
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(data))
	}))
	t.Cleanup(srv.Close)
	stor, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	m, _ := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir(), WithStorage(xorbRejectingStorage{stor}))
	ctx := context.Background()

	res, err := m.Resolve(ctx, "org/repo", "main", "noetag.bin")
	if err != nil || res.Stream == nil {
		t.Fatalf("Resolve = %+v, %v; want an in-flight stream", res, err)
	}
	if etag, _, err := res.Stream.WaitMeta(ctx); err != nil || etag != "" {
		t.Fatalf("WaitMeta = %q, %v; want no etag", etag, err)
	}
	awaitClosed(t, res.Stream.t.done, "etag-less ingest")
	if _, err := m.Resolve(ctx, "org/repo", "main", "noetag.bin"); err == nil || !strings.Contains(err.Error(), "xorb write refused") {
		t.Fatalf("ingest err = %v, want the storage failure", err)
	}
	if got := gets.Load(); got != 1 || res.Stream.t.spool.size() != int64(len(data)) {
		t.Fatalf("data GETs = %d, spooled %d bytes; want one complete fetch of %d bytes before the failure", got, res.Stream.t.spool.size(), len(data))
	}
	if files := spoolFiles(t, m.spoolDir); len(files) != 0 {
		t.Fatalf("spool files after the etag-less ingest failure = %v, want none", files)
	}
}

// A follower fails with its leader's error, and the content spool they
// shared stays behind: the next task under either key resumes it.
func TestIngestFollowerSharesFailure(t *testing.T) {
	data := make([]byte, 96*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	commit := strings.Repeat("ab", 20)
	up := &flakyUpstream{data: data, commit: commit, etag: hashHex(string(data)), failLeft: 1000, sendMax: 32 * 1024}
	srv := httptest.NewServer(up)
	t.Cleanup(srv.Close)
	m, stor := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir())

	a, err := m.Ingest("org/repo", "main", "a.bin")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		m.mu.Lock()
		leader := m.inflight[srv.URL+"\x00"+up.etag]
		m.mu.Unlock()
		if leader != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a.bin never led the download of its content")
		}
		time.Sleep(10 * time.Millisecond)
	}
	b, err := m.Ingest("org/repo", "main", "b.bin")
	if err != nil {
		t.Fatal(err)
	}
	awaitClosed(t, a.Done(), "leader ingest")
	awaitClosed(t, b.Done(), "follower ingest")
	_, errA := a.Entry()
	_, errB := b.Entry()
	if errA == nil || errB == nil || errA.Error() != errB.Error() {
		t.Fatalf("leader err = %v, follower err = %v; want the follower to fail with the leader's error", errA, errB)
	}
	if files := spoolFiles(t, m.spoolDir); !slices.Equal(files, []string{spoolFileName(srv.URL, "", up.etag, int64(len(data)), true)}) {
		t.Fatalf("spool files after the shared failure = %v, want the partial content spool", files)
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
		t.Fatalf("stored bytes mismatch after the resumed ingest: got %d bytes, want %d", len(got), len(data))
	}
	offsets := up.rangeOffsets()
	if len(offsets) <= before {
		t.Fatal("the task under the other key issued no upstream fetch")
	}
	if first := offsets[before]; first == 0 {
		t.Fatalf("the task under the other key restarted from 0 instead of resuming the shared spool: offsets %v", offsets)
	}
	if files := spoolFiles(t, m.spoolDir); len(files) != 0 {
		t.Fatalf("spool files after the resumed ingest = %v, want none", files)
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
	if c.Stream.t.spool != a.Stream.t.spool {
		t.Fatal("honest follower did not attach to the leader's spool")
	}
	if size, ok := c.Stream.WaitSize(ctx); !ok || size != int64(len(data)) {
		t.Fatalf("honest follower WaitSize = %d, %v; want %d, true", size, ok, len(data))
	}
	rc := c.Stream.NewReader(ctx, 0)
	if rc == nil {
		t.Fatal("honest follower NewReader returned nil")
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

// A leader found in inflight after its spool retired — it published, or
// failed, and has not yet left the map — offers nothing to attach to: the
// newcomer downloads into a private spool of its own, whatever the leader's
// outcome.
func TestIngestFollowerAfterLeaderRetired(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.commit = strings.Repeat("ab", 20)
	data := []byte("content a follower takes from its leader")
	for _, p := range []string{"a.bin", "f.bin", "g.bin"} {
		upstream.set("/org/repo/resolve/main/"+p, data)
	}
	adverts := map[string]blobAdvert{"f.bin": {strings.Repeat("1a", 20), int64(len(data))}, "g.bin": {strings.Repeat("2b", 20), int64(len(data))}}
	srv := blobServer(t, upstream, adverts)
	m, _ := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir())
	ctx := context.Background()
	stored, err := ingestWait(t, m, "org/repo", "main", "a.bin")
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	close(closed)
	entry := &fileEntry{State: stateReady, FileHash: stored.FileHash, SHA256: stored.SHA256, Size: stored.Size, ETag: stored.SHA256}
	for _, tc := range []struct {
		name, path string
		entry      *fileEntry
		err        error
	}{{"published leader", "f.bin", entry, nil}, {"failed leader", "g.bin", nil, errors.New("open spool file: is a directory")}} {
		t.Run(tc.name, func(t *testing.T) {
			blob := adverts[tc.path].blob
			sp, err := openSpool(m.spoolDir, srv.URL, "/org/repo/resolve/main/a.bin", blob, int64(len(data)), true)
			if err != nil {
				t.Fatal(err)
			}
			sp.finish(tc.err) // no reference left: the spool retires
			leader := &task{spool: sp, sized: closed, done: closed, entry: tc.entry, err: tc.err}
			leader.size.Store(int64(len(data)))
			m.mu.Lock()
			m.inflight[srv.URL+"\x00"+blob] = leader
			m.mu.Unlock()

			before := upstream.dataGETs.Load()
			res, err := m.Resolve(ctx, "org/repo", "main", tc.path)
			if err != nil || res.Stream == nil {
				t.Fatalf("resolve %s = %+v, %v; want an in-flight stream", tc.path, res, err)
			}
			ft := res.Stream.t
			if ft.spool == nil || ft.spool == sp || ft.leader != nil {
				t.Fatal("newcomer attached to the retired spool")
			}
			src := "/org/repo/resolve/" + upstream.commit + "/" + tc.path
			if got, want := filepath.Base(ft.spool.f.Name()), spoolFileName(srv.URL, src, blob, int64(len(data)), false); got != want {
				t.Fatalf("newcomer spools at %q, want the private %q", got, want)
			}
			awaitClosed(t, ft.done, "own download")
			got, err := m.Resolve(ctx, "org/repo", "main", tc.path)
			if err != nil || got.Entry == nil || got.Entry.FileHash != stored.FileHash || got.Entry.Size != stored.Size || got.Entry.ETag != blob {
				t.Fatalf("%s after its own download = %+v, %v; want the stored file under etag %s", tc.path, got, err, blob)
			}
			if n := upstream.dataGETs.Load(); n != before+1 {
				t.Fatalf("data GETs went %d -> %d, want the newcomer's own download", before, n)
			}
		})
	}
}

// A leader whose size stayed unknown offers no body a follower can trust: a
// key advertising a size downloads on its own, in a private spool beside the
// content spool the leader keeps writing, and the leader keeps leading its
// content until its own end.
func TestIngestFollowerWithUnknownLeaderSize(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.commit = strings.Repeat("ab", 20)
	data := []byte("content a follower takes from its leader")
	upstream.set("/org/repo/resolve/main/a.bin", data)
	upstream.set("/org/repo/resolve/main/f.bin", data)
	blob := strings.Repeat("1a", 20)
	srv := blobServer(t, upstream, map[string]blobAdvert{"f.bin": {blob, int64(len(data))}})
	m, _ := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir())
	ctx := context.Background()
	stored, err := ingestWait(t, m, "org/repo", "main", "a.bin")
	if err != nil {
		t.Fatal(err)
	}
	// The leader is halfway into the content spool of its origin.
	sp, err := openSpool(m.spoolDir, srv.URL, "/org/repo/resolve/main/a.bin", blob, -1, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sp.Write(data[:len(data)/2]); err != nil {
		t.Fatal(err)
	}
	sp.acquire()
	t.Cleanup(func() { sp.finish(nil); sp.release() })
	closed := make(chan struct{})
	close(closed)
	// The leader's first response carried no length either; it is still downloading.
	leader := &task{spool: sp, sized: closed, done: make(chan struct{})}
	leader.size.Store(-1)
	id := srv.URL + "\x00" + blob
	m.mu.Lock()
	m.inflight[id] = leader
	m.mu.Unlock()

	before := upstream.dataGETs.Load()
	res, err := m.Resolve(ctx, "org/repo", "main", "f.bin")
	if err != nil || res.Stream == nil {
		t.Fatalf("resolve f.bin = %+v, %v; want an in-flight stream", res, err)
	}
	ft := res.Stream.t
	if ft.spool == nil || ft.spool == sp || ft.leader != nil {
		t.Fatal("follower attached to a spool of unknown length")
	}
	awaitClosed(t, ft.done, "own download")
	got, err := m.Resolve(ctx, "org/repo", "main", "f.bin")
	if err != nil || got.Entry == nil || got.Entry.FileHash != stored.FileHash || got.Entry.Size != stored.Size || got.Entry.ETag != blob {
		t.Fatalf("f.bin after its own download = %+v, %v; want the stored file under etag %s", got, err, blob)
	}
	if n := upstream.dataGETs.Load(); n != before+1 {
		t.Fatalf("data GETs went %d -> %d, want f.bin's own download", before, n)
	}
	if fi, err := os.Stat(sp.f.Name()); err != nil || fi.Size() != int64(len(data)/2) {
		t.Fatalf("leader's content spool after f.bin's download: %v, %v; want its %d bytes untouched", fi, err, len(data)/2)
	}
	src := "/org/repo/resolve/" + upstream.commit + "/f.bin"
	if got, want := filepath.Base(ft.spool.f.Name()), spoolFileName(srv.URL, src, blob, int64(len(data)), false); got != want {
		t.Fatalf("f.bin spooled at %q, want the private %q beside the leader's content spool", got, want)
	}
	m.mu.Lock()
	leading := m.inflight[id]
	m.mu.Unlock()
	if leading != leader {
		t.Fatalf("inflight task for the content = %p, want the running leader %p", leading, leader)
	}
}

// A follower arriving before its leader's first response waits for the
// leader's size inside the flight — a caller may stop waiting, and no task
// exists meanwhile — then is handed out on the leader's spool (or refused)
// while the leader still runs, and publishes the leader's entry once it ends.
func TestIngestFollowerWaitsForLeaderSize(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.commit = strings.Repeat("ab", 20)
	data := []byte("content a follower streams from its leader")
	for _, p := range []string{"a.bin", "f.bin", "g.bin"} {
		upstream.set("/org/repo/resolve/main/"+p, data)
	}
	size := int64(len(data))
	adverts := map[string]blobAdvert{"f.bin": {strings.Repeat("1a", 20), size}, "g.bin": {strings.Repeat("2b", 20), size + 1}}
	srv := blobServer(t, upstream, adverts)
	m, _ := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir())
	ctx := context.Background()
	// A real stored file, so the follower's published entry passes entryLive.
	stored, err := ingestWait(t, m, "org/repo", "main", "a.bin")
	if err != nil {
		t.Fatal(err)
	}
	entry := &fileEntry{State: stateReady, FileHash: stored.FileHash, SHA256: stored.SHA256, Size: stored.Size, ETag: stored.SHA256}
	for _, tc := range []struct{ name, path, wantErr string }{
		{"agreeing size streams", "f.bin", ""},
		{"disagreeing size rejected", "g.bin", "disagrees with the shared download's"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blob := adverts[tc.path].blob
			sp, err := openSpool(m.spoolDir, srv.URL, "/org/repo/resolve/main/a.bin", blob, -1, true)
			if err != nil {
				t.Fatal(err)
			}
			sp.acquire()
			// The leader has probed without a size and has not yet received its first response.
			leader := &task{spool: sp, sized: make(chan struct{}), done: make(chan struct{}), entry: entry}
			leader.size.Store(-1)
			var finish sync.Once
			finishLeader := func() {
				finish.Do(func() {
					sp.finish(nil)
					close(leader.done)
					sp.release()
				})
			}
			t.Cleanup(finishLeader)
			m.mu.Lock()
			m.inflight[srv.URL+"\x00"+blob] = leader
			m.mu.Unlock()
			key := resolveKey{repo: "org/repo", rev: upstream.commit, path: tc.path}

			short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()
			if _, err := m.Resolve(short, "org/repo", "main", tc.path); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("resolve before the leader's size err = %v; want the caller's deadline", err)
			}
			m.mu.Lock()
			early := m.tasks[key]
			m.mu.Unlock()
			if early != nil {
				t.Fatal("a task was registered before the leader's size was known")
			}
			leader.size.Store(size)
			close(leader.sized)
			waitCtx, cancelWait := context.WithTimeout(ctx, 5*time.Second)
			defer cancelWait()
			res, err := m.Resolve(waitCtx, "org/repo", "main", tc.path)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("resolve once the leader's size is known = %+v, %v; want %q", res, err, tc.wantErr)
				}
				m.mu.Lock()
				e, rejected := m.entries[key], m.tasks[key]
				m.mu.Unlock()
				if rejected != nil || e == nil || e.State != stateFailed {
					t.Fatalf("rejected follower holds %+v with task %v; want a recorded failure and no task", e, rejected)
				}
				return
			}
			if err != nil || res.Stream == nil {
				t.Fatalf("resolve once the leader's size is known = %+v, %v; want a stream while the leader runs", res, err)
			}
			ft := res.Stream.t
			if ft.spool != sp || ft.leader != leader {
				t.Fatal("follower was not handed out on the leader's spool")
			}
			if got, ok := res.Stream.WaitSize(waitCtx); !ok || got != size {
				t.Fatalf("follower WaitSize = %d, %v; want %d", got, ok, size)
			}
			rc := res.Stream.NewReader(ctx, 0)
			if rc == nil {
				t.Fatal("follower NewReader returned nil while the leader runs")
			}
			_ = rc.Close()
			finishLeader()
			awaitClosed(t, ft.done, "follower")
			got, err := m.Resolve(ctx, "org/repo", "main", tc.path)
			if err != nil || got.Entry == nil || got.Entry.FileHash != stored.FileHash || got.Entry.Size != stored.Size || got.Entry.ETag != blob {
				t.Fatalf("%s after the shared ingest = %+v, %v; want the leader's entry under etag %s", tc.path, got, err, blob)
			}
		})
	}
}

// A follower never publishes a leader's entry whose verified digest differs
// from its own probe: handed out on the leader's spool, it fails once the
// leader ends with the other digest.
func TestIngestFollowerRejectsSHA256Mismatch(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.commit = strings.Repeat("ab", 20)
	data := []byte("content whose shared download verifies to another digest")
	upstream.set("/org/repo/resolve/main/f.bin", data)
	srv := httptest.NewServer(upstream)
	t.Cleanup(srv.Close)
	m, _ := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir())
	ctx := context.Background()
	sha, other := hashHex(string(data)), strings.Repeat("cd", 32)
	sp, err := openSpool(m.spoolDir, srv.URL, "/org/repo/resolve/main/a.bin", sha, int64(len(data)), true)
	if err != nil {
		t.Fatal(err)
	}
	sp.acquire()
	closed := make(chan struct{})
	close(closed)
	leader := &task{spool: sp, sized: closed, done: make(chan struct{}), entry: &fileEntry{State: stateReady, FileHash: strings.Repeat("ef", 32), SHA256: other, Size: int64(len(data)), ETag: other}}
	leader.size.Store(int64(len(data)))
	m.mu.Lock()
	m.inflight[srv.URL+"\x00"+sha] = leader
	m.mu.Unlock()

	res, err := m.Resolve(ctx, "org/repo", "main", "f.bin")
	if err != nil || res.Stream == nil || res.Stream.t.spool != sp || res.Stream.t.leader != leader {
		t.Fatalf("resolve f.bin = %+v, %v; want a stream on the leader's spool", res, err)
	}
	in, err := m.Ingest("org/repo", "main", "f.bin")
	if err != nil {
		t.Fatal(err)
	}
	sp.finish(nil)
	close(leader.done)
	sp.release()
	awaitClosed(t, in.Done(), "follower ingest")
	if _, err := in.Entry(); !errors.Is(err, errSpoolCorrupt) {
		t.Fatalf("follower err = %v; want the digest mismatch", err)
	}
	key := resolveKey{repo: "org/repo", rev: upstream.commit, path: "f.bin"}
	m.mu.Lock()
	e, tasks := m.entries[key], len(m.tasks)
	m.mu.Unlock()
	if tasks != 0 || e == nil || e.State != stateFailed || !errors.Is(e.lastErr, errSpoolCorrupt) {
		t.Fatalf("follower key holds %+v with %d tasks; want a recorded digest failure", e, tasks)
	}
	if _, err := m.Resolve(ctx, "org/repo", "main", "f.bin"); !errors.Is(err, errSpoolCorrupt) {
		t.Fatalf("f.bin after the mismatch: err = %v; want the recorded digest failure", err)
	}
}

// Every task is handed out holding a spool, whichever way it came to be —
// the downloader, the follower of its content, the task resuming the partial
// spool a failed task left — so its readers are never nil for want of one;
// a key whose probe fails registers no task and fails its Resolve directly.
func TestTaskAlwaysHoldsSpool(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.gate = make(chan struct{})
	upstream.gateHit = make(chan struct{})
	upstream.commit = strings.Repeat("ab", 20)
	shared, resumed := make([]byte, 64*1024), make([]byte, 64*1024)
	for _, b := range [][]byte{shared, resumed} {
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
	}
	upstream.set("/org/repo/resolve/main/a.bin", shared)
	upstream.set("/org/repo/resolve/main/b.bin", shared)
	upstream.set("/org/repo/resolve/main/c.bin", resumed)
	// c.bin's resumed download is held before its first byte, as a.bin's is at its half.
	hold, held := make(chan struct{}), make(chan struct{})
	var heldOnce sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/cdn") && strings.HasSuffix(r.URL.Path, "/c.bin") {
			heldOnce.Do(func() { close(held) })
			<-hold
		}
		upstream.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	release := sync.OnceFunc(func() {
		close(upstream.gate)
		close(hold)
	})
	m, stor := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir())
	t.Cleanup(release) // registered after newTestMirror so the gates open before its cleanup waits for the tasks
	ctx := context.Background()

	// A failed task of this origin left half of c.bin in its content spool.
	partial, err := openSpool(m.spoolDir, srv.URL, "/org/repo/resolve/"+upstream.commit+"/c.bin", hashHex(string(resumed)), int64(len(resumed)), true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partial.Write(resumed[:len(resumed)/2]); err != nil {
		t.Fatal(err)
	}
	partial.finish(errors.New("interrupted"))

	a, err := m.Resolve(ctx, "org/repo", "main", "a.bin")
	if err != nil || a.Stream == nil {
		t.Fatalf("resolve a.bin = %+v, %v; want an in-flight stream", a, err)
	}
	awaitClosed(t, upstream.gateHit, "leader download")
	b, err := m.Resolve(ctx, "org/repo", "main", "b.bin")
	if err != nil || b.Stream == nil {
		t.Fatalf("resolve b.bin = %+v, %v; want an in-flight stream", b, err)
	}
	c, err := m.Resolve(ctx, "org/repo", "main", "c.bin")
	if err != nil || c.Stream == nil {
		t.Fatalf("resolve c.bin = %+v, %v; want an in-flight stream", c, err)
	}
	awaitClosed(t, held, "resumed download")
	if b.Stream.t.spool != a.Stream.t.spool || b.Stream.t.leader != a.Stream.t {
		t.Fatal("b.bin did not follow a.bin's download")
	}
	if got, want := filepath.Base(a.Stream.t.spool.f.Name()), spoolFileName(srv.URL, "/org/repo/resolve/"+upstream.commit+"/a.bin", hashHex(string(shared)), int64(len(shared)), true); got != want || a.Stream.t.lead == "" {
		t.Fatalf("a.bin leads from %q with lead %q; want the content spool %q", got, a.Stream.t.lead, want)
	}
	if got := c.Stream.t.spool; got.f.Name() != partial.f.Name() || got.size() != int64(len(resumed)/2) {
		t.Fatalf("c.bin holds %d bytes at %s; want the %d resumed bytes at %s", got.size(), got.f.Name(), len(resumed)/2, partial.f.Name())
	}
	m.mu.Lock()
	var running []*task
	for _, tk := range m.tasks {
		running = append(running, tk)
	}
	m.mu.Unlock()
	if len(running) != 3 {
		t.Fatalf("tasks in flight = %d, want a.bin, b.bin and c.bin", len(running))
	}
	for _, tk := range running {
		if tk.spool == nil {
			t.Fatalf("%s runs without a spool", tk.key.path)
		}
		st := &Stream{t: tk}
		size, ok := st.WaitSize(ctx)
		if !ok || size != 64*1024 {
			t.Fatalf("%s WaitSize = %d, %v; want %d, true", tk.key.path, size, ok, 64*1024)
		}
		rc, rs := st.NewReader(ctx, 0), st.NewSeekReader(ctx, size)
		if rc == nil || rs == nil {
			t.Fatalf("%s: NewReader %v, NewSeekReader %v; want readers on the task's spool", tk.key.path, rc != nil, rs != nil)
		}
		_ = rc.Close()
		_ = rs.Close()
	}

	if _, err := m.Resolve(ctx, "org/repo", "main", "missing.bin"); !errors.Is(err, ErrUpstreamNotFound) {
		t.Fatalf("resolve of a missing file err = %v; want ErrUpstreamNotFound", err)
	}
	m.mu.Lock()
	missing, tasks := m.tasks[resolveKey{repo: "org/repo", rev: upstream.commit, path: "missing.bin"}], len(m.tasks)
	m.mu.Unlock()
	if missing != nil || tasks != 3 {
		t.Fatalf("missing.bin registered task %v among %d tasks; want none", missing, tasks)
	}

	release()
	for _, st := range []*Stream{a.Stream, b.Stream, c.Stream} {
		awaitClosed(t, st.t.done, st.t.key.path)
	}
	for p, want := range map[string][]byte{"a.bin": shared, "b.bin": shared, "c.bin": resumed} {
		res, err := m.Resolve(ctx, "org/repo", "main", p)
		if err != nil || res.Entry == nil {
			t.Fatalf("%s after the ingest = %+v, %v; want the ready entry", p, res, err)
		}
		if got := readStored(t, stor, res.Entry.SHA256); !bytes.Equal(got, want) {
			t.Fatalf("%s: stored bytes differ from upstream data", p)
		}
	}
	if got := upstream.dataGETs.Load(); got != 2 {
		t.Fatalf("upstream data GETs = %d, want 2 (a.bin's shared download and c.bin's resume)", got)
	}
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

	in, err := m.Ingest("org/repo", "main", "stall.bin")
	if err != nil {
		t.Fatal(err)
	}
	awaitClosed(t, in.Done(), "stalled ingest")
	entry, err := in.Entry()
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
	seed := &localCAS{storage: stor, namespace: "default"}
	// Seeding the prefix makes the combined upload span two xorbs.
	if _, err := upload.UploadFile(ctx, seed, bytes.NewReader(head), upload.WithEnableSHA256(true)); err != nil {
		t.Fatal(err)
	}
	fileHash, err := upload.UploadFile(ctx, seed, bytes.NewReader(data), upload.WithEnableSHA256(true))
	if err != nil {
		t.Fatal(err)
	}
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

	in, err := m.Ingest("org/repo", "main", "stall.bin")
	if err != nil {
		t.Fatal(err)
	}
	awaitClosed(t, in.Done(), "stalled xet ingest")
	entry, err := in.Entry()
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

	in, err := m.Ingest("org/repo", "main", "slow.bin")
	if err != nil {
		t.Fatal(err)
	}
	awaitClosed(t, in.Done(), "slow xet ingest")
	entry, err := in.Entry()
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
