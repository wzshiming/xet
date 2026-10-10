package spool

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/ioswmr"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/memory"
)

// xorbRejectingStorage refuses every xorb write, so an ingest fails only after its spool is complete.
type xorbRejectingStorage struct {
	storage.Storage
}

func (xorbRejectingStorage) PutXorb(context.Context, string, xet.XorbHash, io.Reader) (bool, error) {
	return false, errors.New("xorb write refused")
}

// spoolFiles lists the .spool names under dir in directory order.
func spoolFiles(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, ent := range ents {
		if strings.HasSuffix(ent.Name(), ".spool") {
			names = append(names, ent.Name())
		}
	}
	return names
}

func TestSpoolTailRead(t *testing.T) {
	name := fileName("https://hub.example", "k", "", -1, true)
	m, err := open(filepath.Join(t.TempDir(), name), "", -1)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 64*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	rc, err := m.NewReader(0)
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(rc)
		_ = rc.Close()
		got <- b
	}()

	// Write in pieces so the reader has to wait repeatedly.
	for i := 0; i < len(data); i += 8192 {
		if _, err := m.Writer().Write(data[i : i+8192]); err != nil {
			t.Fatal(err)
		}
	}
	_ = m.Writer().CloseWithError(nil)

	if b := <-got; !bytes.Equal(b, data) {
		t.Fatalf("tail read mismatch: got %d bytes, want %d", len(b), len(data))
	}

	t.Run("close unblocks reader", func(t *testing.T) {
		m, err := open(filepath.Join(t.TempDir(), name), "", -1)
		if err != nil {
			t.Fatal(err)
		}
		defer m.Writer().CloseWithError(nil)
		rc := (&Item{f: &flight{swmr: m}}).NewReader(0)
		errCh := make(chan error, 1)
		go func() {
			_, err := rc.Read(make([]byte, 1))
			errCh <- err
		}()
		_ = rc.Close()
		if err := <-errCh; !errors.Is(err, ioswmr.ErrClosedPipe) {
			t.Fatalf("read err = %v, want ioswmr.ErrClosedPipe", err)
		}
	})

	t.Run("no readers after removal", func(t *testing.T) {
		m, err := open(filepath.Join(t.TempDir(), name), "", -1)
		if err != nil {
			t.Fatal(err)
		}
		_ = m.Writer().CloseWithError(nil) // no refs: the file is removed immediately
		it := &Item{f: &flight{swmr: m}}
		if rc := it.NewReader(0); rc != nil {
			t.Fatal("expected nil reader after removal")
		}
		if rs := it.NewSeekReader(0); rs != nil {
			t.Fatal("expected nil seek reader after removal")
		}
	})
}

// The spool file name must embed the content identity (etag + size), so a
// crash leftover with the same validators is resumed from its length, other
// validators get another name, and a leftover that cannot be the content is
// removed at open.
func TestSpoolNamedByValidatorsResume(t *testing.T) {
	dir := t.TempDir()
	const origin, key = "https://hub.example", "/org/repo/resolve/main/a.bin"
	etag := strings.Repeat("ab", 16) // md5-style hex etag: not a content hash, so the name stays key-prefixed
	name := fileName(origin, key, etag, 100, true)
	if !strings.HasPrefix(name, keyPrefix(key)) {
		t.Fatalf("spool name = %q, want one prefixed %q", name, keyPrefix(key))
	}
	leftover := filepath.Join(dir, name)
	if err := os.WriteFile(leftover, make([]byte, 40), 0o644); err != nil {
		t.Fatal(err)
	}

	// Same validators: adopt the leftover and resume from its length.
	m, err := open(leftover, etag, 100)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Length(); got != 40 {
		t.Fatalf("resumed size = %d, want 40", got)
	}

	// Changed etag: another name from zero; the leftover keeps its 40 bytes.
	etag2 := strings.Repeat("cd", 16)
	name2 := fileName(origin, key, etag2, 100, true)
	if name2 == name {
		t.Fatalf("spool name after the etag change = %q, want a new name", name2)
	}
	m2, err := open(filepath.Join(dir, name2), etag2, 100)
	if err != nil {
		t.Fatal(err)
	}
	if got := m2.Length(); got != 0 {
		t.Fatalf("size after etag change = %d, want 0", got)
	}
	if info, err := os.Stat(leftover); err != nil || info.Size() != 40 {
		t.Fatalf("leftover after the etag change: %v, %v; want its 40 bytes kept", info, err)
	}
	_ = m2.Writer().CloseWithError(nil)

	// A reader of the resumed spool sees the leftover's bytes, then the appended ones.
	rc, err := m.NewReader(0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Writer().Write([]byte("appended")); err != nil {
		t.Fatal(err)
	}
	_ = m.Writer().CloseWithError(nil)
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if want := append(make([]byte, 40), "appended"...); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("resumed spool read %q, %v; want the 40 leftover bytes, then %q", got, err, "appended")
	}
	if _, err := os.Stat(leftover); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("adopted leftover after its last reader: %v, want removed", err)
	}

	// No etag, nothing written, or longer than expected: removed, the spool starts from zero.
	for _, tc := range []struct {
		name string
		etag string
		size int64
		data []byte
	}{
		{"etag-less", "", -1, make([]byte, 8)},
		{"empty", etag, 100, nil},
		{"overlong", etag, 10, make([]byte, 40)},
	} {
		path := filepath.Join(dir, fileName(origin, key, tc.etag, tc.size, true))
		if err := os.WriteFile(path, tc.data, 0o644); err != nil {
			t.Fatal(err)
		}
		m, err := open(path, tc.etag, tc.size)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(path); m.Length() != 0 || !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("%s leftover: spool resumed %d bytes, file %v; want 0 and the file removed", tc.name, m.Length(), err)
		}
		_ = m.Writer().CloseWithError(nil)
	}
}

// A spool stays in memory until its bytes outgrow the pooled tier and spills
// to its file past it; the file goes once the writer finished and the last
// reader let go.
func TestSpoolSpill(t *testing.T) {
	exists := func(t *testing.T, path string) bool {
		t.Helper()
		_, err := os.Stat(path)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			t.Fatal(err)
		}
		return err == nil
	}
	for _, tc := range []struct {
		name   string
		size   int
		spills bool
	}{
		{"8 KiB stays in memory", 8 << 10, false},
		{"64 KiB spills", 64 << 10, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, tc.size)
			if _, err := rand.Read(data); err != nil {
				t.Fatal(err)
			}
			etag := strings.Repeat("ab", 16)
			path := filepath.Join(t.TempDir(), fileName("https://hub.example", "/org/repo/resolve/main/a.bin", etag, int64(tc.size), true))
			m, err := open(path, etag, int64(tc.size))
			if err != nil {
				t.Fatal(err)
			}
			rc, err := m.NewReader(0) // open takes no hold: the reader keeps the finished spool alive
			if err != nil {
				t.Fatal(err)
			}
			if _, err := m.Writer().Write(data); err != nil {
				t.Fatal(err)
			}
			if got := exists(t, path); got != tc.spills {
				t.Fatalf("spool file exists = %v before finish, want %v", got, tc.spills)
			}
			_ = m.Writer().CloseWithError(nil)
			if got := exists(t, path); got != tc.spills {
				t.Fatalf("spool file exists = %v after finish with a reader attached, want %v", got, tc.spills)
			}
			b, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil || !bytes.Equal(b, data) {
				t.Fatalf("reader read %d bytes, %v; want all %d", len(b), err, len(data))
			}
			if exists(t, path) {
				t.Fatal("spool file exists after the last reader let go, want removed")
			}
		})
	}
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
		{sha, originPrefix(origin) + strings.ToLower(sha) + ".spool"},
		{blob, originPrefix(origin) + strings.ToLower(blob) + ".spool"},
	} {
		if got := fileName(origin, keyA, tc.etag, 100, true); got != tc.want {
			t.Fatalf("fileName(%q) = %q, want %q", tc.etag, got, tc.want)
		}
	}
	etagName, noETagName := fileName(origin, keyA, "etag1", 100, true), fileName(origin, keyA, "", 100, true)
	if !strings.HasPrefix(etagName, keyPrefix(keyA)) || !strings.HasPrefix(noETagName, keyPrefix(keyA)) || etagName == noETagName {
		t.Fatalf("fileName(etag1) = %q, fileName(no etag) = %q; want two names prefixed %q", etagName, noETagName, keyPrefix(keyA))
	}
	for _, size := range []int64{101, -1} {
		if got := fileName(origin, keyA, "etag1", size, true); got == etagName {
			t.Fatalf("fileName(etag1) at size %d = %q, the same as at size 100", size, got)
		}
	}
	if got, content := fileName(origin, keyA, sha, 100, false), fileName(origin, keyA, sha, 100, true); !strings.HasPrefix(got, keyPrefix(keyA)) || got == content {
		t.Fatalf("private fileName(%q) = %q, want a key-prefixed name other than the content spool %q", sha, got, content)
	}
	if a, b := fileName(origin, keyA, sha, 100, true), fileName(origin, keyB, sha, -1, true); a != b {
		t.Fatalf("content spool names differ by key: %q vs %q", a, b)
	}
	if a, b := fileName(origin, keyA, sha, 100, true), fileName(other, keyA, sha, 100, true); a == b {
		t.Fatalf("content spool names collide across origins: %q", a)
	}
	if originPrefix(keyA) == keyPrefix(keyA) {
		t.Fatal("origin and key prefixes share a hash domain")
	}
	if a, b := fileName(origin, keyA, "etag1", 100, true), fileName(origin, keyB, "etag1", 100, true); a == b {
		t.Fatalf("key-prefixed spool names collide across keys: %q", a)
	}

	// Crash leftovers: keyA's private spool, and the content spool a task of keyB wrote without knowing the size.
	private, content := fileName(origin, keyA, "etag1", 100, true), fileName(origin, keyB, sha, -1, true)
	for _, name := range []string{private, content} {
		if err := os.WriteFile(filepath.Join(dir, name), make([]byte, 40), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// keyA resumes keyB's bytes on the same origin and leaves its own private leftover alone; another origin starts its own spool.
	m, err := open(filepath.Join(dir, fileName(origin, keyA, sha, 100, true)), sha, 100)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Length(); got != 40 {
		t.Fatalf("resumed size under another key = %d, want 40", got)
	}
	m2, err := open(filepath.Join(dir, fileName(other, keyA, sha, 100, true)), sha, 100)
	if err != nil {
		t.Fatal(err)
	}
	if got := m2.Length(); got != 0 {
		t.Fatalf("size under another origin = %d, want 0", got)
	}
	want := []string{private, content}
	slices.Sort(want)
	if files := spoolFiles(t, dir); !slices.Equal(files, want) {
		t.Fatalf("spool files after opening the content spool on two origins = %v, want the private leftover and the resumed content spool %v", files, want)
	}
	_ = m.Writer().CloseWithError(nil)
	_ = m2.Writer().CloseWithError(nil)
}

// isContentHash accepts exactly the 40- and 64-hex etags, in any case.
func TestIsContentHash(t *testing.T) {
	hex40, hex64 := strings.Repeat("0a", 20), strings.Repeat("9f", 32)
	for _, tc := range []struct {
		etag string
		want bool
	}{
		{hex40, true},
		{strings.ToUpper(hex40), true},
		{hex64, true},
		{strings.Repeat("aB", 32), true},
		{hex40[:39], false},
		{hex40 + "0", false},
		{hex64[:63], false},
		{hex64 + "0", false},
		{hex40[:39] + "g", false},
		{`"` + hex40 + `"`, false},
		{strings.Repeat("ab", 16), false},
		{"", false},
	} {
		if got := isContentHash(tc.etag); got != tc.want {
			t.Errorf("isContentHash(%q) = %v, want %v", tc.etag, got, tc.want)
		}
	}
}

// Ingest refuses bytes whose digest differs from the expected one without storing anything, otherwise lands the file under its sha256.
func TestIngest(t *testing.T) {
	ctx := context.Background()
	data := make([]byte, 256*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "f.spool")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])

	st := memory.NewStorage()
	cas := newLocalCAS(st, "default")

	if _, err := ingest(ctx, cas, f, strings.Repeat("00", 32)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Ingest with a wrong digest: %v, want ErrCorrupt", err)
	}
	if usage, err := st.Usage(ctx); err != nil || usage != (storage.Usage{}) {
		t.Fatalf("storage after a corrupt ingest = %+v, %v; want empty", usage, err)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	res, err := ingest(ctx, cas, f, digest)
	if err != nil {
		t.Fatal(err)
	}
	if res.SHA256 != digest || res.Size != int64(len(data)) || res.FileHash == "" {
		t.Fatalf("Ingest result = %+v, want sha256 %s, size %d and a file hash", res, digest, len(data))
	}
	rc, err := st.GetReconstructedFile(ctx, "default", sum)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("stored %d bytes, want the %d ingested", len(got), len(data))
	}
}

// newTestSpool returns a Spool over in-memory storage spooling under a fresh temp dir.
func newTestSpool(t *testing.T) (*Spool, storage.Storage) {
	t.Helper()
	st := memory.NewStorage()
	q, err := NewSpool(filepath.Join(t.TempDir(), "spool"), st)
	if err != nil {
		t.Fatal(err)
	}
	return q, st
}

// The writer downloads into the content spool, Finish ingests it, Wait hands back the stored identity, and Release lets the drained spool go.
func TestSpoolWriter(t *testing.T) {
	ctx := context.Background()
	q, st := newTestSpool(t)
	const origin, key = "https://hub.example", "/org/repo/resolve/main/a.bin"
	data := make([]byte, 64*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])

	it, err := q.Accept(ctx, Source{Origin: origin, Key: key, ETag: digest, Size: int64(len(data)), SHA256: digest})
	if err != nil {
		t.Fatal(err)
	}
	if !it.Writer() {
		t.Fatal("first Accept is not the writer")
	}
	if want := filepath.Join(q.dir, fileName(origin, key, digest, int64(len(data)), true)); it.path() != want {
		t.Fatalf("writer path = %q, want the content spool %q", it.path(), want)
	}
	select {
	case <-it.Sized():
	default:
		t.Fatal("Sized is open although the source size is known")
	}
	if it.Size() != int64(len(data)) {
		t.Fatalf("Size = %d, want %d", it.Size(), len(data))
	}
	if _, err := it.Write(data[:1024]); err != nil {
		t.Fatal(err)
	}
	if pos, err := it.Seek(0, io.SeekEnd); err != nil || pos != 1024 {
		t.Fatalf("Seek to end = %d, %v; want 1024", pos, err)
	}
	if _, err := it.Write(data[1024:]); err != nil {
		t.Fatal(err)
	}
	if it.Written() != int64(len(data)) {
		t.Fatalf("Written = %d, want %d", it.Written(), len(data))
	}
	it.Finish(ctx, nil)
	res, err := q.Wait(ctx, it)
	if err != nil {
		t.Fatal(err)
	}
	if res.SHA256 != digest || res.Size != int64(len(data)) || res.FileHash == "" {
		t.Fatalf("Wait = %+v, want sha256 %s, size %d and a file hash", res, digest, len(data))
	}
	rc, err := st.GetReconstructedFile(ctx, "default", sum)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("stored %d bytes, %v; want the %d ingested", len(got), err, len(data))
	}
	it.Release()
	if _, err := os.Stat(it.path()); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("spool after Release: %v, want removed", err)
	}
	q.mu.Lock()
	n := len(q.flights)
	q.mu.Unlock()
	if n != 0 {
		t.Fatalf("ledger holds %d flights after the flight ended", n)
	}
}

// A second Accept for the same content on the same origin follows the writer: it tails the spool as bytes land and settles on the writer's result, judged against its own expectations.
func TestSpoolFollower(t *testing.T) {
	ctx := context.Background()
	const origin = "https://hub.example"
	data := make([]byte, 64*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	size := int64(len(data))

	q, _ := newTestSpool(t)
	w, err := q.Accept(ctx, Source{Origin: origin, Key: "/a", ETag: digest, Size: size, SHA256: digest})
	if err != nil {
		t.Fatal(err)
	}
	f, err := q.Accept(ctx, Source{Origin: origin, Key: "/b", ETag: digest, Size: size, SHA256: digest})
	if err != nil {
		t.Fatal(err)
	}
	if f.Writer() {
		t.Fatal("second Accept for the content became a writer")
	}
	if f.path() != w.path() {
		t.Fatalf("follower path = %q, want the writer's %q", f.path(), w.path())
	}
	if _, err := f.Write([]byte{1}); err == nil {
		t.Fatal("follower Write succeeded")
	}
	if _, err := f.Seek(0, io.SeekEnd); err == nil {
		t.Fatal("follower Seek succeeded")
	}
	f.SetSize(5) // ignored
	if f.Size() != size {
		t.Fatalf("follower SetSize changed the flight size to %d", f.Size())
	}

	rc := f.NewReader(0)
	if rc == nil {
		t.Fatal("follower NewReader returned nil on a live spool")
	}
	tailed := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(rc)
		_ = rc.Close()
		tailed <- b
	}()
	for i := 0; i < len(data); i += 8192 {
		if _, err := w.Write(data[i : i+8192]); err != nil {
			t.Fatal(err)
		}
	}
	w.Finish(ctx, nil)
	if b := <-tailed; !bytes.Equal(b, data) {
		t.Fatalf("follower tail read %d bytes, want %d", len(b), len(data))
	}
	rw, err := q.Wait(ctx, w)
	if err != nil {
		t.Fatal(err)
	}
	rf, err := q.Wait(ctx, f)
	if err != nil {
		t.Fatal(err)
	}
	if rw != rf || rf.SHA256 != digest {
		t.Fatalf("follower result %+v, writer result %+v; want the same stored file", rf, rw)
	}
	f.Release()
	w.Release()
	q.mu.Lock()
	n := len(q.flights)
	q.mu.Unlock()
	if n != 0 {
		t.Fatalf("ledger holds %d flights after the flight ended", n)
	}

	t.Run("sha256 disagreement", func(t *testing.T) {
		q, _ := newTestSpool(t)
		w, err := q.Accept(ctx, Source{Origin: origin, Key: "/a", ETag: digest, Size: size, SHA256: digest})
		if err != nil {
			t.Fatal(err)
		}
		defer w.Release()
		f, err := q.Accept(ctx, Source{Origin: origin, Key: "/b", ETag: digest, Size: size, SHA256: strings.Repeat("00", 32)})
		if err != nil {
			t.Fatal(err)
		}
		defer f.Release()
		if f.Writer() {
			t.Fatal("follower with another sha256 expectation did not follow")
		}
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
		w.Finish(ctx, nil)
		if _, err := q.Wait(ctx, f); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("follower Wait = %v, want ErrCorrupt", err)
		}
		if res, err := q.Wait(ctx, w); err != nil || res.SHA256 != digest {
			t.Fatalf("writer Wait = %+v, %v; want its own success", res, err)
		}
	})

	t.Run("size disagreement with the result", func(t *testing.T) {
		q, _ := newTestSpool(t)
		w, err := q.Accept(ctx, Source{Origin: origin, Key: "/a", ETag: digest, Size: size, SHA256: digest})
		if err != nil {
			t.Fatal(err)
		}
		defer w.Release()
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
		w.Finish(ctx, nil)
		if _, err := q.Wait(ctx, w); err != nil {
			t.Fatal(err)
		}
		other := &Item{s: q, f: w.f, src: Source{Size: size + 1}}
		if _, err := q.Wait(ctx, other); err == nil || !strings.Contains(err.Error(), "size mismatch") {
			t.Fatalf("Wait with another size expectation = %v, want a size mismatch", err)
		}
	})
}

// A newcomer with a known size waits for the leader's size: unknown means it downloads privately beside the leader, a different one is refused, and a newcomer without a size follows regardless.
func TestSpoolAcceptHandshake(t *testing.T) {
	ctx := context.Background()
	const origin = "https://hub.example"
	etag := strings.Repeat("ab", 32)
	q, _ := newTestSpool(t)

	leader, err := q.Accept(ctx, Source{Origin: origin, Key: "/a", ETag: etag, Size: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Release()
	if _, err := leader.Write(make([]byte, 10)); err != nil {
		t.Fatal(err)
	}

	waitCtx, cancel := context.WithCancel(ctx)
	accepted := make(chan error, 1)
	go func() {
		_, err := q.Accept(waitCtx, Source{Origin: origin, Key: "/b", ETag: etag, Size: 100})
		accepted <- err
	}()
	select {
	case err := <-accepted:
		t.Fatalf("Accept returned %v before the leader's size was known", err)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	if err := <-accepted; !errors.Is(err, context.Canceled) {
		t.Fatalf("Accept with a canceled ctx = %v, want context.Canceled", err)
	}

	leader.SetSize(-1) // no early size source: unknown until the leader finishes
	private, err := q.Accept(ctx, Source{Origin: origin, Key: "/b", ETag: etag, Size: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer private.Release()
	if !private.Writer() {
		t.Fatal("newcomer with a known size followed a leader of unknown size")
	}
	if want := filepath.Join(q.dir, fileName(origin, "/b", etag, 100, false)); private.path() != want {
		t.Fatalf("newcomer path = %q, want the private %q", private.path(), want)
	}
	if leader.Written() != 10 {
		t.Fatalf("leader spool after a private open: Written = %d; want its 10 bytes untouched", leader.Written())
	}

	follower, err := q.Accept(ctx, Source{Origin: origin, Key: "/c", ETag: etag, Size: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Release()
	if follower.Writer() || follower.path() != leader.path() {
		t.Fatalf("newcomer without a size: writer=%v path=%q; want a follower of %q", follower.Writer(), follower.path(), leader.path())
	}

	t.Run("size disagreement", func(t *testing.T) {
		q, _ := newTestSpool(t)
		leader, err := q.Accept(ctx, Source{Origin: origin, Key: "/a", ETag: etag, Size: 100})
		if err != nil {
			t.Fatal(err)
		}
		defer leader.Release()
		if _, err := q.Accept(ctx, Source{Origin: origin, Key: "/b", ETag: etag, Size: 200}); err == nil || !strings.Contains(err.Error(), "disagrees") {
			t.Fatalf("Accept with another size = %v, want a disagreement error", err)
		}
	})

	t.Run("private collision", func(t *testing.T) {
		q, _ := newTestSpool(t)
		src := Source{Origin: origin, Key: "/a", ETag: "etag1", Size: 100}
		first, err := q.Accept(ctx, src)
		if err != nil {
			t.Fatal(err)
		}
		defer first.Release()
		if _, err := first.Write(make([]byte, 40)); err != nil {
			t.Fatal(err)
		}
		if _, err := q.Accept(ctx, src); err == nil || !strings.Contains(err.Error(), "already being written") {
			t.Fatalf("second Accept of a private spool = %v, want a collision error", err)
		}
		// Other validators for the key are another spool; the first keeps its progress.
		other, err := q.Accept(ctx, Source{Origin: origin, Key: "/a", ETag: "etag2", Size: 200})
		if err != nil || !other.Writer() || other.path() == first.path() {
			t.Fatalf("Accept of the key under other validators = %+v, %v; want a writer of another spool", other, err)
		}
		other.Release()
		if first.Written() != 40 {
			t.Fatalf("first spool after the other validators opened: Written = %d; want its 40 bytes kept", first.Written())
		}
	})
}

// The same content hash on another origin is another download.
func TestSpoolAcceptOriginScoping(t *testing.T) {
	ctx := context.Background()
	q, _ := newTestSpool(t)
	etag := strings.Repeat("ab", 32)
	a, err := q.Accept(ctx, Source{Origin: "https://a.example", Key: "/k", ETag: etag, Size: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Release()
	b, err := q.Accept(ctx, Source{Origin: "https://b.example", Key: "/k", ETag: etag, Size: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release()
	if !a.Writer() || !b.Writer() || a.path() == b.path() {
		t.Fatalf("origins share a spool: writers %v/%v, paths %q and %q", a.Writer(), b.Writer(), a.path(), b.path())
	}
}

func TestSpoolFinish(t *testing.T) {
	ctx := context.Background()
	const origin, key = "https://hub.example", "/org/repo/resolve/main/f.bin"
	big := make([]byte, 64*1024) // past the memory tier, so the spool has a file to drop

	t.Run("failure drops the spool once released", func(t *testing.T) {
		for _, etag := range []string{"etag1", ""} {
			q, _ := newTestSpool(t)
			src := Source{Origin: origin, Key: key, ETag: etag, Size: -1}
			it, err := q.Accept(ctx, src)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := it.Write(big); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(it.path()); err != nil {
				t.Fatalf("spool of etag %q past the memory tier: %v, want its file", etag, err)
			}
			it.Finish(ctx, errors.New("interrupted"))
			if _, err := q.Wait(ctx, it); err == nil || err.Error() != "interrupted" {
				t.Fatalf("Wait = %v, want the finish error", err)
			}
			it.Release()
			if _, err := os.Stat(it.path()); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("spool of etag %q after a failure: %v, want removed", etag, err)
			}
			again, err := q.Accept(ctx, src)
			if err != nil {
				t.Fatal(err)
			}
			if again.Written() != 0 {
				t.Fatalf("Written after the failed spool of etag %q was dropped = %d, want 0", etag, again.Written())
			}
			again.Release()
		}
	})

	t.Run("crash leftover is resumed", func(t *testing.T) {
		q, _ := newTestSpool(t)
		src := Source{Origin: origin, Key: key, ETag: "etag1", Size: 100}
		if err := os.WriteFile(filepath.Join(q.dir, fileName(origin, key, src.ETag, src.Size, false)), make([]byte, 40), 0o644); err != nil {
			t.Fatal(err)
		}
		it, err := q.Accept(ctx, src)
		if err != nil {
			t.Fatal(err)
		}
		defer it.Release()
		if it.Written() != 40 {
			t.Fatalf("resumed Written = %d, want 40", it.Written())
		}
	})

	t.Run("short download is dropped once released", func(t *testing.T) {
		q, st := newTestSpool(t)
		it, err := q.Accept(ctx, Source{Origin: origin, Key: key, ETag: "etag1", Size: 2 * int64(len(big))})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := it.Write(big); err != nil {
			t.Fatal(err)
		}
		it.Finish(ctx, nil)
		if _, err := q.Wait(ctx, it); err == nil || !strings.Contains(err.Error(), "size mismatch") {
			t.Fatalf("Wait = %v, want a size mismatch", err)
		}
		it.Release()
		if _, err := os.Stat(it.path()); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("short spool: %v, want removed", err)
		}
		if usage, err := st.Usage(ctx); err != nil || usage != (storage.Usage{}) {
			t.Fatalf("storage after a short download = %+v, %v; want nothing ingested", usage, err)
		}
	})

	t.Run("overlong download is removed", func(t *testing.T) {
		q, st := newTestSpool(t)
		it, err := q.Accept(ctx, Source{Origin: origin, Key: key, ETag: "etag1", Size: int64(len(big)) / 2})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := it.Write(big); err != nil {
			t.Fatal(err)
		}
		it.Finish(ctx, nil)
		if _, err := q.Wait(ctx, it); err == nil || !strings.Contains(err.Error(), "size mismatch") {
			t.Fatalf("Wait = %v, want a size mismatch", err)
		}
		it.Release()
		if _, err := os.Stat(it.path()); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("overlong spool: %v, want removed", err)
		}
		if usage, err := st.Usage(ctx); err != nil || usage != (storage.Usage{}) {
			t.Fatalf("storage after an overlong download = %+v, %v; want nothing ingested", usage, err)
		}
	})

	t.Run("corrupt bytes are removed and nothing is stored", func(t *testing.T) {
		q, st := newTestSpool(t)
		data := make([]byte, 64*1024)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}
		claimed := strings.Repeat("00", 32)
		it, err := q.Accept(ctx, Source{Origin: origin, Key: key, ETag: claimed, Size: int64(len(data)), SHA256: claimed})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := it.Write(data); err != nil {
			t.Fatal(err)
		}
		it.Finish(ctx, nil)
		if _, err := q.Wait(ctx, it); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Wait = %v, want ErrCorrupt", err)
		}
		it.Release()
		if _, err := os.Stat(it.path()); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("corrupt spool: %v, want removed", err)
		}
		if usage, err := st.Usage(ctx); err != nil || usage != (storage.Usage{}) {
			t.Fatalf("storage after a corrupt ingest = %+v, %v; want empty", usage, err)
		}
	})

	t.Run("storage failure fails the flight and drops the spool", func(t *testing.T) {
		st := memory.NewStorage()
		q, err := NewSpool(filepath.Join(t.TempDir(), "spool"), xorbRejectingStorage{st})
		if err != nil {
			t.Fatal(err)
		}
		it, err := q.Accept(ctx, Source{Origin: origin, Key: key, ETag: "", Size: -1})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := it.Write(big); err != nil {
			t.Fatal(err)
		}
		it.Finish(ctx, nil)
		if _, err := q.Wait(ctx, it); err == nil || !strings.Contains(err.Error(), "xorb write refused") {
			t.Fatalf("Wait = %v, want the storage failure", err)
		}
		it.Release()
		if _, err := os.Stat(it.path()); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("spool after the storage failure: %v, want removed", err)
		}
		if usage, err := st.Usage(ctx); err != nil || usage != (storage.Usage{}) {
			t.Fatalf("storage after a refused ingest = %+v, %v; want empty", usage, err)
		}
	})

	t.Run("the spool file lives until the last reader lets go", func(t *testing.T) {
		q, _ := newTestSpool(t)
		data := make([]byte, 64*1024)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		digest := hex.EncodeToString(sum[:])
		it, err := q.Accept(ctx, Source{Origin: origin, Key: key, ETag: digest, Size: int64(len(data)), SHA256: digest})
		if err != nil {
			t.Fatal(err)
		}
		rc := it.NewReader(0)
		if rc == nil {
			t.Fatal("NewReader returned nil on a live spool")
		}
		if _, err := it.Write(data); err != nil {
			t.Fatal(err)
		}
		it.Finish(ctx, nil)
		if _, err := q.Wait(ctx, it); err != nil {
			t.Fatal(err)
		}
		it.Release()
		if _, err := os.Stat(it.path()); err != nil {
			t.Fatalf("spool after the ingest and the writer's release, with a reader attached: %v, want kept", err)
		}
		if got, err := io.ReadAll(rc); err != nil || !bytes.Equal(got, data) {
			t.Fatalf("reader after the ingest read %d bytes, %v; want all %d", len(got), err, len(data))
		}
		_ = rc.Close()
		if _, err := os.Stat(it.path()); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("spool after its last reader: %v, want removed", err)
		}
	})
}

// A writer that goes away without finishing fails its flight, so followers settle instead of hanging; Finish and Release are idempotent.
func TestSpoolReleaseBeforeFinish(t *testing.T) {
	ctx := context.Background()
	q, _ := newTestSpool(t)
	const origin = "https://hub.example"
	etag := strings.Repeat("ab", 32)
	w, err := q.Accept(ctx, Source{Origin: origin, Key: "/a", ETag: etag, Size: 100})
	if err != nil {
		t.Fatal(err)
	}
	f, err := q.Accept(ctx, Source{Origin: origin, Key: "/b", ETag: etag, Size: 100})
	if err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() {
		_, err := q.Wait(ctx, f)
		waited <- err
	}()
	w.Release()
	select {
	case err := <-waited:
		if err == nil {
			t.Fatal("follower Wait succeeded after the writer left")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("follower Wait hung after the writer released")
	}
	q.mu.Lock()
	n := len(q.flights)
	q.mu.Unlock()
	if n != 0 {
		t.Fatalf("ledger holds %d flights after the writer released", n)
	}
	w.Finish(ctx, nil)
	w.Release()
	f.Release()
	f.Release()
	if _, err := q.Wait(ctx, w); err == nil {
		t.Fatal("a late Finish overturned the release failure")
	}
}
