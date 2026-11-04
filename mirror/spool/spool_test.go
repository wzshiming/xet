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
	mathrand "math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/ioswmr"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pins"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/memory"
)

// xorbRejectingStorage refuses every xorb write, so an ingest fails only after its spool is complete.
type xorbRejectingStorage struct {
	storage.Storage
}

func (xorbRejectingStorage) PutXorb(context.Context, xet.XorbHash, io.Reader) (bool, error) {
	return false, errors.New("xorb write refused")
}

// countingStorage logs PutXorb and PutShard calls as they return; with hold set, PutXorb first waits for hold to close, ignoring ctx.
type countingStorage struct {
	storage.Storage
	hold    chan struct{}
	lookup  func()             // when set, runs as each GetShardByChunkHash starts
	putXorb func(xet.XorbHash) // when set, runs as each PutXorb starts

	mu     sync.Mutex
	log    []string
	stored int // PutXorb calls that returned nil
	active int // calls not yet returned
}

func (c *countingStorage) PutXorb(ctx context.Context, xorbHash xet.XorbHash, r io.Reader) (bool, error) {
	c.enter()
	if c.putXorb != nil {
		c.putXorb(xorbHash)
	}
	if c.hold != nil {
		<-c.hold
	}
	inserted, err := c.Storage.PutXorb(ctx, xorbHash, r)
	c.exit("PutXorb", err)
	return inserted, err
}

func (c *countingStorage) PutShard(ctx context.Context, sh *shard.Shard) (bool, error) {
	c.enter()
	inserted, err := c.Storage.PutShard(ctx, sh)
	c.exit("PutShard", err)
	return inserted, err
}

func (c *countingStorage) GetShardByChunkHash(ctx context.Context, chunkHash xet.ChunkHash) (*shard.Shard, error) {
	if c.lookup != nil {
		c.lookup()
	}
	return c.Storage.GetShardByChunkHash(ctx, chunkHash)
}

func (c *countingStorage) enter() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active++
}

func (c *countingStorage) exit(call string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active--
	c.log = append(c.log, call)
	if call == "PutXorb" && err == nil {
		c.stored++
	}
}

// state returns the calls returned so far, how many PutXorb among them succeeded, and the calls still running.
func (c *countingStorage) state() (calls string, stored, running int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.log, " "), c.stored, c.active
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
		rc, err := (&Item{f: &flight{swmr: m}}).NewReader(0)
		if err != nil {
			t.Fatal(err)
		}
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
		if _, err := it.NewReader(0); !errors.Is(err, ioswmr.ErrClosedPipe) {
			t.Fatalf("NewReader err = %v, want ioswmr.ErrClosedPipe", err)
		}
		if _, err := it.NewSeekReader(0, 0); !errors.Is(err, ioswmr.ErrClosedPipe) {
			t.Fatalf("NewSeekReader err = %v, want ioswmr.ErrClosedPipe", err)
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

// The ingest refuses bytes whose digest differs from the claim without storing anything, otherwise lands the file under its sha256.
func TestIngest(t *testing.T) {
	ctx := context.Background()
	const origin, key = "https://hub.example", "/org/repo/resolve/main/f.bin"
	data := make([]byte, 256*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	q, st := newTestSpool(t)

	claimed := strings.Repeat("00", 32)
	corrupt, err := q.Accept(ctx, Source{Origin: origin, Key: key, ETag: claimed, Size: int64(len(data)), SHA256: claimed})
	if err != nil {
		t.Fatal(err)
	}
	defer corrupt.Release()
	if _, err := corrupt.Write(data); err != nil {
		t.Fatal(err)
	}
	corrupt.Finish(ctx, nil)
	want := "spool corrupt: sha256 mismatch: got " + digest + ", expected " + claimed
	if _, err := q.Wait(ctx, corrupt); !errors.Is(err, ErrCorrupt) || err.Error() != want {
		t.Fatalf("Wait with a wrong digest = %v, want ErrCorrupt %q", err, want)
	}
	corrupt.Release()
	if _, err := os.Stat(corrupt.path()); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("corrupt spool after Release: %v, want removed", err)
	}
	if usage, err := st.Usage(ctx); err != nil || usage != (storage.Usage{}) {
		t.Fatalf("storage after a corrupt ingest = %+v, %v; want empty", usage, err)
	}

	it, err := q.Accept(ctx, Source{Origin: origin, Key: key, ETag: digest, Size: int64(len(data)), SHA256: digest})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Release()
	if _, err := it.Write(data); err != nil {
		t.Fatal(err)
	}
	it.Finish(ctx, nil)
	res, err := q.Wait(ctx, it)
	if err != nil {
		t.Fatal(err)
	}
	if res.SHA256 != digest || res.Size != int64(len(data)) || res.FileHash == "" {
		t.Fatalf("Wait = %+v, want sha256 %s, size %d and a file hash", res, digest, len(data))
	}
	assertStored(t, st, data)
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

// newCountingSpool returns a Spool over a countingStorage of in-memory storage that holds PutXorb on hold when it is non-nil.
func newCountingSpool(t *testing.T, hold chan struct{}) (*Spool, *countingStorage) {
	t.Helper()
	st := &countingStorage{Storage: memory.NewStorage(), hold: hold}
	q, err := NewSpool(filepath.Join(t.TempDir(), "spool"), st)
	if err != nil {
		t.Fatal(err)
	}
	return q, st
}

// bigSize spans two xorbs; bigHead lies past every chunk of the first.
const (
	bigSize = 70 << 20
	bigHead = 66 << 20
)

// bigData holds bigSize bytes drawn from a fixed seed, generated once per test binary and never modified.
var bigData = sync.OnceValue(func() []byte {
	data := make([]byte, bigSize)
	_, _ = mathrand.NewChaCha8([32]byte{1}).Read(data)
	return data
})

// bigResult is the Result an ingest of bigData settles on, from an independent chunking.
var bigResult = sync.OnceValue(func() Result {
	var hashes []xet.ChunkHash
	var sizes []uint64
	if err := xet.ChunkData(bytes.NewReader(bigData()), func(_ int64, chunk []byte) error {
		hashes = append(hashes, xet.ComputeChunkHash(chunk))
		sizes = append(sizes, uint64(len(chunk)))
		return nil
	}); err != nil {
		panic(err)
	}
	sum := sha256.Sum256(bigData())
	return Result{SHA256: hex.EncodeToString(sum[:]), FileHash: xet.ComputeFileHash(hashes, sizes).String(), Size: bigSize}
})

// testWait bounds a wait on the ingest so a hang fails the test before the binary's deadline.
func testWait(t *testing.T) time.Duration {
	if deadline, ok := t.Deadline(); ok {
		return min(10*time.Minute, time.Until(deadline)*9/10)
	}
	return 10 * time.Minute
}

// waitUntil polls cond until it holds, failing if the ingest ends first or testWait runs out.
func waitUntil(t *testing.T, it *Item, what string, cond func() bool) {
	t.Helper()
	wait := testWait(t)
	deadline := time.After(wait)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !cond() {
		select {
		case <-it.f.ingest.done:
			if !cond() {
				t.Fatalf("no %s: the ingest ended first with %v", what, it.f.ingest.err)
			}
			return
		case <-deadline:
			t.Fatalf("no %s within %v", what, wait)
		case <-tick.C:
		}
	}
}

// writePieces appends b to the writer's spool 1 MiB at a time, as a download lands.
func writePieces(t *testing.T, it *Item, b []byte) {
	t.Helper()
	for piece := range slices.Chunk(b, 1<<20) {
		if _, err := it.Write(piece); err != nil {
			t.Fatal(err)
		}
	}
}

// feedUntilIngestEnds writes until the ingest ended: PutFile sees a cancel only between chunks.
func feedUntilIngestEnds(t *testing.T, it *Item) {
	t.Helper()
	wait := testWait(t)
	deadline := time.After(wait)
	for {
		select {
		case <-it.f.ingest.done:
			return
		case <-deadline:
			t.Fatalf("the ingest did not end within %v while the writer kept writing", wait)
		case <-time.After(10 * time.Millisecond):
			writePieces(t, it, bigData()[:1<<20])
		}
	}
}

// assertStored checks st reconstructs data under its sha256.
func assertStored(t *testing.T, st storage.Storage, data []byte) {
	t.Helper()
	rc, err := st.GetReconstructedFile(context.Background(), sha256.Sum256(data))
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("stored %d bytes, %v; want the %d ingested", len(got), err, len(data))
	}
}

// registered reports whether q's ledger holds the Item's flight.
func registered(q *Spool, it *Item) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.flights[it.path()] == it.f
}

// The writer downloads into the content spool as its ingest tails it, Finish settles the flight, Wait hands back the stored identity, and Release lets the drained spool go.
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
	rc, err := st.GetReconstructedFile(ctx, sum)
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

	rc, err := f.NewReader(0)
	if err != nil {
		t.Fatalf("follower NewReader on a live spool: %v", err)
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
		rc, err := it.NewReader(0)
		if err != nil {
			t.Fatalf("NewReader on a live spool: %v", err)
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

// The ingest runs while the writer downloads: an xorb is stored before Finish, the rest of the file and its shard by the time Wait returns.
func TestSpoolIngestsWhileWriting(t *testing.T) {
	ctx := context.Background()
	data, want := bigData(), bigResult()
	q, st := newCountingSpool(t, nil)
	it, err := q.Accept(ctx, Source{Origin: "https://hub.example", Key: "/org/repo/resolve/main/big.bin", ETag: want.SHA256, Size: want.Size, SHA256: want.SHA256})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Release()
	writePieces(t, it, data[:bigHead])
	waitUntil(t, it, "xorb stored before Finish", func() bool {
		_, stored, _ := st.state()
		return stored > 0
	})
	writePieces(t, it, data[bigHead:])
	it.Finish(ctx, nil)
	if res, err := q.Wait(ctx, it); err != nil || res != want {
		t.Fatalf("Wait = %+v, %v; want %+v", res, err, want)
	}
	if calls, _, _ := st.state(); calls != "PutXorb PutXorb PutShard" {
		t.Fatalf("storage calls %q, want two PutXorb then one PutShard", calls)
	}
	assertStored(t, st, data)
}

// The ingest pins each xorb in the spool's pins before storing it and keeps the stored ones pinned while the file is still being stored; Wait returns with every pin released.
func TestSpoolIngestPinsXorbs(t *testing.T) {
	ctx := context.Background()
	data, want := bigData(), bigResult()
	table := pins.New[xet.XorbHash]()
	var (
		mu       sync.Mutex
		entered  []xet.XorbHash // each PutXorb's xorb, in call order
		unpinned []xet.XorbHash // those not pinned as their PutXorb started
	)
	second, hold := make(chan struct{}), make(chan struct{})
	st := &countingStorage{Storage: memory.NewStorage(), putXorb: func(h xet.XorbHash) {
		mu.Lock()
		entered = append(entered, h)
		if !table.Pinned(h) {
			unpinned = append(unpinned, h)
		}
		n := len(entered)
		mu.Unlock()
		if n == 2 {
			close(second)
			<-hold
		}
	}}
	q, err := NewSpool(filepath.Join(t.TempDir(), "spool"), st, WithPins(table))
	if err != nil {
		t.Fatal(err)
	}
	it, err := q.Accept(ctx, Source{Origin: "https://hub.example", Key: "/org/repo/resolve/main/big.bin", ETag: want.SHA256, Size: want.Size, SHA256: want.SHA256})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Release()
	release := sync.OnceFunc(func() { close(hold) })
	defer release() // before the Release, which awaits a held ingest
	writePieces(t, it, data)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		it.Finish(ctx, nil)
	}()
	wait := testWait(t)
	select {
	case <-second:
	case <-finished:
		_, err := q.Wait(ctx, it)
		t.Fatalf("the ingest ended before its second PutXorb: %v", err)
	case <-time.After(wait):
		t.Fatalf("the ingest made no second PutXorb within %v", wait)
	}

	mu.Lock()
	hashes, unpinnedAtEntry := slices.Clone(entered), slices.Clone(unpinned)
	mu.Unlock()
	if len(unpinnedAtEntry) > 0 {
		t.Fatalf("PutXorb started with xorbs %v unpinned, want each pinned in the spool's pins before it is stored", unpinnedAtEntry)
	}
	for _, h := range hashes {
		if !table.Pinned(h) {
			t.Fatalf("xorb %s unpinned while the file's second xorb is being stored, want every xorb of the in-flight ingest pinned", h)
		}
	}
	release()
	select {
	case <-finished:
	case <-time.After(wait):
		t.Fatalf("Finish did not return within %v after the second PutXorb was released", wait)
	}
	if res, err := q.Wait(ctx, it); err != nil || res != want {
		t.Fatalf("Wait = %+v, %v; want %+v", res, err, want)
	}
	for _, h := range hashes {
		if table.Pinned(h) {
			t.Fatalf("xorb %s still pinned once Wait returned, want every pin released with the ingest", h)
		}
	}
}

// An adopted crash leftover is ingested from its first byte: a complete one stores an xorb before Finish is called, a partial one lands with the bytes appended to it.
func TestSpoolIngestsAdoptedLeftover(t *testing.T) {
	ctx := context.Background()
	const origin, key = "https://hub.example", "/org/repo/resolve/main/big.bin"

	t.Run("complete leftover", func(t *testing.T) {
		data, want := bigData(), bigResult()
		q, st := newCountingSpool(t, nil)
		if err := os.WriteFile(filepath.Join(q.dir, fileName(origin, key, want.SHA256, want.Size, true)), data, 0o644); err != nil {
			t.Fatal(err)
		}
		it, err := q.Accept(ctx, Source{Origin: origin, Key: key, ETag: want.SHA256, Size: want.Size, SHA256: want.SHA256})
		if err != nil {
			t.Fatal(err)
		}
		defer it.Release()
		if it.Written() != want.Size {
			t.Fatalf("adopted Written = %d, want the leftover's %d", it.Written(), want.Size)
		}
		waitUntil(t, it, "xorb of the adopted leftover stored before Finish", func() bool {
			_, stored, _ := st.state()
			return stored > 0
		})
		it.Finish(ctx, nil)
		if res, err := q.Wait(ctx, it); err != nil || res != want {
			t.Fatalf("Wait = %+v, %v; want %+v", res, err, want)
		}
		assertStored(t, st, data)
	})

	t.Run("partial leftover resumed", func(t *testing.T) {
		data := make([]byte, 256*1024)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		digest := hex.EncodeToString(sum[:])
		const n = 100 * 1024
		q, st := newTestSpool(t)
		if err := os.WriteFile(filepath.Join(q.dir, fileName(origin, key, digest, int64(len(data)), true)), data[:n], 0o644); err != nil {
			t.Fatal(err)
		}
		it, err := q.Accept(ctx, Source{Origin: origin, Key: key, ETag: digest, Size: int64(len(data)), SHA256: digest})
		if err != nil {
			t.Fatal(err)
		}
		defer it.Release()
		if it.Written() != n {
			t.Fatalf("adopted Written = %d, want the leftover's %d", it.Written(), n)
		}
		if _, err := it.Write(data[n:]); err != nil {
			t.Fatal(err)
		}
		it.Finish(ctx, nil)
		if res, err := q.Wait(ctx, it); err != nil || res.SHA256 != digest || res.Size != int64(len(data)) || res.FileHash == "" {
			t.Fatalf("Wait = %+v, %v; want sha256 %s, size %d and a file hash", res, err, digest, len(data))
		}
		assertStored(t, st, data)
		if usage, err := st.Usage(ctx); err != nil || usage.Shards.Count != 1 {
			t.Fatalf("storage after the resumed ingest = %+v, %v; want one shard", usage, err)
		}
	})
}

// An abort while the ingest is inside a PutXorb returns only once that call returned, the flight in the ledger until then; nothing reaches storage afterwards, and writer and follower settle on the abort.
func TestSpoolAbortAwaitsIngest(t *testing.T) {
	ctx := context.Background()
	const origin = "https://hub.example"
	etag := strings.Repeat("ab", 32)
	errAbort := errors.New("download aborted")
	for _, tc := range []struct {
		name  string
		abort func(*Item)
		want  string // the flight's error text
	}{
		{"finish with an error", func(w *Item) { w.Finish(ctx, errAbort) }, errAbort.Error()},
		{"release before finish", (*Item).Release, "spool: writer released before finish"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hold := make(chan struct{})
			q, st := newCountingSpool(t, hold)
			w, err := q.Accept(ctx, Source{Origin: origin, Key: "/a", ETag: etag, Size: bigSize})
			if err != nil {
				t.Fatal(err)
			}
			defer w.Release()
			f, err := q.Accept(ctx, Source{Origin: origin, Key: "/b", ETag: etag, Size: bigSize})
			if err != nil {
				t.Fatal(err)
			}
			defer f.Release()
			release := sync.OnceFunc(func() { close(hold) })
			defer release() // before the Releases, which await a held ingest
			if f.Writer() {
				t.Fatal("second Accept for the content did not follow the writer")
			}
			writePieces(t, w, bigData()[:bigHead])
			waitUntil(t, w, "PutXorb held while the writer downloads", func() bool {
				_, _, running := st.state()
				return running > 0
			})

			aborted := make(chan int, 1)
			go func() {
				tc.abort(w)
				_, _, running := st.state()
				aborted <- running
			}()
			select {
			case <-aborted:
				t.Fatal("abort returned while its ingest's PutXorb was still held")
			case <-time.After(2 * time.Second):
			}
			if !registered(q, w) {
				t.Fatal("the ledger lacks the writer's flight while the abort awaits its ingest's held PutXorb")
			}
			release()
			select {
			case running := <-aborted:
				if running != 0 {
					t.Fatalf("abort returned with %d storage calls still running", running)
				}
			case <-time.After(testWait(t)):
				t.Fatal("abort did not return after its held PutXorb was released")
			}
			if registered(q, w) {
				t.Fatal("the ledger holds the writer's flight once the abort returned")
			}
			for _, it := range []*Item{w, f} {
				if _, err := q.Wait(ctx, it); err == nil || err.Error() != tc.want {
					t.Fatalf("Wait (writer %v) = %v, want %q", it.Writer(), err, tc.want)
				}
			}
			w.Release()
			f.Release()
			if _, err := os.Stat(w.path()); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("spool after both Releases: %v, want removed", err)
			}
			if calls, _, _ := st.state(); calls != "PutXorb" {
				t.Fatalf("storage calls %q, want only the held PutXorb", calls)
			}
		})
	}
}

// Finish whose ctx expires while the ingest is inside a PutXorb cancels the ingest and returns only once that call returned: no shard is stored and the flight fails with the ctx's error.
func TestSpoolFinishDeadlineAwaitsIngest(t *testing.T) {
	ctx := context.Background()
	hold := make(chan struct{})
	q, st := newCountingSpool(t, hold)
	it, err := q.Accept(ctx, Source{Origin: "https://hub.example", Key: "/a", ETag: strings.Repeat("ab", 32), Size: bigSize})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Release()
	release := sync.OnceFunc(func() { close(hold) })
	defer release() // before the Release, which awaits a held ingest
	writePieces(t, it, bigData())
	waitUntil(t, it, "PutXorb held while the writer downloads", func() bool {
		_, _, running := st.state()
		return running > 0
	})

	finishCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	finished := make(chan int, 1)
	go func() {
		it.Finish(finishCtx, nil)
		_, _, running := st.state()
		finished <- running
	}()
	select {
	case <-finished:
		t.Fatal("Finish returned while its ingest's PutXorb was still held")
	case <-time.After(2 * time.Second):
	}
	release()
	select {
	case running := <-finished:
		if running != 0 {
			t.Fatalf("Finish returned with %d storage calls still running", running)
		}
	case <-time.After(testWait(t)):
		t.Fatal("Finish did not return after its held PutXorb was released")
	}
	if _, err := q.Wait(ctx, it); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait = %v, want Finish's context.DeadlineExceeded", err)
	}
	if calls, _, _ := st.state(); strings.Contains(calls, "PutShard") {
		t.Fatalf("storage calls %q after Finish's ctx expired, want no shard", calls)
	}
	it.Release()
	if registered(q, it) {
		t.Fatal("the ledger holds the failed flight after it was released")
	}
}

// A writer released before Finish with no follower keeps its spool while its ingest is inside a PutXorb, and drops it once that call returned.
func TestSpoolLoneWriterReleaseKeepsSpool(t *testing.T) {
	ctx := context.Background()
	hold := make(chan struct{})
	q, st := newCountingSpool(t, hold)
	w, err := q.Accept(ctx, Source{Origin: "https://hub.example", Key: "/a", ETag: strings.Repeat("ab", 32), Size: bigSize})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Release()
	release := sync.OnceFunc(func() { close(hold) })
	defer release() // before the Release, which awaits a held ingest
	// Spooled to the end of the chunk that closes the first xorb, the tail waits at the spool's end while that xorb is held.
	end := 0
	if err := xet.ChunkData(bytes.NewReader(bigData()), func(off int64, chunk []byte) error {
		if end <= xet.MaxXorbSize {
			end = int(off) + len(chunk)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	writePieces(t, w, bigData()[:end])
	waitUntil(t, w, "PutXorb held while the writer downloads", func() bool {
		_, _, running := st.state()
		return running > 0
	})

	released := make(chan int, 1)
	go func() {
		w.Release()
		_, _, running := st.state()
		released <- running
	}()
	select {
	case <-released:
		t.Fatal("Release returned while its ingest's PutXorb was still held")
	case <-time.After(2 * time.Second):
	}
	if _, err := os.Stat(w.path()); err != nil {
		t.Fatalf("spool while Release awaits its ingest's held PutXorb: %v, want kept", err)
	}
	release()
	select {
	case running := <-released:
		if running != 0 {
			t.Fatalf("Release returned with %d storage calls still running", running)
		}
	case <-time.After(testWait(t)):
		t.Fatal("Release did not return after its held PutXorb was released")
	}
	if _, err := os.Stat(w.path()); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("spool after Release: %v, want removed", err)
	}
}

// An ingest that fails mid-stream closes its tail reader on return, leaving only the writer's hold on the spool.
func TestSpoolFailedIngestClosesTail(t *testing.T) {
	ctx := context.Background()
	const origin, key = "https://hub.example", "/org/repo/resolve/main/big.bin"
	etag := strings.Repeat("ab", 32)
	q, err := NewSpool(filepath.Join(t.TempDir(), "spool"), xorbRejectingStorage{memory.NewStorage()})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(q.dir, fileName(origin, key, etag, bigSize, true)), bigData(), 0o644); err != nil {
		t.Fatal(err)
	}
	it, err := q.Accept(ctx, Source{Origin: origin, Key: key, ETag: etag, Size: bigSize})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Release()
	feedUntilIngestEnds(t, it)
	if n := it.f.swmr.ReaderUsing(); n != 1 {
		t.Fatalf("readers holding the spool after its ingest failed = %d, want only the writer's hold", n)
	}
}

// An ingest that already ended settles the flight on its own outcome even when Finish's ctx is done too.
func TestSpoolFinishAfterIngestEnded(t *testing.T) {
	ctx := context.Background()
	q, err := NewSpool(filepath.Join(t.TempDir(), "spool"), xorbRejectingStorage{memory.NewStorage()})
	if err != nil {
		t.Fatal(err)
	}
	it, err := q.Accept(ctx, Source{Origin: "https://hub.example", Key: "/org/repo/resolve/main/big.bin", Size: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Release()
	writePieces(t, it, bigData())
	feedUntilIngestEnds(t, it)
	done, cancel := context.WithCancel(ctx)
	cancel()
	it.Finish(done, nil)
	if _, err := q.Wait(ctx, it); err == nil || errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "xorb write refused") {
		t.Fatalf("Wait = %v, want the ingest's storage failure, not Finish's canceled ctx", err)
	}
}

// Accept registers the writer's flight before starting its ingest, so a storing flight is always in the ledger: a happens-before pin, as the reverse order fails it only under adversarial scheduling.
func TestSpoolRegistersFlightBeforeIngest(t *testing.T) {
	ctx := context.Background()
	const origin, key = "https://hub.example", "/org/repo/resolve/main/big.bin"
	etag := strings.Repeat("ab", 32)
	q, st := newCountingSpool(t, nil)
	path := filepath.Join(q.dir, fileName(origin, key, etag, bigSize, true))
	var (
		probed       = make(chan struct{})
		probedFlight *flight
	)
	st.lookup = sync.OnceFunc(func() {
		q.mu.Lock()
		probedFlight = q.flights[path]
		q.mu.Unlock()
		close(probed)
	})
	// An adopted leftover past chunk 0 lets the ingest probe storage for it before any Write.
	if err := os.WriteFile(path, bigData()[:1<<20], 0o644); err != nil {
		t.Fatal(err)
	}
	it, err := q.Accept(ctx, Source{Origin: origin, Key: key, ETag: etag, Size: bigSize})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Release()
	wait := testWait(t)
	select {
	case <-probed:
	case <-time.After(wait):
		t.Fatalf("the ingest made no storage call within %v", wait)
	}
	if probedFlight != it.f {
		t.Fatalf("the ledger's flight for %s at the ingest's first storage call = %p, want the writer's %p", path, probedFlight, it.f)
	}
}

// An empty writer flight settles on the empty file's identity without a storage call.
func TestSpoolEmptyFlight(t *testing.T) {
	ctx := context.Background()
	q, st := newCountingSpool(t, nil)
	sum := sha256.Sum256(nil)
	digest := hex.EncodeToString(sum[:])
	it, err := q.Accept(ctx, Source{Origin: "https://hub.example", Key: "/org/repo/resolve/main/empty.bin", ETag: digest, Size: 0, SHA256: digest})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Release()
	it.Finish(ctx, nil)
	if res, err := q.Wait(ctx, it); err != nil || res != (Result{SHA256: digest}) {
		t.Fatalf("Wait = %+v, %v; want %+v", res, err, Result{SHA256: digest})
	}
	if calls, _, _ := st.state(); calls != "" {
		t.Fatalf("storage calls %q for an empty flight, want none", calls)
	}
}

// A writer flight without a sha256 claim settles on the sha256 and file hash its ingest computed.
func TestSpoolUnclaimedFlight(t *testing.T) {
	ctx := context.Background()
	q, st := newTestSpool(t)
	data := make([]byte, 64*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	// A regular hub file's etag is its git blob sha1, which claims no sha256.
	it, err := q.Accept(ctx, Source{Origin: "https://hub.example", Key: "/org/repo/resolve/main/config.json", ETag: strings.Repeat("cd", 20), Size: int64(len(data))})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Release()
	if _, err := it.Write(data); err != nil {
		t.Fatal(err)
	}
	it.Finish(ctx, nil)
	if res, err := q.Wait(ctx, it); err != nil || res.SHA256 != digest || res.Size != int64(len(data)) || res.FileHash == "" {
		t.Fatalf("Wait = %+v, %v; want sha256 %s, size %d and a file hash", res, err, digest, len(data))
	}
	assertStored(t, st, data)
}
