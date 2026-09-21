package local

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage/storagetest"
	"github.com/wzshiming/xet/xorb"
)

// TestShardNameIsDeterministicContentHash proves the stored object name is the
// SHA-256 of the exact stored bytes and does not vary with the creation time
// embedded in the (unstored) footer.
func TestShardNameIsDeterministicContentHash(t *testing.T) {
	newIdenticalShard := func(creationTime uint64) *shard.Shard {
		s := shard.NewShard()
		s.AddFile(shard.FileBlock{FileHash: xet.FileHash{}})
		s.AddCASBlock(shard.CASBlock{
			CASHash: xet.XorbHash{2},
			Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: xet.ChunkHash{3}}},
		})
		s.SetFooter(time.Unix(int64(creationTime), 0))
		return s
	}

	var names []string
	for _, creationTime := range []uint64{1, 1 << 30} {
		basePath := t.TempDir()
		st, err := NewStorage(WithBasePath(basePath))
		if err != nil {
			t.Fatal(err)
		}
		if inserted, err := st.PutShard(context.Background(), newIdenticalShard(creationTime)); err != nil || !inserted {
			t.Fatalf("PutShard() = %v, %v", inserted, err)
		}
		entries, err := fanoutEntries(filepath.Join(basePath, "shards"))
		if err != nil {
			t.Fatalf("read shards directory: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("shards directory contains %d entries, want 1", len(entries))
		}
		name := entries[0]
		data, err := os.ReadFile(st.objectPath("shards", name))
		if err != nil {
			t.Fatalf("read stored shard: %v", err)
		}
		if footerSize := binary.LittleEndian.Uint64(data[40:48]); footerSize != 0 {
			t.Fatalf("stored FooterSize = %d, want 0", footerSize)
		}
		sum := sha256.Sum256(data)
		if want := hex.EncodeToString(sum[:]); name != want {
			t.Fatalf("shard name %q != sha256 of stored bytes %q", name, want)
		}
		names = append(names, name)
	}
	if names[0] != names[1] {
		t.Fatalf("identical content produced different names: %q vs %q", names[0], names[1])
	}
}

// TestFooteredShardObjectStaysReadable covers objects written before shards
// went footerless: they must keep resolving, with their stored footer intact.
func TestFooteredShardObjectStaysReadable(t *testing.T) {
	basePath := t.TempDir()
	fs, err := NewStorage(WithBasePath(basePath))
	if err != nil {
		t.Fatal(err)
	}

	fileHash := xet.FileHash{7}
	s := shard.NewShard()
	s.AddFile(shard.FileBlock{FileHash: fileHash})
	s.AddCASBlock(shard.CASBlock{
		CASHash: xet.XorbHash{8},
		Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: xet.ChunkHash{9}}},
	})
	const creationTime = 1700000000
	data, name := storagetest.LegacyShardBytes(t, s, creationTime)

	shardPath := fs.objectPath("shards", name)
	if err := os.MkdirAll(filepath.Dir(shardPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(shardPath, data, 0644); err != nil {
		t.Fatal(err)
	}
	if err := writeIndexFile(fs.objectPath("index/files", fileHash.String()), []byte(name)); err != nil {
		t.Fatal(err)
	}

	loaded, err := fs.GetShard(context.Background(), fileHash)
	if err != nil {
		t.Fatalf("GetShard on a footered object: %v", err)
	}
	if loaded.Files[0].FileHash != fileHash {
		t.Fatal("loaded the wrong shard")
	}
	if loaded.Footer == nil || loaded.Footer.ShardCreationTimestamp != creationTime {
		t.Fatalf("stored footer was not preserved: %+v", loaded.Footer)
	}
}

// TestPutShardRetryAfterPartialFailure kills PutShard midway through its index
// writes and verifies a retry fully repairs the shard instead of reporting
// "already exists" with indexes missing.
func TestPutShardRetryAfterPartialFailure(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	fs, err := NewStorage(WithBasePath(basePath))
	if err != nil {
		t.Fatal(err)
	}

	content := []byte("retry me")
	shardObj := shard.NewShard()
	fileHash, _, _ := storagetest.AddFileBlock(t, ctx, fs, shardObj, [][]byte{content})

	// First attempt dies while writing SHA-256 indexes: a regular file where
	// the index/sha256 directory belongs makes those writes fail.
	blocker := filepath.Join(basePath, "index", "sha256")
	if err := os.RemoveAll(blocker); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blocker, nil, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.PutShard(ctx, shardObj); err == nil {
		t.Fatal("PutShard() succeeded despite blocked sha256 index")
	}

	// The partial shard must not count as existing.
	if exists, err := fs.hasFile(fileHash); err != nil || exists {
		t.Fatalf("hasFile() after partial failure = %v, %v", exists, err)
	}

	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.PutShard(ctx, shardObj); err != nil {
		t.Fatalf("PutShard() retry: %v", err)
	}

	// A fresh storage must resolve every index written by the retry.
	fresh, err := NewStorage(WithBasePath(basePath))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(content)
	gotFileHash, err := fresh.GetFileHashBySHA256(ctx, "default", digest)
	if err != nil {
		t.Fatalf("GetFileHashBySHA256 after retry: %v", err)
	}
	if gotFileHash != fileHash {
		t.Fatal("SHA-256 index resolved wrong file hash")
	}
	if _, err := fresh.GetShard(ctx, fileHash); err != nil {
		t.Fatalf("GetShard after retry: %v", err)
	}
	if _, err := fresh.GetShardByChunkHash(ctx, "default", xet.ComputeChunkHash(content)); err != nil {
		t.Fatalf("GetShardByChunkHash after retry: %v", err)
	}
}

// fanoutEntries returns the hash names reassembled from a fanout directory.
func fanoutEntries(dir string) ([]string, error) {
	var names []string
	err := filepath.WalkDir(dir, func(path string, d iofs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		// Every object must sit at exactly <2>/<2>/<rest>.
		if len(parts) != 3 || len(parts[0]) != 2 || len(parts[1]) != 2 {
			return fmt.Errorf("unexpected entry %q", rel)
		}
		names = append(names, strings.Join(parts, ""))
		return nil
	})
	return names, err
}

// TestFileStorageTwoLevelFanoutLayout pins the on-disk layout to
// <kind>/<hash[:2]>/<hash[2:4]>/<hash[4:]> for every hash-named object and
// proves a store reopened over that layout resolves and enumerates full hashes.
func TestFileStorageTwoLevelFanoutLayout(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	fs, err := NewStorage(WithBasePath(basePath))
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("fan me out")
	shardObj := shard.NewShard()
	fileHash, _, _ := storagetest.AddFileBlock(t, ctx, fs, shardObj, [][]byte{content})
	if inserted, err := fs.PutShard(ctx, shardObj); err != nil || !inserted {
		t.Fatalf("PutShard() = %v, %v", inserted, err)
	}

	fresh, err := NewStorage(WithBasePath(basePath))
	if err != nil {
		t.Fatal(err)
	}
	want := storagetest.CheckFanoutStore(t, ctx, fresh, shardObj, fileHash, content)
	wantPaths := make(map[string]bool, len(want))
	for kind, hash := range want {
		wantPaths[filepath.Join(kind, hash[:2], hash[2:4], hash[4:])] = true
	}
	if err := filepath.WalkDir(basePath, func(entryPath string, entry iofs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(basePath, entryPath)
		if err != nil {
			return err
		}
		if !wantPaths[relative] {
			t.Errorf("unexpected stored file: %s", relative)
		}
		delete(wantPaths, relative)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for entryPath := range wantPaths {
		t.Errorf("missing stored file: %s", entryPath)
	}
}

func TestGetXorbDataRangeFromOffsets(t *testing.T) {
	chunks := [][]byte{
		bytes.Repeat([]byte{1}, 1000),
		[]byte("second chunk"),
		bytes.Repeat([]byte("abc"), 700),
	}
	numChunks := uint32(len(chunks))

	// Reference ranges scanned from the chunk-data region, which is identical
	// for footer and chunk-only formats.
	reference, _ := storagetest.EncodeXorb(t, false, chunks...)

	for _, format := range []struct {
		name       string
		withFooter bool
	}{
		{"footer", true},
		{"chunk-only", false},
	} {
		t.Run(format.name, func(t *testing.T) {
			fs, err := NewStorage(WithBasePath(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			encoded, xorbHash := storagetest.EncodeXorb(t, format.withFooter, chunks...)
			if _, err := fs.PutXorb(context.Background(), "default", xorbHash, bytes.NewReader(encoded)); err != nil {
				t.Fatal(err)
			}

			for start := range numChunks {
				for end := start + 1; end <= numChunks; end++ {
					wantStart, wantEnd, err := xorb.ChunkDataRange(bytes.NewReader(reference), start, end)
					if err != nil {
						t.Fatalf("ChunkDataRange(%d, %d): %v", start, end, err)
					}
					gotStart, gotEnd, err := fs.GetXorbDataRange(context.Background(), "default", xorbHash, start, end)
					if err != nil {
						t.Fatalf("GetXorbDataRange(%d, %d): %v", start, end, err)
					}
					if gotStart != wantStart || gotEnd != wantEnd {
						t.Fatalf("range [%d, %d) = [%d, %d], want [%d, %d]", start, end, gotStart, gotEnd, wantStart, wantEnd)
					}
				}
			}

			if _, _, err := fs.GetXorbDataRange(context.Background(), "default", xorbHash, 0, numChunks+1); err == nil {
				t.Fatal("GetXorbDataRange() accepted out-of-bounds chunk range")
			}

			// Cached offsets keep serving ranges without touching the xorb file.
			if err := os.Remove(fs.objectPath("xorbs", xorbHash.String())); err != nil {
				t.Fatal(err)
			}
			if _, _, err := fs.GetXorbDataRange(context.Background(), "default", xorbHash, 1, 2); err != nil {
				t.Fatalf("GetXorbDataRange() after xorb removal: %v", err)
			}
		})
	}
}

// TestOverwriteIndexFileConcurrentSameKey races writers over the same keys:
// each writer renames its own unique temp file, so no rename can steal
// another writer's temp (the old fixed "<path>.tmp" scheme failed with
// ENOENT) and every key ends up holding one of the written values.
func TestOverwriteIndexFileConcurrentSameKey(t *testing.T) {
	dir := t.TempDir()
	keys := []string{
		filepath.Join(dir, "aa", "key-one"),
		filepath.Join(dir, "ab", "key-two"),
	}
	const writers = 20
	const rounds = 50

	valid := map[string]bool{}
	for i := range writers {
		valid[fmt.Sprintf("shard-%02d", i)] = true
	}

	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Go(func() {
			value := []byte(fmt.Sprintf("shard-%02d", i))
			for r := range rounds {
				if err := overwriteIndexFile(keys[(i+r)%len(keys)], value); err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("overwriteIndexFile: %v", err)
	}

	for _, key := range keys {
		data, err := os.ReadFile(key)
		if err != nil {
			t.Fatal(err)
		}
		if !valid[string(data)] {
			t.Fatalf("key %s holds %q, not one of the written values", key, data)
		}
	}
}

// stepReader feeds PutXorb its input one step at a time. A Read that finds
// nothing buffered first signals entered, proving the previous step was
// fully consumed and teed to disk, then blocks until the test sends the next
// step; a closed steps channel reads as EOF.
type stepReader struct {
	steps   chan readStep
	entered chan struct{}
	buf     []byte
	err     error
}

type readStep struct {
	data []byte
	err  error
}

func newStepReader() *stepReader {
	return &stepReader{steps: make(chan readStep), entered: make(chan struct{}, 4)}
}

func (r *stepReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 && r.err == nil {
		r.entered <- struct{}{}
		step, ok := <-r.steps
		if !ok {
			return 0, io.EOF
		}
		r.buf, r.err = step.data, step.err
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	if len(r.buf) == 0 {
		return n, r.err
	}
	return n, nil
}

// putXorbAsync runs PutXorb in the background and reports its error.
func putXorbAsync(fs *Storage, xorbHash xet.XorbHash, r io.Reader) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := fs.PutXorb(context.Background(), "default", xorbHash, r)
		done <- err
	}()
	return done
}

// await receives from ch, failing instead of hanging if a writer stalls.
func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting on PutXorb")
	}
	var zero T
	return zero
}

// assertStoredXorb checks the committed object holds exactly encoded and
// still validates against its hash.
func assertStoredXorb(t *testing.T, fs *Storage, xorbHash xet.XorbHash, encoded []byte) {
	t.Helper()
	stored, err := os.ReadFile(fs.objectPath("xorbs", xorbHash.String()))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, encoded) {
		t.Fatalf("stored xorb differs from the uploaded bytes (%d vs %d bytes)", len(stored), len(encoded))
	}
	if err := xorb.Validate(bytes.NewReader(stored), xorbHash); err != nil {
		t.Fatalf("stored xorb does not validate: %v", err)
	}
}

// assertNoXorbTemp checks the committed object is the only entry left in
// its directory.
func assertNoXorbTemp(t *testing.T, fs *Storage, xorbHash xet.XorbHash) {
	t.Helper()
	xorbPath := fs.objectPath("xorbs", xorbHash.String())
	entries, err := os.ReadDir(filepath.Dir(xorbPath))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	if len(names) != 1 || names[0] != filepath.Base(xorbPath) {
		t.Fatalf("xorb directory holds %q, want only %q", names, filepath.Base(xorbPath))
	}
}

// TestPutXorbInterruptedConcurrentWriterKeepsCommittedBytes lets a second
// upload of the same xorb open its output after the first has written a
// prefix, finishes the first while the second stalls, then disconnects the
// second. The committed object must be the first writer's complete stream:
// sharing "<path>.tmp" let the second open truncate the prefix, so the first
// published a hole-filled file its stream validation never saw.
func TestPutXorbInterruptedConcurrentWriterKeepsCommittedBytes(t *testing.T) {
	fs, err := NewStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	encoded, xorbHash := storagetest.EncodeXorb(t, true, bytes.Repeat([]byte{1}, 1000), []byte("second chunk"))
	split := len(encoded) / 2

	first := newStepReader()
	firstDone := putXorbAsync(fs, xorbHash, first)
	await(t, first.entered)
	first.steps <- readStep{data: encoded[:split]}
	await(t, first.entered) // prefix consumed and teed to disk

	second := newStepReader()
	secondDone := putXorbAsync(fs, xorbHash, second)
	await(t, second.entered) // second writer has opened its output

	first.steps <- readStep{data: encoded[split:]}
	if err := await(t, firstDone); err != nil {
		t.Fatalf("first PutXorb: %v", err)
	}
	assertStoredXorb(t, fs, xorbHash, encoded)

	second.steps <- readStep{err: errors.New("client disconnected")}
	if err := await(t, secondDone); err == nil {
		t.Fatal("second PutXorb succeeded despite a disconnected client")
	}
	assertStoredXorb(t, fs, xorbHash, encoded)
	assertNoXorbTemp(t, fs, xorbHash)
}

// TestPutXorbConcurrentWritersBothPublish lets two uploads of the same xorb
// open their outputs before either finishes, then completes them one after
// the other. Each must publish its own validated file instead of failing on
// a temp the other writer already renamed away, leaving one committed object.
func TestPutXorbConcurrentWritersBothPublish(t *testing.T) {
	fs, err := NewStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	encoded, xorbHash := storagetest.EncodeXorb(t, true, []byte("shared xorb"))

	first, second := newStepReader(), newStepReader()
	firstDone := putXorbAsync(fs, xorbHash, first)
	await(t, first.entered)
	secondDone := putXorbAsync(fs, xorbHash, second)
	await(t, second.entered)

	first.steps <- readStep{data: encoded}
	if err := await(t, firstDone); err != nil {
		t.Fatalf("first PutXorb: %v", err)
	}
	second.steps <- readStep{data: encoded}
	if err := await(t, secondDone); err != nil {
		t.Fatalf("second PutXorb: %v", err)
	}
	assertStoredXorb(t, fs, xorbHash, encoded)
	assertNoXorbTemp(t, fs, xorbHash)
}
