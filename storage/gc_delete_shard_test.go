package storage

import (
	"bytes"
	"context"
	"errors"
	iofs "io/fs"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
)

// corruptStoredShard overwrites the stored shard object's bytes in place so
// it stays present but can no longer be decoded.
func corruptStoredShard(t *testing.T, st Storage, shardHash string) {
	t.Helper()
	garbage := bytes.Repeat([]byte{0xff}, 128)
	switch b := st.(type) {
	case *FileStorage:
		if err := os.WriteFile(b.objectPath("shards", shardHash), garbage, 0644); err != nil {
			t.Fatal(err)
		}
	case *S3Storage:
		if err := b.putObject(context.Background(), b.objectKey("shards", shardHash), bytes.NewReader(garbage)); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("no corruption path for backend %T", st)
	}
}

// TestDeleteShardObjectUnreadable: a corrupt stored shard freezes every xorb
// delete; DeleteShardObject removes it without force, and the next sweep
// reclaims its orphaned xorbs and reports its dangling entries.
func TestDeleteShardObjectUnreadable(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("unreadable remediation target")})
			corruptStoredShard(t, st, f.shardHash)

			res, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace})
			if err != nil {
				t.Fatalf("Sweep with the corrupt shard: %v", err)
			}
			if got, want := res.UnreadableShards, []string{f.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("UnreadableShards = %v, want %v", got, want)
			}
			if len(res.SweptXorbs) != 0 {
				t.Fatalf("SweptXorbs = %v, want none while a shard is unreadable", res.SweptXorbs)
			}
			if ok, err := st.HasXorb(ctx, "default", f.xorbHashes[0]); err != nil || !ok {
				t.Fatalf("frozen xorb stored = %t, %v; want kept", ok, err)
			}

			out, err := NewGC(gcs).DeleteShardObject(ctx, f.shardHash, false)
			if err != nil {
				t.Fatalf("DeleteShardObject: %v", err)
			}
			if !out.Removed || out.WasReadable || out.Referenced {
				t.Fatalf("outcome = %+v, want removed unreadable unreferenced", out)
			}
			if ok, err := gcs.HasShardObject(ctx, f.shardHash); err != nil || ok {
				t.Fatalf("shard object stored after delete = %t, %v", ok, err)
			}

			res, err = Sweep(ctx, gcs, SweepOptions{Grace: noGrace})
			if err != nil {
				t.Fatalf("Sweep after the delete: %v", err)
			}
			if len(res.UnreadableShards) != 0 {
				t.Fatalf("UnreadableShards = %v, want empty after remediation", res.UnreadableShards)
			}
			if got, want := sweptHashes(res.SweptXorbs), []string{f.xorbHashes[0].String()}; !slices.Equal(got, want) {
				t.Fatalf("SweptXorbs = %v, want the unfrozen orphan %v", got, want)
			}
			if ok, _ := st.HasXorb(ctx, "default", f.xorbHashes[0]); ok {
				t.Fatal("orphaned xorb still stored after the unfrozen sweep")
			}
			if got, want := res.DanglingFileEntries, []string{f.fileHash.String()}; !slices.Equal(got, want) {
				t.Fatalf("DanglingFileEntries = %v, want %v", got, want)
			}
			if got, want := res.DanglingSHA256Entries, []string{f.sha256Hex}; !slices.Equal(got, want) {
				t.Fatalf("DanglingSHA256Entries = %v, want %v", got, want)
			}
		})
	}
}

// TestDeleteShardObjectReadableReferenced: a live referenced shard is
// refused without force and deleted with it; the next sweep reports the
// dangling entries the forced delete left behind.
func TestDeleteShardObjectReadableReferenced(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)
			g := NewGC(gcs)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("referenced shard refusal")})

			out, err := g.DeleteShardObject(ctx, f.shardHash, false)
			if err != nil {
				t.Fatalf("DeleteShardObject without force: %v", err)
			}
			if out.Removed || !out.WasReadable || !out.Referenced {
				t.Fatalf("outcome = %+v, want a readable referenced refusal", out)
			}
			if ok, err := gcs.HasShardObject(ctx, f.shardHash); err != nil || !ok {
				t.Fatalf("shard object stored after refusal = %t, %v; want kept", ok, err)
			}
			if got, _, err := gcs.GetFileIndexEntry(ctx, f.fileHash); err != nil || got != f.shardHash {
				t.Fatalf("file entry after refusal = %q, %v; want intact", got, err)
			}
			if got, err := gcs.GetSHA256IndexEntry(ctx, f.sha256Hex); err != nil || got != f.shardHash {
				t.Fatalf("sha256 entry after refusal = %q, %v; want intact", got, err)
			}

			out, err = g.DeleteShardObject(ctx, f.shardHash, true)
			if err != nil {
				t.Fatalf("DeleteShardObject with force: %v", err)
			}
			if !out.Removed || !out.WasReadable || !out.Referenced {
				t.Fatalf("forced outcome = %+v, want removed readable referenced", out)
			}
			if ok, err := gcs.HasShardObject(ctx, f.shardHash); err != nil || ok {
				t.Fatalf("shard object stored after force = %t, %v", ok, err)
			}

			res, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace})
			if err != nil {
				t.Fatalf("Sweep after force: %v", err)
			}
			if got, want := res.DanglingFileEntries, []string{f.fileHash.String()}; !slices.Equal(got, want) {
				t.Fatalf("DanglingFileEntries = %v, want %v", got, want)
			}
			if got, want := res.DanglingSHA256Entries, []string{f.sha256Hex}; !slices.Equal(got, want) {
				t.Fatalf("DanglingSHA256Entries = %v, want %v", got, want)
			}
		})
	}
}

// TestDeleteShardObjectUnreferenced: a readable shard with every own entry
// unlinked deletes without force — chunk entries alone never refuse.
func TestDeleteShardObjectUnreferenced(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)
			g := NewGC(gcs)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("entry-less readable shard")})
			if _, err := g.Unlink(ctx, f.fileHash); err != nil {
				t.Fatalf("Unlink: %v", err)
			}
			if _, err := g.UnlinkSHA256(ctx, sha256Digest(f.sha256Hex)); err != nil {
				t.Fatalf("UnlinkSHA256: %v", err)
			}

			out, err := g.DeleteShardObject(ctx, f.shardHash, false)
			if err != nil {
				t.Fatalf("DeleteShardObject: %v", err)
			}
			if !out.Removed || !out.WasReadable || out.Referenced {
				t.Fatalf("outcome = %+v, want removed readable unreferenced", out)
			}
			if ok, err := gcs.HasShardObject(ctx, f.shardHash); err != nil || ok {
				t.Fatalf("shard object stored after delete = %t, %v", ok, err)
			}
		})
	}
}

// TestDeleteShardObjectBusy: a sweep step holding the GC turns
// DeleteShardObject away with ErrGCBusy instead of racing it.
func TestDeleteShardObjectBusy(t *testing.T) {
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	f := putGCFile(t, ctx, st, [][]byte{[]byte("busy guard shard")})

	hooked := &hookedGCStore{GCStore: st}
	g := NewGC(hooked)

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	hooked.beforeWalkShards = func() {
		once.Do(func() {
			close(entered)
			<-release
		})
	}
	done := make(chan struct{})
	var stepErr error
	go func() {
		defer close(done)
		_, stepErr = g.SweepStep(ctx, SweepOptions{})
	}()
	<-entered
	if _, err := g.DeleteShardObject(ctx, f.shardHash, true); !errors.Is(err, ErrGCBusy) {
		t.Fatalf("DeleteShardObject during a step = %v, want ErrGCBusy", err)
	}
	close(release)
	<-done
	hooked.beforeWalkShards = nil
	if stepErr != nil {
		t.Fatalf("SweepStep: %v", stepErr)
	}

	out, err := g.DeleteShardObject(ctx, f.shardHash, true)
	if err != nil {
		t.Fatalf("DeleteShardObject after release: %v", err)
	}
	if !out.Removed || !out.WasReadable {
		t.Fatalf("outcome after release = %+v, want removed readable", out)
	}
}

// TestDeleteShardObjectValidation: malformed hashes are rejected before any
// backend call — the nil store would panic on one.
func TestDeleteShardObjectValidation(t *testing.T) {
	g := NewGC(nil)
	for _, hash := range []string{
		"",
		"abc",
		strings.Repeat("z", 64),
		strings.Repeat("a", 63),
		strings.Repeat("a", 65),
	} {
		out, err := g.DeleteShardObject(context.Background(), hash, false)
		if !errors.Is(err, ErrInvalidShardHash) {
			t.Fatalf("%q error = %v, want ErrInvalidShardHash", hash, err)
		}
		if out != (DeleteShardOutcome{}) {
			t.Fatalf("%q outcome = %+v, want zero", hash, out)
		}
	}
}

// TestDeleteShardObjectAbsent: a well-formed hash with no stored object
// answers with io/fs.ErrNotExist.
func TestDeleteShardObjectAbsent(t *testing.T) {
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewGC(st).DeleteShardObject(context.Background(), strings.Repeat("ab", 32), false)
	if !errors.Is(err, iofs.ErrNotExist) {
		t.Fatalf("error = %v, want ErrNotExist", err)
	}
}

// TestDeleteShardObjectCanceledContext: a dying context must not delete —
// FileStorage itself ignores ctx, so the coordinator has to check.
func TestDeleteShardObjectCanceledContext(t *testing.T) {
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	f := putGCFile(t, ctx, st, [][]byte{[]byte("canceled ctx shard")})

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	g := NewGC(st)
	out, err := g.DeleteShardObject(canceled, f.shardHash, true)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if out.Removed {
		t.Fatal("outcome reports a removal under a canceled context")
	}
	if stored, err := st.HasShardObject(ctx, f.shardHash); err != nil || !stored {
		t.Fatalf("shard object gone despite canceled context: %v %v", stored, err)
	}
}

// TestDeleteShardObjectDiscardsParkedCycle: a successful deletion drops a
// parked cycle, whose owner maps may name the deleted shard as a repoint
// target.
func TestDeleteShardObjectDiscardsParkedCycle(t *testing.T) {
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	f1 := putGCFile(t, ctx, st, [][]byte{[]byte("parked cycle shard one")})
	f2 := putGCFile(t, ctx, st, [][]byte{[]byte("parked cycle shard two")})
	live := putGCFile(t, ctx, st, [][]byte{[]byte("parked cycle live shard")})

	g := NewGC(st)
	for _, f := range []gcFile{f1, f2} {
		if _, err := g.Unlink(ctx, f.fileHash); err != nil {
			t.Fatal(err)
		}
		if _, err := g.UnlinkSHA256(ctx, sha256Digest(f.sha256Hex)); err != nil {
			t.Fatal(err)
		}
	}
	res, err := g.SweepStep(ctx, SweepOptions{Grace: -1, MaxDeletes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if res.Done || g.cycle == nil {
		t.Fatalf("step done = %v, cycle = %v; want a parked cycle", res.Done, g.cycle)
	}

	out, err := g.DeleteShardObject(ctx, live.shardHash, true)
	if err != nil || !out.Removed {
		t.Fatalf("DeleteShardObject = %+v, %v", out, err)
	}
	if g.cycle != nil {
		t.Fatal("parked cycle survived a shard deletion")
	}
}
