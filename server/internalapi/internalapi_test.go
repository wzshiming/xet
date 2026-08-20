package internalapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/xorb"
)

func TestListFilesEndpoint(t *testing.T) {
	ctx := context.Background()
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}

	content := []byte("internal file listing content")
	var encoded bytes.Buffer
	encoder := xorb.NewEncoder(&encoded, true)
	if _, err := encoder.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	xorbHash := encoder.SummoryHash()
	if _, err := stor.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded.Bytes())); err != nil {
		t.Fatal(err)
	}
	chunkHash := xet.ComputeChunkHash(content)
	fileHash := xet.ComputeFileHash([]xet.ChunkHash{chunkHash}, []uint64{uint64(len(content))})
	shardObj := shard.NewShard()
	shardObj.AddCASBlock(shard.CASBlock{
		CASHash: xorbHash,
		Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: chunkHash, UnpackedSegBytes: uint32(len(content))}},
	})
	shardObj.AddFile(shard.FileBlock{
		FileHash: fileHash,
		Entries: []shard.FileDataSequenceEntry{
			{CASHash: xorbHash, UnpackedSegBytes: uint32(len(content)), ChunkIndexEnd: 1},
		},
	})
	if _, err := stor.PutShard(ctx, shardObj); err != nil {
		t.Fatal(err)
	}

	handler := NewHandler(WithStorage(stor))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/internal/files", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	var entries []storage.FileListEntry
	if err := json.NewDecoder(rec.Body).Decode(&entries); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	digest := sha256.Sum256(content)
	want := []storage.FileListEntry{{
		SHA256:       hex.EncodeToString(digest[:]),
		FileHashes:   []string{fileHash.String()},
		OriginalSize: uint64(len(content)),
	}}
	if len(entries) != 1 || entries[0].SHA256 != want[0].SHA256 ||
		len(entries[0].FileHashes) != 1 || entries[0].FileHashes[0] != want[0].FileHashes[0] ||
		entries[0].OriginalSize != want[0].OriginalSize || entries[0].Missing {
		t.Fatalf("entries = %+v, want %+v", entries, want)
	}
}

// listlessStorage implements storage.Storage but not storage.ListStore.
type listlessStorage struct{ storage.Storage }

func TestListFilesEndpointNotImplemented(t *testing.T) {
	handler := NewHandler(WithStorage(listlessStorage{}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/internal/files", nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotImplemented)
	}
}

func TestHandlerFallsThroughToNext(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	handler := NewHandler(WithNext(next))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/shards", nil))
	if rec.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusTeapot)
	}
}
