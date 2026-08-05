package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/xorb"
)

func TestXetBridgeExtractsCompleteFileBySHA256(t *testing.T) {
	ctx := context.Background()
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{[]byte("first part "), []byte("and the second part")}
	fileData := bytes.Join(parts, nil)
	shardObj := shard.NewShard()
	fileBlock := shard.FileBlock{}
	var chunkHashes []xet.ChunkHash
	var chunkSizes []uint64
	for _, part := range parts {
		var encoded bytes.Buffer
		encoder := xorb.NewEncoder(&encoded, true)
		if _, err := encoder.Write(part); err != nil {
			t.Fatal(err)
		}
		if err := encoder.Close(); err != nil {
			t.Fatal(err)
		}
		xorbHash := encoder.SummoryHash()
		if _, err := stor.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded.Bytes())); err != nil {
			t.Fatal(err)
		}
		chunkHash := xet.ComputeChunkHash(part)
		chunkHashes = append(chunkHashes, chunkHash)
		chunkSizes = append(chunkSizes, uint64(len(part)))
		fileBlock.Entries = append(fileBlock.Entries, shard.FileDataSequenceEntry{
			CASHash: xorbHash, UnpackedSegBytes: uint32(len(part)), ChunkIndexEnd: 1,
		})
		shardObj.AddCASBlock(shard.CASBlock{
			CASHash: xorbHash,
			Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: chunkHash, UnpackedSegBytes: uint32(len(part))}},
		})
	}
	fileBlock.FileHash = xet.ComputeFileHash(chunkHashes, chunkSizes)
	shardObj.AddFile(fileBlock)
	if _, err := stor.PutShard(ctx, shardObj); err != nil {
		t.Fatal(err)
	}

	digest := sha256.Sum256(fileData)
	handler := NewHandler(WithStorage(stor))
	req := httptest.NewRequest(http.MethodGet, "/xet-bridge/"+hex.EncodeToString(digest[:]), nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp := rec.Result()
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %q", resp.StatusCode, got)
	}
	if !bytes.Equal(got, fileData) {
		t.Fatalf("body = %q, want %q", got, fileData)
	}
	if resp.ContentLength != int64(len(fileData)) {
		t.Fatalf("Content-Length = %d, want %d", resp.ContentLength, len(fileData))
	}

	t.Run("HEAD", func(t *testing.T) {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodHead, req.URL.Path, nil))
		resp := rec.Result()
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
		}
		if len(body) != 0 {
			t.Fatalf("body = %q, want empty", body)
		}
		if resp.ContentLength != int64(len(fileData)) {
			t.Fatalf("Content-Length = %d, want %d", resp.ContentLength, len(fileData))
		}
	})

	t.Run("range crossing reconstruction entries", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet, req.URL.Path, nil)
		start, end := len(parts[0])-2, len(parts[0])+3
		request.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, request)
		resp := rec.Result()
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusPartialContent {
			t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusPartialContent)
		}
		if want := fileData[start : end+1]; !bytes.Equal(body, want) {
			t.Fatalf("body = %q, want %q", body, want)
		}
		if got, want := resp.Header.Get("Content-Range"), fmt.Sprintf("bytes %d-%d/%d", start, end, len(fileData)); got != want {
			t.Fatalf("Content-Range = %q, want %q", got, want)
		}
	})
}

func TestXetBridgeRejectsInvalidAndUnknownSHA256(t *testing.T) {
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(stor))
	for _, test := range []struct {
		path string
		want int
	}{
		{path: "/xet-bridge/not-a-digest", want: http.StatusBadRequest},
		{path: "/xet-bridge/" + string(bytes.Repeat([]byte{'0'}, 64)), want: http.StatusNotFound},
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, test.path, nil))
		if rec.Code != test.want {
			t.Errorf("GET %s: status = %d, want %d", test.path, rec.Code, test.want)
		}
	}
}

func TestReconstructionMakesRelativeXorbURLAbsolute(t *testing.T) {
	ctx := context.Background()
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("absolute fetch URL")
	var encoded bytes.Buffer
	encoder := xorb.NewEncoder(&encoded, true)
	if _, err := encoder.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	xorbHash := encoder.SummoryHash()
	if _, err := stor.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded.Bytes())); err != nil {
		t.Fatal(err)
	}
	chunkHash := xet.ComputeChunkHash(data)
	fileHash := xet.ComputeFileHash([]xet.ChunkHash{chunkHash}, []uint64{uint64(len(data))})
	sh := shard.NewShard()
	sh.AddCASBlock(shard.CASBlock{
		CASHash: xorbHash,
		Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: chunkHash, UnpackedSegBytes: uint32(len(data))}},
	})
	sh.AddFile(shard.FileBlock{
		FileHash: fileHash,
		Entries: []shard.FileDataSequenceEntry{{
			CASHash: xorbHash, UnpackedSegBytes: uint32(len(data)), ChunkIndexEnd: 1,
		}},
	})
	if _, err := stor.PutShard(ctx, sh); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/reconstructions/"+fileHash.String(), nil)
	req.Host = "mirror.example"
	req.Header.Set("X-Forwarded-Proto", "https")
	rec := httptest.NewRecorder()
	NewHandler(WithStorage(stor)).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var reconstruction download.ReconstructionResponseV1
	if err := json.NewDecoder(rec.Body).Decode(&reconstruction); err != nil {
		t.Fatal(err)
	}
	entries := reconstruction.FetchInfo[xorbHash.String()]
	if len(entries) == 0 {
		t.Fatal("missing fetch info")
	}
	want := "https://mirror.example/v1/xorbs/default/" + xorbHash.String()
	if got := entries[0].URL; got != want {
		t.Fatalf("xorb URL = %q, want %q", got, want)
	}
}
