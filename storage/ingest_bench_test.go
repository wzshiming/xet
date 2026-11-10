package storage_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/benchprof"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/memory"
	"github.com/wzshiming/xet/storage/storagetest"
	"github.com/wzshiming/xet/xorb"
)

const (
	ingestPieces    = 1024
	ingestPieceSize = 64 << 10
	ingestSize      = ingestPieces * ingestPieceSize
)

// ingestInput is a class's file to ingest and its SHA-256.
type ingestInput struct {
	data   []byte
	digest [32]byte
}

// ingestInputs generates each class's input on first use and keeps it for the
// rest of the process, so every stage and -count rerun shares one copy.
var ingestInputs = func() []func() ingestInput {
	inputs := make([]func() ingestInput, len(storagetest.ChunkClasses))
	for i, c := range storagetest.ChunkClasses {
		inputs[i] = sync.OnceValue(func() ingestInput {
			data := make([]byte, ingestSize)
			for k := range ingestPieces {
				c.Chunk(0, k, data[k*ingestPieceSize:(k+1)*ingestPieceSize])
			}
			return ingestInput{data: data, digest: sha256.Sum256(data)}
		})
	}
	return inputs
}()

// ingestChunks is the chunk stage: its callback counts the bytes chunked and
// passes each chunk on to fn.
func ingestChunks(data []byte, fn func(chunk []byte) error) error {
	n := 0
	err := xet.ChunkData(bytes.NewReader(data), func(_ int64, chunk []byte) error {
		n += len(chunk)
		return fn(chunk)
	})
	if err == nil && n != len(data) {
		err = fmt.Errorf("chunked %d bytes, want %d", n, len(data))
	}
	return err
}

// ingestEncode is the encode stage: the hash stage plus writing each chunk
// into one xorb with a footer, which hashes the chunk again as PutFile does.
func ingestEncode(data []byte, w io.Writer) error {
	enc := xorb.NewEncoder(w, true)
	if err := ingestChunks(data, func(chunk []byte) error {
		xet.ComputeChunkHash(chunk)
		_, err := enc.Write(chunk)
		return err
	}); err != nil {
		return err
	}
	return enc.Close()
}

// ingestCounter counts the bytes written to it.
type ingestCounter int64

func (c *ingestCounter) Write(p []byte) (int, error) {
	*c += ingestCounter(len(p))
	return len(p), nil
}

// ingestLoop times stage, which ingests the whole input, once per iteration.
func ingestLoop(b *testing.B, stage func() error) {
	b.SetBytes(ingestSize)
	b.ReportAllocs()
	stop := benchprof.Start(b)
	for b.Loop() {
		if err := stage(); err != nil {
			b.Fatal(err)
		}
	}
	stop()
}

func benchmarkIngestChunk(b *testing.B, in ingestInput) {
	ingestLoop(b, func() error { return ingestChunks(in.data, func([]byte) error { return nil }) })
}

func benchmarkIngestHash(b *testing.B, in ingestInput) {
	ingestLoop(b, func() error {
		return ingestChunks(in.data, func(chunk []byte) error {
			xet.ComputeChunkHash(chunk)
			return nil
		})
	})
}

func benchmarkIngestEncode(b *testing.B, in ingestInput) {
	var wrote ingestCounter
	if err := ingestEncode(in.data, &wrote); err != nil || wrote == 0 {
		b.Fatalf("encode stage wrote %d bytes, %v; want some", wrote, err)
	}
	ingestLoop(b, func() error { return ingestEncode(in.data, io.Discard) })
}

// benchmarkIngestPutFile stores the input on a new memory backend per
// iteration, created untimed and dropped once stored.
func benchmarkIngestPutFile(b *testing.B, in ingestInput) {
	checkIngestPutFile(b, in)
	ctx := b.Context()
	ingestLoop(b, func() error {
		b.StopTimer()
		st := memory.NewStorage()
		b.StartTimer()
		_, err := storage.PutFile(ctx, st, bytes.NewReader(in.data))
		return err
	})
}

// checkIngestPutFile stores the input once on a new backend and checks the
// file is then found by its SHA-256.
func checkIngestPutFile(b *testing.B, in ingestInput) {
	b.Helper()
	st := memory.NewStorage()
	if _, err := storage.PutFile(b.Context(), st, bytes.NewReader(in.data)); err != nil {
		b.Fatal(err)
	}
	sh, err := st.GetShardBySHA256(b.Context(), in.digest)
	if err != nil {
		b.Fatalf("GetShardBySHA256: %v", err)
	}
	if sh.FindFileBySHA256(in.digest) == nil {
		b.Fatal("FindFileBySHA256: not found")
	}
}

// BenchmarkIngest stores a 64 MiB file per compression class through a ladder
// of stages, each adding one step of PutFile's work to the stage before it.
func BenchmarkIngest(b *testing.B) {
	stages := []struct {
		name  string
		bench func(*testing.B, ingestInput)
	}{
		{"chunk", benchmarkIngestChunk},
		{"hash", benchmarkIngestHash},
		{"encode", benchmarkIngestEncode},
		{"putfile", benchmarkIngestPutFile},
	}
	for _, s := range stages {
		b.Run("stage="+s.name, func(b *testing.B) {
			for i, c := range storagetest.ChunkClasses {
				b.Run("class="+c.Name, func(b *testing.B) { s.bench(b, ingestInputs[i]()) })
			}
		})
	}
}
