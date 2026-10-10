package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/pool"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// xorbReader encodes the chunks of r it lists into an xorb as it is read, so
// only one encoded chunk, or the footer, is buffered at a time.
type xorbReader struct {
	r        io.ReadSeeker
	offsets  []int64
	sizes    []uint64
	chunkBuf *[xet.MaxChunkSize + 8]byte
	enc      *xorb.Encoder
	buf      bytes.Buffer
}

// PutFile stores the file read from r under namespace: chunks already held by
// a stored shard are reused, the new ones are packed into xorbs, and the shard
// describing the file is committed. It returns the file's xet hash.
func PutFile(ctx context.Context, stor Storage, namespace string, r io.ReadSeeker) (xet.FileHash, error) {
	base, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return xet.FileHash{}, fmt.Errorf("locate file start: %w", err)
	}

	// chunks lists the file's unique chunks in first-occurrence order;
	// offsets[i] is where chunks[i] starts in r.
	var chunks []shard.ChunkInfo
	var offsets []int64
	var chunkHashes []xet.ChunkHash
	var chunkSizes []uint64
	var file shard.FileInfo
	unique := map[xet.ChunkHash]int{}
	err = xet.ChunkData(r, func(offset int64, chunk []byte) error {
		h := xet.ComputeChunkHash(chunk)
		chunkHashes = append(chunkHashes, h)
		chunkSizes = append(chunkSizes, uint64(len(chunk)))
		i, ok := unique[h]
		if !ok {
			i = len(chunks)
			unique[h] = i
			chunks = append(chunks, shard.ChunkInfo{Hash: h, Size: uint32(len(chunk))})
			offsets = append(offsets, base+offset)
		}
		file.ChunkIndices = append(file.ChunkIndices, i)
		return nil
	})
	if err != nil {
		return xet.FileHash{}, fmt.Errorf("chunk file: %w", err)
	}
	file.Hash = xet.ComputeFileHash(chunkHashes, chunkSizes)

	// A failed lookup only leaves the chunks it would locate to be stored anew.
	located := map[xet.ChunkHash]shard.ChunkLocation{}
	for i, c := range chunks {
		if _, ok := located[c.Hash]; ok || !shard.IsChunkGlobalDedupEligible(c.Hash, i == 0, 0) {
			continue
		}
		sh, err := stor.GetShardByChunkHash(ctx, namespace, c.Hash)
		if err != nil || sh == nil {
			continue
		}
		for h, loc := range sh.ChunkLocations() {
			if _, ok := located[h]; !ok {
				located[h] = loc
			}
		}
	}

	var fresh []int
	var freshSizes []uint32
	for i := range chunks {
		if loc, ok := located[chunks[i].Hash]; ok {
			chunks[i].XorbHash, chunks[i].ChunkIndex = loc.XorbHash, loc.ChunkIndex
		} else {
			fresh = append(fresh, i)
			freshSizes = append(freshSizes, chunks[i].Size)
		}
	}
	chunkBuf := pool.GetChunkBuf()
	defer pool.PutChunkBuf(chunkBuf)
	for _, group := range xorb.GroupChunkIndicesBySize(freshSizes, xet.MaxXorbSize) {
		x := &xorbReader{r: r, chunkBuf: chunkBuf}
		x.enc = xorb.NewEncoder(&x.buf, true)
		var hashes []xet.ChunkHash
		for _, k := range group {
			hashes = append(hashes, chunks[fresh[k]].Hash)
			x.sizes = append(x.sizes, uint64(chunks[fresh[k]].Size))
			x.offsets = append(x.offsets, offsets[fresh[k]])
		}
		xorbHash := xet.ComputeXorbHash(hashes, x.sizes)
		if _, err := stor.PutXorb(ctx, namespace, xorbHash, x); err != nil {
			return xet.FileHash{}, fmt.Errorf("store xorb %s: %w", xorbHash.String(), err)
		}
		for j, k := range group {
			c := &chunks[fresh[k]]
			c.IsNew, c.XorbHash, c.ChunkIndex = true, xorbHash, uint32(j)
		}
	}

	if _, err := stor.PutShard(ctx, shard.BuildShard([]shard.FileInfo{file}, chunks)); err != nil {
		return xet.FileHash{}, fmt.Errorf("store shard: %w", err)
	}
	return file.Hash, nil
}

func (x *xorbReader) Read(p []byte) (int, error) {
	for x.buf.Len() == 0 {
		if len(x.sizes) == 0 {
			return 0, io.EOF
		}
		chunk := x.chunkBuf[:x.sizes[0]]
		if _, err := x.r.Seek(x.offsets[0], io.SeekStart); err != nil {
			return 0, fmt.Errorf("seek chunk: %w", err)
		}
		if _, err := io.ReadFull(x.r, chunk); err != nil {
			return 0, fmt.Errorf("read chunk: %w", err)
		}
		if _, err := x.enc.Write(chunk); err != nil {
			return 0, err
		}
		x.offsets, x.sizes = x.offsets[1:], x.sizes[1:]
		if len(x.sizes) == 0 {
			if err := x.enc.Close(); err != nil {
				return 0, err
			}
		}
	}
	return x.buf.Read(p)
}
