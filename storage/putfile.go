package storage

import (
	"bytes"
	"context"
	"errors"
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

// PutFile stores the file read from r: chunks already held by a stored shard
// are reused, the new ones are packed into xorbs, and the shard describing the
// file is committed; an empty file stores nothing. It returns the file's xet hash.
func PutFile(ctx context.Context, stor Storage, r io.ReadSeeker) (xet.FileHash, error) {
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
		sh, err := stor.GetShardByChunkHash(ctx, c.Hash)
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
		if _, err := PutXorb(ctx, stor, xorbHash, x); err != nil {
			return xet.FileHash{}, fmt.Errorf("store xorb %s: %w", xorbHash.String(), err)
		}
		for j, k := range group {
			c := &chunks[fresh[k]]
			c.IsNew, c.XorbHash, c.ChunkIndex = true, xorbHash, uint32(j)
		}
	}

	if len(chunks) > 0 {
		if _, err := PutShard(ctx, stor, shard.BuildShard([]shard.FileInfo{file}, chunks)); err != nil {
			return xet.FileHash{}, fmt.Errorf("store shard: %w", err)
		}
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

// PutXorb stores the xorb read from r through st while xorb.Validate checks it
// against xorbHash: st reads the bytes Validate consumes, then io.EOF or the
// validation error, so a rejected xorb is not stored.
func PutXorb(ctx context.Context, st Storage, xorbHash xet.XorbHash, r io.Reader) (bool, error) {
	v := &validatingReader{r: r, xorbHash: xorbHash}
	inserted, err := st.PutXorb(ctx, xorbHash, v)
	if verr := v.stop(); verr != nil {
		return false, fmt.Errorf("validate xorb: %w", verr)
	}
	return inserted, err
}

// validatingReader yields the bytes xorb.Validate consumes from r, starting
// Validate on the first Read.
type validatingReader struct {
	r        io.Reader
	xorbHash xet.XorbHash
	pr       *io.PipeReader
	done     chan struct{}
	err      error
}

func (v *validatingReader) Read(p []byte) (int, error) {
	if v.pr == nil {
		pr, pw := io.Pipe()
		v.pr, v.done = pr, make(chan struct{})
		go func() {
			defer close(v.done)
			v.err = xorb.Validate(io.TeeReader(v.r, pw), v.xorbHash)
			pw.CloseWithError(v.err)
		}()
	}
	return v.pr.Read(p)
}

// stop ends a validation the store stopped reading and returns its error,
// ignoring the one the stop itself causes.
func (v *validatingReader) stop() error {
	if v.pr == nil {
		return nil
	}
	v.pr.Close()
	<-v.done
	if errors.Is(v.err, io.ErrClosedPipe) {
		return nil
	}
	return v.err
}
