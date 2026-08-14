package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/pool"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// xorbRangeReader is the subset of Storage that reconstructedFile needs to
// stream chunk ranges out of stored xorbs.
type xorbRangeReader interface {
	GetXorbReadSeekCloser(ctx context.Context, namespace string, xorbHash xet.XorbHash) (io.ReadSeekCloser, error)
	GetXorbDataRange(ctx context.Context, namespace string, xorbHash xet.XorbHash, chunkStart, chunkEnd uint32) (startByte, endByte int64, err error)
}

// reconstructedFile exposes a shard file as an io.ReadSeekCloser. Seeking is
// implemented in terms of reconstruction entries, so http.ServeContent can
// provide HEAD and byte-range responses without materializing the whole file.
type reconstructedFile struct {
	ctx       context.Context
	storage   xorbRangeReader
	namespace string
	entries   []shard.FileDataSequenceEntry
	offsets   []int64
	size      int64
	position  int64

	entryIndex int
	xorb       io.ReadSeekCloser
	decoder    *xorb.Decoder
	chunkBuf   *[xet.MaxChunkSize + 8]byte
	chunk      []byte
	chunkPos   int
	closed     bool
}

func newReconstructedFile(ctx context.Context, stor xorbRangeReader, namespace string, sh *shard.Shard, digest [32]byte) (io.ReadSeekCloser, error) {
	var file *shard.FileBlock
	for i := range sh.Files {
		if sh.Files[i].MetadataExt != nil && sh.Files[i].MetadataExt.SHA256Hash == shard.NewSHA256Hash(digest) {
			file = &sh.Files[i]
			break
		}
	}
	if file == nil {
		return nil, fmt.Errorf("SHA-256 is not present in shard")
	}

	r := &reconstructedFile{
		ctx:        ctx,
		storage:    stor,
		namespace:  namespace,
		entries:    file.Entries,
		entryIndex: -1,
	}
	for i := range r.entries {
		r.size += int64(r.entries[i].UnpackedSegBytes)
	}
	return r, nil
}

func (r *reconstructedFile) Read(p []byte) (int, error) {
	if r.closed {
		return 0, fmt.Errorf("read reconstructed file: closed")
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.position >= r.size {
		return 0, io.EOF
	}

	n := 0
	for n < len(p) && r.position < r.size {
		idx := r.entryAt(r.position)
		if r.entryIndex != idx {
			if err := r.openEntry(idx, r.position-r.offsets[idx]); err != nil {
				return n, err
			}
		}
		if r.chunkPos == len(r.chunk) {
			if err := r.decodeChunk(); err != nil {
				if err == io.EOF && r.position == r.offsets[idx+1] {
					r.closeEntry()
					continue
				}
				return n, err
			}
		}
		copied := copy(p[n:], r.chunk[r.chunkPos:])
		r.chunkPos += copied
		r.position += int64(copied)
		n += copied
	}
	if n == 0 && r.position >= r.size {
		return 0, io.EOF
	}
	return n, nil
}

func (r *reconstructedFile) Seek(offset int64, whence int) (int64, error) {
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = r.position + offset
	case io.SeekEnd:
		next = r.size + offset
	default:
		return 0, fmt.Errorf("invalid seek whence %d", whence)
	}
	if next < 0 {
		return 0, fmt.Errorf("negative seek position %d", next)
	}
	if next == r.position {
		return next, nil
	}
	// Reposition within the buffered chunk to avoid reopening the entry.
	if r.chunk != nil {
		if pos := int64(r.chunkPos) + next - r.position; pos >= 0 && pos <= int64(len(r.chunk)) {
			r.chunkPos = int(pos)
			r.position = next
			return next, nil
		}
	}
	r.closeEntry()
	r.position = next
	return next, nil
}

func (r *reconstructedFile) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return r.closeEntry()
}

func (r *reconstructedFile) entryAt(position int64) int {
	r.initOffsets()
	for i := range r.entries {
		if position < r.offsets[i+1] {
			return i
		}
	}
	return len(r.entries)
}

func (r *reconstructedFile) initOffsets() {
	if r.offsets != nil {
		return
	}
	r.offsets = make([]int64, len(r.entries)+1)
	for i := range r.entries {
		r.offsets[i+1] = r.offsets[i] + int64(r.entries[i].UnpackedSegBytes)
	}
}

func (r *reconstructedFile) openEntry(index int, skip int64) error {
	r.closeEntry()
	entry := r.entries[index]
	f, err := r.storage.GetXorbReadSeekCloser(r.ctx, r.namespace, entry.CASHash)
	if err != nil {
		return err
	}
	start, end, err := r.storage.GetXorbDataRange(r.ctx, r.namespace, entry.CASHash, entry.ChunkIndexStart, entry.ChunkIndexEnd)
	if err != nil {
		f.Close()
		return err
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		f.Close()
		return err
	}
	r.xorb = f
	r.decoder = xorb.NewDecoder(io.LimitReader(f, end-start+1), false)
	r.entryIndex = index
	for skip > 0 {
		if err := r.decodeChunk(); err != nil {
			return err
		}
		remaining := int64(len(r.chunk) - r.chunkPos)
		if skip < remaining {
			r.chunkPos += int(skip)
			break
		}
		skip -= remaining
		r.chunkPos = len(r.chunk)
	}
	return nil
}

func (r *reconstructedFile) decodeChunk() error {
	if r.chunkBuf == nil {
		r.chunkBuf = pool.GetChunkBuf()
	}
	buf := r.chunkBuf[:xet.MaxChunkSize]
	n, err := r.decoder.Read(buf)
	if n != 0 {
		r.chunk = buf[:n]
		r.chunkPos = 0
		return nil
	}
	return err
}

func (r *reconstructedFile) closeEntry() error {
	r.decoder = nil
	if r.chunkBuf != nil {
		pool.PutChunkBuf(r.chunkBuf)
		r.chunkBuf = nil
	}
	r.chunk = nil
	r.chunkPos = 0
	r.entryIndex = -1
	if r.xorb == nil {
		return nil
	}
	err := r.xorb.Close()
	r.xorb = nil
	return err
}
