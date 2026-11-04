package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"sync"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/pool"
	"github.com/wzshiming/xet/pins"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// Source delivers a file through Read from its first byte; ReadAt serves what Read delivered at offsets from that byte, concurrently and without blocking, so *os.File, *bytes.Reader and *io.SectionReader qualify when positioned at their start.
type Source interface {
	io.Reader
	io.ReaderAt
}

// PutFileResult identifies the file PutFile read.
type PutFileResult struct {
	FileHash xet.FileHash
	SHA256   [32]byte
	Size     int64
}

// PutFileOption configures one PutFile call.
type PutFileOption func(*putFileConfig)

type putFileConfig struct {
	pins *pins.Table[xet.XorbHash]
}

// WithPins pins in p every xorb the file's shard will reference until PutFile returns, a stored one before its PutXorb; nil pins nothing.
func WithPins(p *pins.Table[xet.XorbHash]) PutFileOption {
	return func(c *putFileConfig) {
		c.pins = p
	}
}

// PutFile stores src's file as Read delivers it, nothing for an empty src, and never touches src after returning; with pins the xorbs it stores or reuses stay pinned until it returns.
func PutFile(ctx context.Context, stor Storage, src Source, opts ...PutFileOption) (PutFileResult, error) {
	var cfg putFileConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	pinned := map[xet.XorbHash]func(){}
	pin := func(h xet.XorbHash) {
		if _, ok := pinned[h]; !ok {
			pinned[h] = cfg.pins.Pin(h)
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	w := &xorbWorker{
		ctx: ctx, cancel: cancel, stor: stor, src: src,
		ready: make(chan struct{}, 1), done: make(chan struct{}),
	}
	go w.run()
	defer func() {
		cancel()
		<-w.done
		for _, unpin := range pinned {
			unpin()
		}
	}()

	var (
		chunks  []shard.ChunkInfo // unique chunks in first-occurrence order
		offsets []int64           // where each of chunks starts in src
		file    shard.FileInfo
		size    int64
	)
	unique := map[xet.ChunkHash]int{}
	located := map[xet.ChunkHash]shard.ChunkLocation{}
	grouper := newXorbGrouper(xet.MaxXorbSize)
	closeGroup := func(ids []int) {
		job := xorbJob{chunks: make([]chunkSpan, len(ids))}
		hashes := make([]xet.ChunkHash, len(ids))
		sizes := make([]uint64, len(ids))
		for k, id := range ids {
			job.chunks[k] = chunkSpan{offset: offsets[id], size: chunks[id].Size}
			hashes[k], sizes[k] = chunks[id].Hash, uint64(chunks[id].Size)
		}
		job.hash = xet.ComputeXorbHash(hashes, sizes)
		for k, id := range ids {
			chunks[id].IsNew, chunks[id].XorbHash, chunks[id].ChunkIndex = true, job.hash, uint32(k)
		}
		// Pinned before the push, so before the worker's PutXorb.
		pin(job.hash)
		w.push(job)
	}

	digest := sha256.New()
	err := xet.ChunkData(io.TeeReader(src, digest), func(offset int64, chunk []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		size += int64(len(chunk))
		h := xet.ComputeChunkHash(chunk)
		if i, ok := unique[h]; ok {
			file.ChunkIndices = append(file.ChunkIndices, i)
			return nil
		}
		i := len(chunks)
		unique[h] = i
		chunks = append(chunks, shard.ChunkInfo{Hash: h, Size: uint32(len(chunk))})
		offsets = append(offsets, offset)
		file.ChunkIndices = append(file.ChunkIndices, i)

		// A failed lookup only leaves the chunks it would locate to be stored anew.
		if _, ok := located[h]; !ok && shard.IsChunkGlobalDedupEligible(h, i == 0, 0) {
			if sh, err := stor.GetShardByChunkHash(ctx, h); err == nil && sh != nil {
				for ch, loc := range sh.ChunkLocations() {
					if _, ok := located[ch]; ok {
						continue
					}
					located[ch] = loc
					// Chunks of closed groups keep their xorb.
					if j, ok := unique[ch]; ok && grouper.remove(j) {
						chunks[j].XorbHash, chunks[j].ChunkIndex = loc.XorbHash, loc.ChunkIndex
						pin(loc.XorbHash)
					}
				}
			}
		}
		if loc, ok := located[h]; ok {
			chunks[i].XorbHash, chunks[i].ChunkIndex = loc.XorbHash, loc.ChunkIndex
			pin(loc.XorbHash)
		} else if ids := grouper.add(i, chunks[i].Size); ids != nil {
			closeGroup(ids)
		}
		return nil
	})
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return PutFileResult{}, w.fail(fmt.Errorf("chunk file: %w", err))
	}
	if ids := grouper.flush(); ids != nil {
		closeGroup(ids)
	}
	w.end()

	hashes := make([]xet.ChunkHash, len(file.ChunkIndices))
	sizes := make([]uint64, len(file.ChunkIndices))
	for k, i := range file.ChunkIndices {
		hashes[k], sizes[k] = chunks[i].Hash, uint64(chunks[i].Size)
	}
	file.Hash = xet.ComputeFileHash(hashes, sizes)
	digest.Sum(file.SHA256[:0])

	<-w.done
	if err := ctx.Err(); err != nil {
		return PutFileResult{}, w.fail(err)
	}
	if len(chunks) > 0 {
		if _, err := stor.PutShard(ctx, shard.BuildShard([]shard.FileInfo{file}, chunks)); err != nil {
			return PutFileResult{}, w.fail(fmt.Errorf("store shard: %w", err))
		}
	}
	return PutFileResult{FileHash: file.Hash, SHA256: file.SHA256, Size: size}, nil
}

// xorbJob is a closed group: its xorb hash and where its chunks lie in the source.
type xorbJob struct {
	hash   xet.XorbHash
	chunks []chunkSpan
}

type chunkSpan struct {
	offset int64
	size   uint32
}

// xorbWorker stores the queued xorbs one at a time, in stream order, beside the chunk loop.
type xorbWorker struct {
	ctx    context.Context
	cancel context.CancelFunc
	stor   Storage
	src    io.ReaderAt
	ready  chan struct{}
	done   chan struct{}

	mu    sync.Mutex
	err   error
	jobs  []xorbJob
	ended bool
}

// fail records the first failure only, stops the run and returns the recorded failure.
func (w *xorbWorker) fail(err error) error {
	w.mu.Lock()
	if w.err == nil {
		w.err = err
	}
	err = w.err
	w.mu.Unlock()
	w.cancel()
	return err
}

// push never blocks, so slow storage cannot stall the chunk loop.
func (w *xorbWorker) push(job xorbJob) {
	w.mu.Lock()
	w.jobs = append(w.jobs, job)
	w.mu.Unlock()
	w.wake()
}

func (w *xorbWorker) end() {
	w.mu.Lock()
	w.ended = true
	w.mu.Unlock()
	w.wake()
}

func (w *xorbWorker) wake() {
	select {
	case w.ready <- struct{}{}:
	default:
	}
}

func (w *xorbWorker) run() {
	defer close(w.done)
	buf := pool.GetChunkBuf()
	defer pool.PutChunkBuf(buf)
	for {
		w.mu.Lock()
		jobs, ended := w.jobs, w.ended
		w.jobs = nil
		w.mu.Unlock()
		for _, job := range jobs {
			if w.ctx.Err() != nil {
				return
			}
			x := &xorbReader{ctx: w.ctx, src: w.src, chunks: job.chunks, buf: buf}
			x.enc = xorb.NewEncoder(&x.out, true)
			_, err := w.stor.PutXorb(w.ctx, job.hash, x)
			x.seal()
			if err != nil {
				w.fail(fmt.Errorf("store xorb %s: %w", job.hash.String(), err))
				return
			}
		}
		if ended {
			return
		}
		select {
		case <-w.ready:
		case <-w.ctx.Done():
			return
		}
	}
}

// xorbReader encodes its chunks, read back from src, as it is read, buffering one encoded chunk or the footer.
type xorbReader struct {
	ctx    context.Context
	src    io.ReaderAt
	chunks []chunkSpan
	buf    *[xet.MaxChunkSize + 8]byte
	enc    *xorb.Encoder
	out    bytes.Buffer

	mu     sync.Mutex
	sealed bool
}

func (x *xorbReader) Read(p []byte) (int, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.sealed {
		return 0, io.ErrClosedPipe
	}
	for x.out.Len() == 0 {
		if len(x.chunks) == 0 {
			return 0, io.EOF
		}
		if err := x.ctx.Err(); err != nil {
			return 0, err
		}
		c := x.chunks[0]
		chunk := x.buf[:c.size]
		if n, err := x.src.ReadAt(chunk, c.offset); n < len(chunk) || (err != nil && err != io.EOF) {
			if err == nil || err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return 0, fmt.Errorf("read chunk at %d: %w", c.offset, err)
		}
		if _, err := x.enc.Write(chunk); err != nil {
			return 0, err
		}
		if x.chunks = x.chunks[1:]; len(x.chunks) == 0 {
			if err := x.enc.Close(); err != nil {
				return 0, err
			}
		}
	}
	return x.out.Read(p)
}

// seal keeps a backend that retained x from reaching src after PutXorb returned.
func (x *xorbReader) seal() {
	x.mu.Lock()
	x.sealed = true
	x.mu.Unlock()
}

// xorbGrouper replicates xorb.GroupChunkIndicesBySize one chunk id at a time.
type xorbGrouper struct {
	target  uint64
	order   []int          // pending ids in insertion order; removed ones are skipped at close
	pending map[int]uint32 // size of each pending id
	size    uint64
}

func newXorbGrouper(target uint64) *xorbGrouper {
	return &xorbGrouper{target: target, pending: map[int]uint32{}}
}

// add appends id after closing and returning the pending group if id overflows it or it holds xet.MaxChunksPerXorb.
func (g *xorbGrouper) add(id int, size uint32) (closed []int) {
	if len(g.pending) > 0 && (g.size+uint64(size) > g.target || len(g.pending) >= xet.MaxChunksPerXorb) {
		closed = g.flush()
	}
	g.order = append(g.order, id)
	g.pending[id] = size
	g.size += uint64(size)
	return closed
}

// remove drops id from the pending group, reporting whether it was pending.
func (g *xorbGrouper) remove(id int) bool {
	size, ok := g.pending[id]
	if ok {
		delete(g.pending, id)
		g.size -= uint64(size)
	}
	return ok
}

// flush closes and returns the pending group, nil when it is empty.
func (g *xorbGrouper) flush() []int {
	var group []int
	for _, id := range g.order {
		if _, ok := g.pending[id]; ok {
			group = append(group, id)
		}
	}
	g.order, g.size = g.order[:0], 0
	clear(g.pending)
	return group
}
