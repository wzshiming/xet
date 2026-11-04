package storage

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/pool"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

const (
	// decodeWindow bounds the chunks of one reader between fetch and Read;
	// 32 is the knee of the #284 sweep, at up to 4 MiB of buffers per reader.
	decodeWindow = 32
	// maxDecodeWorkers bounds the goroutines decoding one reader's chunks.
	maxDecodeWorkers = 4
)

// frameBufSize fits any packed or decoded chunk; pool's chunk buffers have this size.
const frameBufSize = xet.MaxChunkSize + xorb.ChunkHeaderSize

type frameBuf = [frameBufSize]byte

// decodeWorkers is the number of goroutines decoding one reader's chunks.
func decodeWorkers() int {
	return min(runtime.GOMAXPROCS(0), maxDecodeWorkers)
}

// frame carries one chunk: the fetcher reads it packed into in, a worker
// decodes it into out and returns in, and Read consumes data until it
// releases the frame. An uncompressed chunk keeps in, which data aliases.
type frame struct {
	in, out *frameBuf
	packed  int
	header  xorb.ChunkHeader
	term    *shard.FileDataSequenceEntry
	index   uint32
	data    []byte
	err     error
	ready   chan struct{} // signalled once data or err is set
}

// pipeline delivers the decoded chunks of a run of terms in file order: one
// fetcher reads each term through one bounded seeker read, decodeWorkers()
// workers decode, and decodeWindow frames bound what is in flight.
type pipeline struct {
	ctx     context.Context
	cancel  context.CancelFunc
	storage Storage
	wg      sync.WaitGroup

	frames  [decodeWindow]frame
	free    chan *frame // frames the fetcher may fill
	jobs    chan *frame // filled frames awaiting a worker
	ordered chan *frame // filled frames in file order, awaiting Read
	err     error       // why the fetcher stopped early; set before ordered closes
}

// startPipeline fetches terms under a child of ctx, which stop cancels.
func startPipeline(ctx context.Context, stor Storage, terms []shard.FileDataSequenceEntry) *pipeline {
	p := &pipeline{
		storage: stor,
		free:    make(chan *frame, decodeWindow),
		jobs:    make(chan *frame, decodeWindow),
		ordered: make(chan *frame, decodeWindow),
	}
	p.ctx, p.cancel = context.WithCancel(ctx)
	for i := range p.frames {
		p.frames[i].ready = make(chan struct{}, 1)
		p.free <- &p.frames[i]
	}
	p.wg.Go(func() { p.fetch(terms) })
	for range decodeWorkers() {
		p.wg.Go(p.decode)
	}
	return p
}

// fetch reads the frames of every non-empty term in order; an error ends
// the stream after the frames before it.
func (p *pipeline) fetch(terms []shard.FileDataSequenceEntry) {
	defer close(p.jobs)
	for i := range terms {
		if terms[i].UnpackedSegBytes == 0 {
			continue
		}
		if err := p.fetchTerm(&terms[i]); err != nil {
			p.err = err
			break
		}
	}
	close(p.ordered)
}

// fetchTerm reads term's chunks through one bounded seeker read, each frame
// with one ReadFull of the packed size its chunk offsets give.
func (p *pipeline) fetchTerm(term *shard.FileDataSequenceEntry) error {
	// Storage is touched only once the window has room for the term's first frame.
	fr, err := p.acquire()
	if err != nil {
		return err
	}
	offsets, err := p.storage.GetXorbChunkOffsets(p.ctx, term.CASHash)
	if err != nil {
		return fmt.Errorf("locate xorb %s chunks: %w", term.CASHash, err)
	}
	start, end, err := xorb.ChunkDataRangeFromOffsets(offsets, term.ChunkIndexStart, term.ChunkIndexEnd)
	if err != nil {
		return fmt.Errorf("locate xorb %s chunks: %w", term.CASHash, err)
	}
	rc, err := openXorbRange(p.ctx, p.storage, term.CASHash, start, end)
	if err != nil {
		return fmt.Errorf("read xorb %s: %w", term.CASHash, err)
	}
	defer rc.Close()

	var unpacked uint64
	for i := term.ChunkIndexStart; i < term.ChunkIndexEnd; i++ {
		if i > term.ChunkIndexStart {
			if fr, err = p.acquire(); err != nil {
				return err
			}
		}
		packed := int64(offsets[i]) - start
		start = int64(offsets[i])
		if packed < xorb.ChunkHeaderSize || packed > frameBufSize {
			return fmt.Errorf("read xorb %s chunk %d: invalid packed size %d", term.CASHash, i, packed)
		}
		fr.in = pool.GetChunkBuf()
		if _, err := io.ReadFull(rc, fr.in[:packed]); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return fmt.Errorf("read xorb %s chunk %d: %w", term.CASHash, i, err)
		}
		h, err := xorb.ParseChunkHeader(fr.in[:xorb.ChunkHeaderSize])
		if err != nil {
			return fmt.Errorf("read xorb %s chunk %d: %w", term.CASHash, i, err)
		}
		if int64(h.CompressedSize)+xorb.ChunkHeaderSize != packed {
			return fmt.Errorf("read xorb %s chunk %d: header declares %d compressed bytes in a %d-byte frame", term.CASHash, i, h.CompressedSize, packed)
		}
		unpacked += uint64(h.UncompressedSize)
		if unpacked > uint64(term.UnpackedSegBytes) {
			return fmt.Errorf("read xorb %s chunk %d: chunks exceed the term's %d bytes", term.CASHash, i, term.UnpackedSegBytes)
		}
		fr.packed, fr.header, fr.term, fr.index = int(packed), h, term, i
		// Neither send blocks: a channel never holds more than the window's other frames.
		p.ordered <- fr
		p.jobs <- fr
	}
	if unpacked != uint64(term.UnpackedSegBytes) {
		return fmt.Errorf("reconstructed term of xorb %s has %d bytes, expected %d", term.CASHash, unpacked, term.UnpackedSegBytes)
	}
	return nil
}

// acquire waits for a free window slot.
func (p *pipeline) acquire() (*frame, error) {
	if err := p.ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case fr := <-p.free:
		return fr, nil
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	}
}

// decode decodes filled frames until the fetcher closes jobs.
func (p *pipeline) decode() {
	for fr := range p.jobs {
		body := fr.in[xorb.ChunkHeaderSize:fr.packed]
		var data []byte
		var err error
		if fr.header.Compression == xorb.CompressionNone {
			data, err = xorb.DecodeChunk(nil, fr.header, body)
		} else {
			fr.out = pool.GetChunkBuf()
			data, err = xorb.DecodeChunk(fr.out[:], fr.header, body)
			pool.PutChunkBuf(fr.in)
			fr.in = nil
		}
		if err != nil {
			err = fmt.Errorf("decode xorb %s chunk %d: %w", fr.term.CASHash, fr.index, err)
		}
		fr.data, fr.err = data, err
		fr.ready <- struct{}{}
	}
}

// next waits for the next decoded chunk in file order.
func (p *pipeline) next(ctx context.Context) (*frame, error) {
	var fr *frame
	var ok bool
	select {
	case fr, ok = <-p.ordered:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if !ok {
		if p.err != nil {
			return nil, p.err
		}
		return nil, io.ErrUnexpectedEOF
	}
	select {
	case <-fr.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if fr.err != nil {
		return nil, fr.err
	}
	// A chunk ready alongside the cancel is not served.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return fr, nil
}

// drain waits for the fetcher to stop and returns why it stopped early, if it
// did. Past the file's end every frame is empty, so drain releases those still
// delivered and the fetcher never runs out of free slots.
func (p *pipeline) drain(ctx context.Context) error {
	for {
		var fr *frame
		var ok bool
		select {
		case fr, ok = <-p.ordered:
		case <-ctx.Done():
			return ctx.Err()
		}
		if !ok {
			if p.err != nil {
				return p.err
			}
			// A fetcher that finished alongside the cancel does not end the drain cleanly.
			return ctx.Err()
		}
		select {
		case <-fr.ready:
		case <-ctx.Done():
			return ctx.Err()
		}
		if fr.err != nil {
			return fr.err
		}
		p.release(fr)
	}
}

// release returns a delivered frame's buffers and window slot.
func (p *pipeline) release(fr *frame) {
	fr.putBufs()
	p.free <- fr
}

// stop cancels and joins the fetcher, which closes its term's seeker, and the
// workers, then returns every buffer the frames still hold.
func (p *pipeline) stop() {
	p.cancel()
	p.wg.Wait()
	for i := range p.frames {
		p.frames[i].putBufs()
	}
}

// putBufs returns whichever buffers fr holds to the pool, which a nil Put
// would poison, and drops data, which aliases one of them.
func (fr *frame) putBufs() {
	if fr.in != nil {
		pool.PutChunkBuf(fr.in)
	}
	if fr.out != nil {
		pool.PutChunkBuf(fr.out)
	}
	fr.in, fr.out, fr.data = nil, nil, nil
}
