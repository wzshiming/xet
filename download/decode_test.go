package download

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"
)

type blockingDownloadClient struct {
	mu        sync.Mutex
	responses map[string]*xorb.Xorb
	release   chan struct{}
	started   chan string
	active    int
	maxActive int
}

func newBlockingDownloadClient(responses map[string]*xorb.Xorb) *blockingDownloadClient {
	return &blockingDownloadClient{
		responses: responses,
		release:   make(chan struct{}),
		started:   make(chan string, len(responses)),
	}
}

func (c *blockingDownloadClient) DownloadXorb(ctx context.Context, url string, header http.Header) (*xorb.Xorb, error) {
	key := url + "|" + header.Get("Range")

	c.mu.Lock()
	c.active++
	if c.active > c.maxActive {
		c.maxActive = c.active
	}
	c.mu.Unlock()

	c.started <- key

	defer func() {
		c.mu.Lock()
		c.active--
		c.mu.Unlock()
	}()

	select {
	case <-c.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	resp, ok := c.responses[key]
	if !ok {
		return nil, errors.New("unexpected xorb request")
	}

	return resp, nil
}

func (c *blockingDownloadClient) waitForStarts(t *testing.T, count int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for range count {
		select {
		case <-c.started:
		case <-deadline:
			t.Fatalf("timed out waiting for %d concurrent downloads", count)
		}
	}
}

func (c *blockingDownloadClient) releaseAll() {
	close(c.release)
}

func (c *blockingDownloadClient) MaxActive() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxActive
}

func makeTestXorb(t *testing.T, chunks ...string) *xorb.Xorb {
	t.Helper()
	xorbObj := xorb.NewXorb()
	for _, chunk := range chunks {
		if err := xorbObj.AddChunk(xet.ChunkBytes([]byte(chunk))); err != nil {
			t.Fatalf("add chunk: %v", err)
		}
	}
	return xorbObj
}

func TestReaderV1PrefetchesConcurrently(t *testing.T) {
	client := newBlockingDownloadClient(map[string]*xorb.Xorb{
		"https://example.com/xorb-1|bytes=0-10":  makeTestXorb(t, "alpha"),
		"https://example.com/xorb-2|bytes=11-20": makeTestXorb(t, "beta"),
		"https://example.com/xorb-3|bytes=21-30": makeTestXorb(t, "gamma"),
	})

	reconstruction := &ReconstructionResponse{
		Terms: []Term{
			{Hash: "xorb-1", Range: ChunkRange{Start: 0, End: 1}, UnpackedLength: 5},
			{Hash: "xorb-2", Range: ChunkRange{Start: 0, End: 1}, UnpackedLength: 4},
			{Hash: "xorb-3", Range: ChunkRange{Start: 0, End: 1}, UnpackedLength: 5},
		},
		FetchInfo: map[string][]FetchInfoEntry{
			"xorb-1": {{Range: ChunkRange{Start: 0, End: 1}, URL: "https://example.com/xorb-1", URLRange: ByteRange{Start: 0, End: 10}}},
			"xorb-2": {{Range: ChunkRange{Start: 0, End: 1}, URL: "https://example.com/xorb-2", URLRange: ByteRange{Start: 11, End: 20}}},
			"xorb-3": {{Range: ChunkRange{Start: 0, End: 1}, URL: "https://example.com/xorb-3", URLRange: ByteRange{Start: 21, End: 30}}},
		},
	}

	reader := NewReaderV1(context.Background(), client, reconstruction, WithConcurrency(3))
	client.waitForStarts(t, 3)
	client.releaseAll()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}

	if got, want := string(data), "alphabetagamma"; got != want {
		t.Fatalf("unexpected data: got %q want %q", got, want)
	}

	if client.MaxActive() < 2 {
		t.Fatalf("expected at least 2 concurrent downloads, got %d", client.MaxActive())
	}
}

func TestReaderV2PrefetchesConcurrently(t *testing.T) {
	client := newBlockingDownloadClient(map[string]*xorb.Xorb{
		"https://example.com/xorb-a|bytes=0-10":  makeTestXorb(t, "one"),
		"https://example.com/xorb-b|bytes=11-20": makeTestXorb(t, "two"),
		"https://example.com/xorb-c|bytes=21-30": makeTestXorb(t, "three"),
	})

	reconstruction := &ReconstructionResponseV2{
		Terms: []Term{
			{Hash: "xorb-a", Range: ChunkRange{Start: 0, End: 1}, UnpackedLength: 3},
			{Hash: "xorb-b", Range: ChunkRange{Start: 0, End: 1}, UnpackedLength: 3},
			{Hash: "xorb-c", Range: ChunkRange{Start: 0, End: 1}, UnpackedLength: 5},
		},
		Xorbs: map[string][]XorbMultiRangeFetch{
			"xorb-a": {{URL: "https://example.com/xorb-a", Ranges: []XorbRangeDescriptor{{Chunks: ChunkRange{Start: 0, End: 1}, Bytes: ByteRange{Start: 0, End: 10}}}}},
			"xorb-b": {{URL: "https://example.com/xorb-b", Ranges: []XorbRangeDescriptor{{Chunks: ChunkRange{Start: 0, End: 1}, Bytes: ByteRange{Start: 11, End: 20}}}}},
			"xorb-c": {{URL: "https://example.com/xorb-c", Ranges: []XorbRangeDescriptor{{Chunks: ChunkRange{Start: 0, End: 1}, Bytes: ByteRange{Start: 21, End: 30}}}}},
		},
	}

	reader := NewReaderV2(context.Background(), client, reconstruction, WithConcurrency(3))
	client.waitForStarts(t, 3)
	client.releaseAll()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}

	if got, want := string(data), "onetwothree"; got != want {
		t.Fatalf("unexpected data: got %q want %q", got, want)
	}

	if client.MaxActive() < 2 {
		t.Fatalf("expected at least 2 concurrent downloads, got %d", client.MaxActive())
	}
}
