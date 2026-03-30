package client

import (
	"context"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/reconstruction"
	"github.com/wzshiming/xet/pkg/xorb"
)

// DownloadSession represents a download session
type DownloadSession struct {
	client     *Client
	chunkCache map[xet.Hash][]byte
}

// mapChunkCache wraps a map to implement reconstruction.ChunkCache
type mapChunkCache map[xet.Hash][]byte

func (m mapChunkCache) Get(hash xet.Hash) ([]byte, bool) {
	if m == nil {
		return nil, false
	}
	data, ok := m[hash]
	return data, ok
}

func (m mapChunkCache) Set(hash xet.Hash, data []byte) {
	if m != nil {
		m[hash] = data
	}
}

// clientXorbFetcher wraps a Client to implement reconstruction.XorbFetcher
type clientXorbFetcher struct {
	client *Client
}

func (f *clientXorbFetcher) FetchXorb(ctx context.Context, url string, rangeStart, rangeEnd int64) (*xorb.Xorb, error) {
	var opts []ReqOpt
	if rangeStart != 0 || rangeEnd != 0 {
		opts = append(opts, WithRange(rangeStart, rangeEnd))
	}
	return f.client.DownloadXorb(ctx, url, opts...)
}

// DownloadSessionOptions configures a download session
type DownloadSessionOptions struct {
	Client        *Client
	EnableCaching bool
}

// NewDownloadSession creates a new download session
func NewDownloadSession(opts DownloadSessionOptions) *DownloadSession {
	var cache map[xet.Hash][]byte
	if opts.EnableCaching {
		cache = make(map[xet.Hash][]byte)
	}

	return &DownloadSession{
		client:     opts.Client,
		chunkCache: cache,
	}
}

// DownloadFile downloads and reconstructs a file from its hash, automatically falling back to V1 if V2 is not supported
func (s *DownloadSession) DownloadFile(ctx context.Context, fileHash xet.Hash, opts ...ReqOpt) (io.Reader, int64, error) {
	r, size, err := s.DownloadFileV2(ctx, fileHash, opts...)
	if err != nil {
		if err == errNotFound {
			return s.DownloadFileV1(ctx, fileHash, opts...)
		}
		return nil, 0, err
	}
	return r, size, nil
}

// DownloadFileV1 downloads and reconstructs a file from its hash
func (s *DownloadSession) DownloadFileV1(ctx context.Context, fileHash xet.Hash, opts ...ReqOpt) (io.Reader, int64, error) {
	// Step 1: Query reconstruction
	resp, err := s.client.GetReconstructionV1(ctx, fileHash, opts...)
	if err != nil {
		return nil, 0, fmt.Errorf("query reconstruction: %w", err)
	}

	// Convert to reconstruction package types
	reconResp := &reconstruction.ReconstructionResponse{
		OffsetIntoFirstRange: resp.OffsetIntoFirstRange,
		Terms:                make([]reconstruction.Term, len(resp.Terms)),
		FetchInfo:            make(map[string][]reconstruction.FetchInfoEntry),
	}

	for i, term := range resp.Terms {
		reconResp.Terms[i] = reconstruction.Term{
			Hash:           term.Hash,
			UnpackedLength: term.UnpackedLength,
			Range: reconstruction.ChunkRange{
				Start: term.Range.Start,
				End:   term.Range.End,
			},
		}
	}

	for hash, entries := range resp.FetchInfo {
		reconEntries := make([]reconstruction.FetchInfoEntry, len(entries))
		for i, entry := range entries {
			reconEntries[i] = reconstruction.FetchInfoEntry{
				Range: reconstruction.ChunkRange{
					Start: entry.Range.Start,
					End:   entry.Range.End,
				},
				URL: entry.URL,
				URLRange: reconstruction.ByteRange{
					Start: entry.URLRange.Start,
					End:   entry.URLRange.End,
				},
			}
		}
		reconResp.FetchInfo[hash] = reconEntries
	}

	// Create reader using reconstruction package
	var cache reconstruction.ChunkCache
	if s.chunkCache != nil {
		cache = mapChunkCache(s.chunkCache)
	}

	fetcher := &clientXorbFetcher{client: s.client}
	reader, length := reconstruction.ReaderV1(ctx, reconResp, fetcher, cache)
	return reader, length, nil
}

// DownloadFileV2 downloads and reconstructs a file from its hash using the V2 API
func (s *DownloadSession) DownloadFileV2(ctx context.Context, fileHash xet.Hash, opts ...ReqOpt) (io.Reader, int64, error) {
	resp, err := s.client.GetReconstructionV2(ctx, fileHash, opts...)
	if err != nil {
		return nil, 0, fmt.Errorf("query reconstruction v2: %w", err)
	}

	// Convert to reconstruction package types
	reconResp := &reconstruction.ReconstructionResponseV2{
		OffsetIntoFirstRange: resp.OffsetIntoFirstRange,
		Terms:                make([]reconstruction.Term, len(resp.Terms)),
		Xorbs:                make(map[string][]reconstruction.XorbMultiRangeFetch),
	}

	for i, term := range resp.Terms {
		reconResp.Terms[i] = reconstruction.Term{
			Hash:           term.Hash,
			UnpackedLength: term.UnpackedLength,
			Range: reconstruction.ChunkRange{
				Start: term.Range.Start,
				End:   term.Range.End,
			},
		}
	}

	for hash, fetches := range resp.Xorbs {
		reconFetches := make([]reconstruction.XorbMultiRangeFetch, len(fetches))
		for i, fetch := range fetches {
			ranges := make([]reconstruction.XorbRangeDescriptor, len(fetch.Ranges))
			for j, r := range fetch.Ranges {
				ranges[j] = reconstruction.XorbRangeDescriptor{
					Chunks: reconstruction.ChunkRange{
						Start: r.Chunks.Start,
						End:   r.Chunks.End,
					},
					Bytes: reconstruction.ByteRange{
						Start: r.Bytes.Start,
						End:   r.Bytes.End,
					},
				}
			}
			reconFetches[i] = reconstruction.XorbMultiRangeFetch{
				URL:    fetch.URL,
				Ranges: ranges,
			}
		}
		reconResp.Xorbs[hash] = reconFetches
	}

	// Create reader using reconstruction package
	var cache reconstruction.ChunkCache
	if s.chunkCache != nil {
		cache = mapChunkCache(s.chunkCache)
	}

	fetcher := &clientXorbFetcher{client: s.client}
	reader, length := reconstruction.ReaderV2(ctx, reconResp, fetcher, cache)
	return reader, length, nil
}
