package upload

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"sync/atomic"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

type stubUploadClientAdapter struct {
	uploadedShard *shard.Shard
}

func (s *stubUploadClientAdapter) UploadXorb(_ context.Context, _ *xorb.Xorb) (*XorbUploadResponse, error) {
	return &XorbUploadResponse{WasInserted: true}, nil
}

func (s *stubUploadClientAdapter) UploadShard(_ context.Context, shardObj *shard.Shard) (*ShardUploadResponse, error) {
	s.uploadedShard = shardObj
	return &ShardUploadResponse{Result: 1}, nil
}

func (s *stubUploadClientAdapter) QueryChunkDeduplication(_ context.Context, _ xet.Hash) (*shard.Shard, error) {
	return nil, nil
}

type batchStubUploadClientAdapter struct {
	stubUploadClientAdapter
	batchCalls  atomic.Int32
	singleCalls atomic.Int32
}

type inspectBatchStubUploadClientAdapter struct {
	stubUploadClientAdapter
	lastBatch []xet.Hash
}

func (s *batchStubUploadClientAdapter) QueryChunkDeduplication(_ context.Context, _ xet.Hash) (*shard.Shard, error) {
	s.singleCalls.Add(1)
	return nil, nil
}

func (s *batchStubUploadClientAdapter) QueryChunksDeduplication(_ context.Context, chunkHashes []xet.Hash) (map[xet.Hash]*DeduplicationResult, error) {
	s.batchCalls.Add(1)
	results := make(map[xet.Hash]*DeduplicationResult, len(chunkHashes))
	for _, chunkHash := range chunkHashes {
		results[chunkHash] = &DeduplicationResult{ChunkHash: chunkHash, IsNew: true}
	}
	return results, nil
}

func (s *inspectBatchStubUploadClientAdapter) QueryChunksDeduplication(_ context.Context, chunkHashes []xet.Hash) (map[xet.Hash]*DeduplicationResult, error) {
	s.lastBatch = append([]xet.Hash(nil), chunkHashes...)
	results := make(map[xet.Hash]*DeduplicationResult, len(chunkHashes))
	for _, chunkHash := range chunkHashes {
		results[chunkHash] = &DeduplicationResult{ChunkHash: chunkHash, IsNew: false, XorbHash: xet.Hash{9}, ChunkIndex: 0}
	}
	return results, nil
}

func TestEncodeDecodeXorb(t *testing.T) {
	// Create a test xorb
	xorbObj := xorb.NewXorb()
	testData := xet.ChunkBytes([]byte("test chunk data"))
	if err := xorbObj.AddChunk(testData); err != nil {
		t.Fatalf("AddChunk failed: %v", err)
	}

	// Encode
	reader, err := EncodeXorb(xorbObj)
	if err != nil {
		t.Fatalf("EncodeXorb failed: %v", err)
	}

	// Decode with correct hash
	decoded, err := DecodeXorb(reader, xorbObj.Hash)
	if err != nil {
		t.Fatalf("DecodeXorb failed: %v", err)
	}

	if decoded.Hash != xorbObj.Hash {
		t.Errorf("Hash mismatch: got %s, want %s", decoded.Hash.String(), xorbObj.Hash.String())
	}

	if len(decoded.Chunks) != len(xorbObj.Chunks) {
		t.Errorf("Chunk count mismatch: got %d, want %d", len(decoded.Chunks), len(xorbObj.Chunks))
	}
}

func TestDecodeXorbHashMismatch(t *testing.T) {
	// Create a test xorb
	xorbObj := xorb.NewXorb()
	testData := xet.ChunkBytes([]byte("test chunk data"))
	if err := xorbObj.AddChunk(testData); err != nil {
		t.Fatalf("AddChunk failed: %v", err)
	}

	// Encode
	reader, err := EncodeXorb(xorbObj)
	if err != nil {
		t.Fatalf("EncodeXorb failed: %v", err)
	}

	// Decode with wrong hash
	wrongHash := xet.Hash{}
	_, err = DecodeXorb(reader, wrongHash)
	if err == nil {
		t.Fatal("Expected error for hash mismatch, got nil")
	}
}

func TestEncodeDecodeShard(t *testing.T) {
	// Create a test shard
	shardObj := shard.NewShard()

	// Encode
	reader, err := EncodeShard(shardObj)
	if err != nil {
		t.Fatalf("EncodeShard failed: %v", err)
	}

	// Read encoded data
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	// Decode
	decoded, err := DecodeShard(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("DecodeShard failed: %v", err)
	}

	if decoded == nil {
		t.Fatal("Decoded shard is nil")
	}
}

func TestEncodeDecodeChunkQueryResponse(t *testing.T) {
	// Create a test shard
	shardObj := shard.NewShard()

	// Encode as chunk query response
	reader, err := EncodeShard(shardObj)
	if err != nil {
		t.Fatalf("EncodeShard failed: %v", err)
	}

	// Decode as chunk query response
	decoded, err := DecodeShard(reader)
	if err != nil {
		t.Fatalf("DecodeShard failed: %v", err)
	}

	if decoded == nil {
		t.Fatal("Decoded shard is nil")
	}
}

func TestDecodeXorbInvalidData(t *testing.T) {
	invalidData := bytes.NewReader([]byte("not a valid xorb"))
	_, err := DecodeXorb(invalidData, xet.Hash{})
	if err == nil {
		t.Fatal("Expected error for invalid xorb data, got nil")
	}
}

func TestDecodeShardInvalidData(t *testing.T) {
	invalidData := bytes.NewReader([]byte("not a valid shard"))
	_, err := DecodeShard(invalidData)
	if err == nil {
		t.Fatal("Expected error for invalid shard data, got nil")
	}
}

func TestDecodeShardRejectsFooterInUploadPayload(t *testing.T) {
	sh := shard.NewShard()
	sh.Footer = &shard.Footer{Version: 1}

	r, err := shard.Encode(sh, true)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	_, err = DecodeShard(r)
	if err == nil {
		t.Fatal("expected error for shard footer in upload payload")
	}
}

func TestDecodeShardRejectsNonZeroCASFlags(t *testing.T) {
	sh := shard.NewShard()
	sh.Files = []shard.FileBlock{{
		FileHash: xet.Hash{1},
		Entries: []shard.FileDataSequenceEntry{{
			CASHash:          xet.Hash{2},
			CASFlags:         1,
			UnpackedSegBytes: 1,
			ChunkIndexStart:  0,
			ChunkIndexEnd:    1,
		}},
	}}
	sh.CASInfos = []shard.CASBlock{{
		CASHash:        xet.Hash{2},
		NumBytesInCAS:  1,
		NumBytesOnDisk: 1,
		Chunks: []shard.CASChunkSequenceEntry{{
			ChunkHash:        xet.Hash{3},
			ByteRangeStart:   0,
			UnpackedSegBytes: 1,
		}},
	}}

	r, err := EncodeShard(sh)
	if err != nil {
		t.Fatalf("EncodeShard failed: %v", err)
	}

	_, err = DecodeShard(r)
	if err == nil {
		t.Fatal("expected error for non-zero CAS flags")
	}
}

func TestDecodeShardRejectsNonContiguousByteRangeStart(t *testing.T) {
	sh := shard.NewShard()
	sh.Files = []shard.FileBlock{{
		FileHash: xet.Hash{1},
	}}
	sh.CASInfos = []shard.CASBlock{{
		CASHash:        xet.Hash{2},
		NumBytesInCAS:  2,
		NumBytesOnDisk: 2,
		Chunks: []shard.CASChunkSequenceEntry{
			{ChunkHash: xet.Hash{3}, ByteRangeStart: 0, UnpackedSegBytes: 1},
			{ChunkHash: xet.Hash{4}, ByteRangeStart: 2, UnpackedSegBytes: 1},
		},
	}}

	r, err := EncodeShard(sh)
	if err != nil {
		t.Fatalf("EncodeShard failed: %v", err)
	}

	_, err = DecodeShard(r)
	if err == nil {
		t.Fatal("expected error for non-contiguous byte range starts")
	}
}

func TestXorbUploadResponseJSON(t *testing.T) {
	resp := XorbUploadResponse{WasInserted: true}
	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded XorbUploadResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if decoded.WasInserted != resp.WasInserted {
		t.Errorf("WasInserted mismatch: got %v, want %v", decoded.WasInserted, resp.WasInserted)
	}
}

func TestShardUploadResponseJSON(t *testing.T) {
	resp := ShardUploadResponse{Result: 1}
	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded ShardUploadResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if decoded.Result != resp.Result {
		t.Errorf("Result mismatch: got %d, want %d", decoded.Result, resp.Result)
	}
}

func TestUploadFilesPrefersBatchChunkDeduplication(t *testing.T) {
	adapter := &batchStubUploadClientAdapter{}
	data := bytes.Repeat([]byte("batch-dedup-test-"), 2048)

	if _, err := UploadFiles(context.Background(), adapter, []io.Reader{bytes.NewReader(data)}); err != nil {
		t.Fatalf("UploadFiles failed: %v", err)
	}

	if adapter.batchCalls.Load() == 0 {
		t.Fatal("expected batch dedup query to be used")
	}
	if adapter.singleCalls.Load() != 0 {
		t.Fatalf("expected single dedup query not to be used, got %d calls", adapter.singleCalls.Load())
	}
}

func TestDeduplicateChunksUsesSparseGlobalProbes(t *testing.T) {
	adapter := &inspectBatchStubUploadClientAdapter{}

	chunkHashes := make([]xet.Hash, 600)
	for i := range chunkHashes {
		binary.LittleEndian.PutUint64(chunkHashes[i][24:32], uint64(i*13+1))
	}

	// Make only these positions eligible by setting value % 1024 == 0.
	binary.LittleEndian.PutUint64(chunkHashes[300][24:32], 1024)
	binary.LittleEndian.PutUint64(chunkHashes[560][24:32], 2048)

	cache := make(map[xet.Hash]*DeduplicationResult)
	deduplicateChunks(context.Background(), adapter, cache, chunkHashes, 4)

	if len(adapter.lastBatch) != 3 {
		t.Fatalf("expected 3 sparse probes (first, eligible@300, eligible@560), got %d", len(adapter.lastBatch))
	}
	if adapter.lastBatch[0] != chunkHashes[0] || adapter.lastBatch[1] != chunkHashes[300] || adapter.lastBatch[2] != chunkHashes[560] {
		t.Fatal("unexpected sparse probe positions")
	}

	if len(cache) != len(chunkHashes) {
		t.Fatalf("cache size mismatch: got %d want %d", len(cache), len(chunkHashes))
	}

	if cache[chunkHashes[0]].IsNew {
		t.Fatal("expected probed chunk to be deduplicated")
	}
	if !cache[chunkHashes[1]].IsNew {
		t.Fatal("expected non-probed chunk to stay new")
	}
}

func TestBuildAndUploadShardExcludesDedupedCASBlocks(t *testing.T) {
	adapter := &stubUploadClientAdapter{}
	fileHash := xet.Hash{1}
	oldXorbHash := xet.Hash{2}
	newXorbHash := xet.Hash{3}

	allChunks := []chunkInfo{
		{
			Data: []byte("existing-deduped"),
			Hash: xet.Hash{10},
			Dedup: &DeduplicationResult{
				ChunkHash:  xet.Hash{10},
				IsNew:      false,
				XorbHash:   oldXorbHash,
				ChunkIndex: 7,
			},
		},
		{
			Data: []byte("new-upload"),
			Hash: xet.Hash{11},
			Dedup: &DeduplicationResult{
				ChunkHash:  xet.Hash{11},
				IsNew:      true,
				XorbHash:   newXorbHash,
				ChunkIndex: 0,
			},
		},
	}

	fileChunkRanges := map[int][]int{0: {0, 1}}
	err := buildAndUploadShard(context.Background(), adapter, []xet.Hash{fileHash}, nil, allChunks, fileChunkRanges)
	if err != nil {
		t.Fatalf("buildAndUploadShard failed: %v", err)
	}

	if adapter.uploadedShard == nil {
		t.Fatal("expected shard upload to be called")
	}

	if len(adapter.uploadedShard.CASInfos) != 1 {
		t.Fatalf("expected exactly one CAS block for newly uploaded xorb, got %d", len(adapter.uploadedShard.CASInfos))
	}

	if adapter.uploadedShard.CASInfos[0].CASHash != newXorbHash {
		t.Fatalf("unexpected CAS hash: got %s want %s", adapter.uploadedShard.CASInfos[0].CASHash, newXorbHash)
	}
}
