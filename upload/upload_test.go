package upload

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	reader, err := EncodeChunkQueryResponse(shardObj)
	if err != nil {
		t.Fatalf("EncodeChunkQueryResponse failed: %v", err)
	}

	// Decode as chunk query response
	decoded, err := DecodeChunkQueryResponse(reader)
	if err != nil {
		t.Fatalf("DecodeChunkQueryResponse failed: %v", err)
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

func TestUploadFilesSetsFileMetadataExtSHA256(t *testing.T) {
	data := []byte("upload metadata ext sha256 test")
	expectedSHA256 := sha256.Sum256(data)

	adapter := &stubUploadClientAdapter{}
	if _, err := UploadFiles(context.Background(), adapter, []io.Reader{bytes.NewReader(data)}, WithEnableSHA256(true)); err != nil {
		t.Fatalf("UploadFiles failed: %v", err)
	}

	if adapter.uploadedShard == nil {
		t.Fatal("expected shard upload to be called")
	}
	if len(adapter.uploadedShard.Files) != 1 {
		t.Fatalf("unexpected file blocks count: got %d want 1", len(adapter.uploadedShard.Files))
	}

	file := adapter.uploadedShard.Files[0]
	if file.Flags&shard.FileWithMetadataExt == 0 {
		t.Fatalf("expected FileWithMetadataExt flag to be set, got flags=%032b", uint32(file.Flags))
	}
	if file.MetadataExt == nil {
		t.Fatal("expected metadata extension to be present")
	}
	if !bytes.Equal(file.MetadataExt.SHA256Hash[:], expectedSHA256[:]) {
		t.Fatalf("sha256 mismatch: got %x want %x", file.MetadataExt.SHA256Hash, expectedSHA256)
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
		chunkHashes[i][0] = byte(i)
		chunkHashes[i][1] = byte(i >> 8)
		chunkHashes[i][2] = byte(i >> 16)
		chunkHashes[i][3] = byte(i >> 24)
	}

	cache := make(map[xet.Hash]*DeduplicationResult)
	deduplicateChunks(context.Background(), adapter, cache, chunkHashes, 4)

	if len(adapter.lastBatch) != 3 {
		t.Fatalf("expected 3 sparse probes (0,256,512), got %d", len(adapter.lastBatch))
	}
	if adapter.lastBatch[0] != chunkHashes[0] || adapter.lastBatch[1] != chunkHashes[256] || adapter.lastBatch[2] != chunkHashes[512] {
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
