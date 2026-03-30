package upload

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/shard"
	"github.com/wzshiming/xet/pkg/xorb"
)

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
