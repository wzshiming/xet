package e2e

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/wzshiming/xet/pkg/gearhash"
	"github.com/wzshiming/xet/pkg/merkle"
	"github.com/wzshiming/xet/pkg/xet"
)

// TestQwen25_05B_ModelSafetensors downloads the Qwen2.5-0.5B model.safetensors
// file from Hugging Face and verifies that the computed XET file hash matches
// the expected value.
//
// This test is skipped in short mode (-short) since it downloads a large file (~1 GB).
// Run with: go test -v -timeout 30m ./e2e/ -run TestQwen25_05B_ModelSafetensors
func TestQwen25_05B_ModelSafetensors(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode (requires large file download)")
	}

	const (
		fileURL      = "https://huggingface.co/Qwen/Qwen2.5-0.5B/resolve/main/model.safetensors"
		expectedHash = "aeb713fdee2a083353a999d46771858f952744509d8af12868a1e95e9c45c7e3"
	)

	// Download the file to a temporary location
	t.Log("Downloading", fileURL)
	tmpFile, err := os.CreateTemp("", "model-*.safetensors")
	if err != nil {
		t.Fatalf("creating temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	resp, err := http.Get(fileURL)
	if err != nil {
		t.Skipf("skipping: unable to download file (network unavailable): %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.StatusCode)
	}

	written, err := io.Copy(tmpFile, resp.Body)
	if err != nil {
		t.Fatalf("writing file: %v", err)
	}
	t.Logf("Downloaded %d bytes (%s)", written, humanSize(written))

	// Read the file into memory for chunking
	data, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("reading file: %v", err)
	}

	// Chunk the file using Gearhash
	chunks := gearhash.ChunkFile(data)
	t.Logf("Chunked into %d chunks", len(chunks))

	// Compute chunk hashes and sizes
	chunkHashes := make([][32]byte, len(chunks))
	chunkSizes := make([]uint64, len(chunks))
	for i, chunk := range chunks {
		chunkHashes[i] = xet.ComputeChunkHash(chunk)
		chunkSizes[i] = uint64(len(chunk))
	}

	// Compute file hash (Merkle root + final keyed hash)
	fileHash := merkle.ComputeFileHash(chunkHashes, chunkSizes)
	fileHashXET := xet.HashToString(fileHash)

	t.Logf("Computed file hash (XET str): %s", fileHashXET)

	if fileHashXET != expectedHash {
		t.Errorf("file hash mismatch:\n  got:  %s\n  want: %s", fileHashXET, expectedHash)
	}
}

func humanSize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
