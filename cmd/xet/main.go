package main

import (
	"fmt"
	"os"

	"github.com/wzshiming/xet/pkg/gearhash"
	"github.com/wzshiming/xet/pkg/xet"
	"github.com/wzshiming/xet/pkg/xorb"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: xet <file>")
		fmt.Println("  Chunks a file using the XET protocol and displays information")
		os.Exit(1)
	}

	filename := os.Args[1]

	// Read file
	data, err := os.ReadFile(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("File: %s (%d bytes)\n\n", filename, len(data))

	// Chunk the data
	fmt.Println("=== Chunking ===")
	chunks := gearhash.ChunkData(data)
	fmt.Printf("Number of chunks: %d\n", len(chunks))

	// Compute chunk hashes
	chunkHashes := make([]xet.Hash, len(chunks))
	for i, chunk := range chunks {
		chunkHashes[i] = xet.ComputeChunkHash(chunk.Data)
		fmt.Printf("Chunk %d: offset=%d, size=%d, hash=%s\n",
			i, chunk.Offset, len(chunk.Data), chunkHashes[i].String())
	}

	// Create an xorb
	fmt.Println("\n=== Creating Xorb ===")
	xorbObj := xorb.NewXorb()
	for _, chunk := range chunks {
		if err := xorbObj.AddChunk(chunk.Data); err != nil {
			fmt.Fprintf(os.Stderr, "Error adding chunk: %v\n", err)
			os.Exit(1)
		}
	}

	// Serialize the xorb
	serialized, err := xorbObj.Serialize()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error serializing xorb: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Xorb hash: %s\n", xorbObj.Hash.String())
	fmt.Printf("Xorb size: %d bytes\n", len(serialized))

	// Compute file hash using merkle tree
	fmt.Println("\n=== File Hash ===")
	chunkSizes := make([]uint64, len(chunks))
	for i, chunk := range chunks {
		chunkSizes[i] = uint64(len(chunk.Data))
	}

	// For file hash, we need to apply ZERO_KEY to the merkle root
	// The xorb hash is the merkle root, so we apply the final keyed hash
	fileHash := xet.ComputeFileHash(xorbObj.Hash[:])
	fmt.Printf("File hash: %s\n", fileHash.String())

	// Test deserialization
	fmt.Println("\n=== Testing Deserialization ===")
	deserialized, err := xorb.Deserialize(serialized)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error deserializing xorb: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Deserialized xorb hash: %s\n", deserialized.Hash.String())
	fmt.Printf("Number of chunks: %d\n", len(deserialized.Chunks))

	// Verify data integrity
	fmt.Println("\n=== Verifying Data Integrity ===")
	var reconstructed []byte
	for i, chunk := range deserialized.Chunks {
		reconstructed = append(reconstructed, chunk.UncompressedData...)

		// Verify chunk hash
		computedHash := xet.ComputeChunkHash(chunk.UncompressedData)
		if computedHash != chunk.Hash {
			fmt.Printf("ERROR: Chunk %d hash mismatch!\n", i)
			os.Exit(1)
		}
	}

	if len(reconstructed) != len(data) {
		fmt.Printf("ERROR: Reconstructed size mismatch: %d vs %d\n", len(reconstructed), len(data))
		os.Exit(1)
	}

	// Verify byte-by-byte
	allMatch := true
	for i := range data {
		if reconstructed[i] != data[i] {
			allMatch = false
			break
		}
	}

	if allMatch {
		fmt.Println("✓ Data integrity verified - all chunks match!")
	} else {
		fmt.Println("✗ ERROR: Reconstructed data does not match original!")
		os.Exit(1)
	}

	fmt.Println("\n=== Summary ===")
	fmt.Printf("Original size:  %d bytes\n", len(data))
	fmt.Printf("Xorb size:      %d bytes\n", len(serialized))
	fmt.Printf("Compression:    %.2f%%\n", float64(len(serialized))*100/float64(len(data)))
}
