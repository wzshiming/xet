// Package shard_example demonstrates how to use the shard serialization format
package main

import (
	"fmt"
	"log"

	"github.com/wzshiming/xet/pkg/shard"
	"github.com/wzshiming/xet/pkg/xet"
)

func main() {
	// Create a new shard
	s := shard.NewShard()

	// Example 1: Create a file block
	fmt.Println("=== Creating File Block ===")

	fileHash := xet.Hash{}
	for i := range fileHash {
		fileHash[i] = byte(i)
	}

	casHash := xet.Hash{}
	for i := range casHash {
		casHash[i] = byte(i + 32)
	}

	fb := shard.FileBlock{
		FileHash: fileHash,
		Flags:    0,
		Entries: []shard.FileDataSequenceEntry{
			{
				CASHash:          casHash,
				CASFlags:         0,
				UnpackedSegBytes: 1024,
				ChunkIndexStart:  0,
				ChunkIndexEnd:    5,
			},
		},
	}

	s.AddFile(fb)
	fmt.Printf("Added file block with hash: %s\n", fileHash.String())

	// Example 2: Create a CAS block
	fmt.Println("\n=== Creating CAS Block ===")

	chunkHash := xet.Hash{}
	for i := range chunkHash {
		chunkHash[i] = byte(i + 64)
	}

	cb := shard.CASBlock{
		CASHash:        casHash,
		CASFlags:       0,
		NumBytesInCAS:  1024,
		NumBytesOnDisk: 512,
		Chunks: []shard.CASChunkSequenceEntry{
			{
				ChunkHash:        chunkHash,
				ByteRangeStart:   0,
				UnpackedSegBytes: 1024,
				Flags:            shard.ChunkGlobalDedupEligible,
			},
		},
	}

	s.AddCASBlock(cb)
	fmt.Printf("Added CAS block with hash: %s\n", casHash.String())

	// Example 3: Serialize without footer (for upload API)
	fmt.Println("\n=== Serializing (Upload API format) ===")

	data, err := s.Serialize()
	if err != nil {
		log.Fatalf("Failed to serialize: %v", err)
	}

	fmt.Printf("Serialized shard size: %d bytes\n", len(data))

	// Example 4: Deserialize
	fmt.Println("\n=== Deserializing ===")

	s2, err := shard.Deserialize(data)
	if err != nil {
		log.Fatalf("Failed to deserialize: %v", err)
	}

	fmt.Printf("Deserialized shard:\n")
	fmt.Printf("  Version: %d\n", s2.Header.Version)
	fmt.Printf("  Files: %d\n", len(s2.Files))
	fmt.Printf("  CAS blocks: %d\n", len(s2.CASInfos))

	// Example 5: Serialize with footer (for stored shards)
	fmt.Println("\n=== Serializing with Footer ===")

	s.Footer = &shard.Footer{
		Version:                1,
		FileLookupOffset:       0,
		FileLookupNumEntries:   0,
		CASLookupOffset:        0,
		CASLookupNumEntries:    0,
		ChunkLookupOffset:      0,
		ChunkLookupNumEntries:  0,
		ShardCreationTimestamp: 1234567890,
		ShardKeyExpiry:         1234567900,
		StoredBytesOnDisk:      512,
		MaterializedBytes:      1024,
		StoredBytes:            512,
	}

	dataWithFooter, err := s.SerializeWithFooter()
	if err != nil {
		log.Fatalf("Failed to serialize with footer: %v", err)
	}

	fmt.Printf("Serialized shard with footer size: %d bytes\n", len(dataWithFooter))

	// Example 6: Deserialize shard with footer
	fmt.Println("\n=== Deserializing with Footer ===")

	s3, err := shard.Deserialize(dataWithFooter)
	if err != nil {
		log.Fatalf("Failed to deserialize shard with footer: %v", err)
	}

	if s3.Footer != nil {
		fmt.Printf("Footer:\n")
		fmt.Printf("  Version: %d\n", s3.Footer.Version)
		fmt.Printf("  Creation timestamp: %d\n", s3.Footer.ShardCreationTimestamp)
		fmt.Printf("  Key expiry: %d\n", s3.Footer.ShardKeyExpiry)
		fmt.Printf("  Stored bytes on disk: %d\n", s3.Footer.StoredBytesOnDisk)
		fmt.Printf("  Materialized bytes: %d\n", s3.Footer.MaterializedBytes)
	}

	// Example 7: Create shard with verification and metadata
	fmt.Println("\n=== Creating Shard with Verification and Metadata ===")

	s4 := shard.NewShard()

	verifHash := xet.Hash{}
	for i := range verifHash {
		verifHash[i] = byte(i * 2)
	}

	sha256Hash := [32]byte{}
	for i := range sha256Hash {
		sha256Hash[i] = byte(i * 3)
	}

	fb2 := shard.FileBlock{
		FileHash: fileHash,
		Flags:    shard.FileWithVerification | shard.FileWithMetadataExt,
		Entries: []shard.FileDataSequenceEntry{
			{
				CASHash:          casHash,
				CASFlags:         0,
				UnpackedSegBytes: 512,
				ChunkIndexStart:  0,
				ChunkIndexEnd:    3,
			},
		},
		Verification: []xet.Hash{verifHash},
		MetadataExt: &shard.FileMetadataExt{
			SHA256Hash: sha256Hash,
		},
	}

	s4.AddFile(fb2)

	dataWithExtras, err := s4.Serialize()
	if err != nil {
		log.Fatalf("Failed to serialize shard with extras: %v", err)
	}

	fmt.Printf("Serialized shard with verification and metadata size: %d bytes\n", len(dataWithExtras))

	s5, err := shard.Deserialize(dataWithExtras)
	if err != nil {
		log.Fatalf("Failed to deserialize shard with extras: %v", err)
	}

	if len(s5.Files) > 0 {
		fb := s5.Files[0]
		fmt.Printf("File flags: 0x%08x\n", fb.Flags)
		if fb.Flags&shard.FileWithVerification != 0 {
			fmt.Printf("  ✓ Has verification entries (%d)\n", len(fb.Verification))
		}
		if fb.Flags&shard.FileWithMetadataExt != 0 {
			fmt.Printf("  ✓ Has metadata extension\n")
		}
	}

	fmt.Println("\n=== Shard Serialization Example Completed Successfully! ===")
}
