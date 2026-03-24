# Shard Package

This package implements the XET shard serialization format as specified in [draft-denis-xet-03.txt](https://github.com/wzshiming/xet/blob/master/draft-denis-xet-03.txt).

## Overview

A shard is a binary metadata structure that describes file reconstructions and xorb contents. Shards serve two purposes:

1. **Upload Registration**: Describing newly uploaded files and xorbs to the CAS server
2. **Deduplication Response**: Providing information about existing chunks for deduplication

## Structure

A shard consists of:

- **Header** (48 bytes): Contains magic tag, version, and footer size
- **File Info Section**: Zero or more file blocks describing file reconstructions
- **CAS Info Section**: Zero or more CAS blocks describing xorbs and their chunks
- **Footer** (200 bytes, optional): Metadata including lookup tables and timestamps

## Usage

### Creating a Shard

```go
import "github.com/wzshiming/xet/pkg/shard"

// Create a new shard with default header
s := shard.NewShard()
```

### Adding File Blocks

```go
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
```

### Adding CAS Blocks

```go
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
```

### Serialization

#### Without Footer (for Upload API)

```go
data, err := s.Serialize()
if err != nil {
    log.Fatalf("Failed to serialize: %v", err)
}
```

#### With Footer (for Stored Shards)

```go
s.Footer = &shard.Footer{
    Version:                1,
    ShardCreationTimestamp: time.Now().Unix(),
    ShardKeyExpiry:         time.Now().Add(24 * time.Hour).Unix(),
    // ... other footer fields
}

data, err := s.SerializeWithFooter()
if err != nil {
    log.Fatalf("Failed to serialize with footer: %v", err)
}
```

### Deserialization

```go
s, err := shard.Deserialize(data)
if err != nil {
    log.Fatalf("Failed to deserialize: %v", err)
}

// Access file blocks
for _, fb := range s.Files {
    fmt.Printf("File hash: %s\n", fb.FileHash.String())
}

// Access CAS blocks
for _, cb := range s.CASInfos {
    fmt.Printf("CAS hash: %s\n", cb.CASHash.String())
}
```

## Flags

### File Flags

- `FileWithVerification` (bit 31): FileVerificationEntry present for each entry
- `FileWithMetadataExt` (bit 30): FileMetadataExt present at end

### Chunk Flags

- `ChunkGlobalDedupEligible` (bit 31): Chunk is eligible for global deduplication

## Format Details

### Header

The header is 48 bytes at offset 0:

- Bytes 0-31: Magic tag (application ID + magic sequence)
- Bytes 32-39: Version (64-bit unsigned, must be 2)
- Bytes 40-47: Footer size (64-bit unsigned, 0 if omitted)

### Magic Tag

- Bytes 0-13: Application identifier (ASCII, null-padded)
- Byte 14: Null byte (0x00)
- Bytes 15-31: Magic sequence (fixed)

The default application identifier is `HFRepoMetaData` for Hugging Face deployments.

### Footer

The footer is 200 bytes and is required for stored shards but must be omitted when uploading shards via the upload API.

## Example

See [examples/shard_example.go](../../examples/shard_example.go) for a complete working example.

## Testing

Run the tests with:

```bash
go test ./pkg/shard/...
```

## References

- [XET Protocol Specification](https://github.com/wzshiming/xet/blob/master/draft-denis-xet-03.txt)
- Section 9: Shard Format
