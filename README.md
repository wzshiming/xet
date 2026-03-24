# XET Protocol - Golang Implementation

A Golang implementation of the XET (Content-Addressable Storage) protocol for efficient storage and transfer of large files with chunk-level deduplication.

## Overview

XET is a content-addressable storage protocol that uses:
- **Content-defined chunking** (Gearhash algorithm) to split files into variable-sized chunks
- **Chunk-level deduplication** using cryptographic hashing (BLAKE3)
- **Xorbs** (XET Orbs) as containers that aggregate multiple compressed chunks
- **Merkle trees** with variable fan-out for efficient verification

This implementation follows the [XET Protocol Specification](draft-denis-xet-03.txt) (draft-denis-xet-03).

## Features

### ✅ Implemented

- [x] **Content-Defined Chunking** - Gearhash algorithm with configurable parameters
- [x] **BLAKE3 Hashing** - Keyed hashing with domain separation
- [x] **Merkle Tree Construction** - Variable fan-out aggregated hash trees
- [x] **Compression Support**
  - LZ4 Frame format
  - ByteGrouping4LZ4 (optimal for structured data)
  - Automatic best compression selection
- [x] **Xorb Format** - Binary serialization and deserialization
- [x] **Basic CLI Tool** - Command-line tool for chunking and processing files

### 🚧 In Progress

- [ ] Shard serialization format
- [ ] HTTP API client
- [ ] Upload/download flows
- [ ] Global deduplication support

## Installation

```bash
go get github.com/wzshiming/xet
```

## Usage

### Command-Line Tool

Build and run the CLI tool:

```bash
go build -o xet ./cmd/xet
./xet <filename>
```

Example:

```bash
$ echo "Hello World!" > test.txt
$ ./xet test.txt

File: test.txt (13 bytes)

=== Chunking ===
Number of chunks: 1
Chunk 0: offset=0, size=13, hash=f6aeac10af81b13e9c620b095efdbb98a2a96002c2cf2c177b07d52e52b4ea04

=== Creating Xorb ===
Xorb hash: f6aeac10af81b13e9c620b095efdbb98a2a96002c2cf2c177b07d52e52b4ea04
Xorb size: 133 bytes

=== File Hash ===
File hash: 12a1cc4ff021a67d069dd40833fd697f4048524a11168df125906e967e558ed8

✓ Data integrity verified - all chunks match!
```

### Library Usage

#### Content-Defined Chunking

```go
import "github.com/wzshiming/xet/pkg/gearhash"

data := []byte("your data here...")
chunks := gearhash.ChunkData(data)

for i, chunk := range chunks {
    fmt.Printf("Chunk %d: offset=%d, size=%d\n",
        i, chunk.Offset, len(chunk.Data))
}
```

#### Computing Hashes

```go
import "github.com/wzshiming/xet/pkg/xet"

// Compute chunk hash
chunkHash := xet.ComputeChunkHash(chunkData)
fmt.Println("Chunk hash:", chunkHash.String())

// Compute file hash from merkle root
fileHash := xet.ComputeFileHash(merkleRoot[:])
fmt.Println("File hash:", fileHash.String())
```

#### Creating and Serializing Xorbs

```go
import "github.com/wzshiming/xet/pkg/xorb"

// Create a new xorb
x := xorb.NewXorb()

// Add chunks
for _, chunk := range chunks {
    if err := x.AddChunk(chunk.Data); err != nil {
        log.Fatal(err)
    }
}

// Serialize to binary format
serialized, err := x.Serialize()
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Xorb hash: %s\n", x.Hash.String())
fmt.Printf("Serialized size: %d bytes\n", len(serialized))

// Deserialize
deserialized, err := xorb.Deserialize(serialized)
if err != nil {
    log.Fatal(err)
}
```

## Architecture

### Package Structure

```
xet/
├── pkg/
│   ├── xet/          # Core types and constants
│   │   ├── constants.go   # Algorithm parameters and keys
│   │   ├── hash.go        # BLAKE3 hashing utilities
│   │   └── hash_test.go   # Hash tests
│   ├── gearhash/     # Content-defined chunking
│   │   ├── gearhash.go       # Gearhash implementation
│   │   └── gearhash_test.go  # Chunking tests
│   ├── merkle/       # Merkle tree construction
│   │   └── merkle.go         # Variable fan-out trees
│   ├── xorb/         # Xorb format and compression
│   │   ├── xorb.go           # Xorb serialization
│   │   └── compression.go    # LZ4 and ByteGrouping4
│   ├── shard/        # Shard format (TODO)
│   └── api/          # HTTP API client (TODO)
├── cmd/
│   └── xet/          # CLI tool
│       └── main.go
├── draft-denis-xet-03.txt  # Protocol specification
└── README.md
```

### Algorithm Parameters

- **Target Chunk Size**: 64 KiB (65,536 bytes)
- **Min Chunk Size**: 8 KiB (8,192 bytes)
- **Max Chunk Size**: 128 KiB (131,072 bytes)
- **Merkle Branching**: Mean=4, Min=2, Max=9
- **Hash Size**: 32 bytes (BLAKE3)

## Testing

Run all tests:

```bash
go test ./pkg/...
```

Run tests with verbose output:

```bash
go test -v ./pkg/...
```

Test with a specific file:

```bash
go build -o xet ./cmd/xet
./xet path/to/your/file
```

## Protocol Specification

This implementation follows the XET protocol specification version draft-denis-xet-03. Key features include:

### Content-Defined Chunking (Gearhash)
- Rolling hash algorithm for boundary detection
- Deterministic chunk boundaries that remain stable across modifications
- Configurable size constraints (min/max/target)

### Cryptographic Hashing (BLAKE3)
- Keyed hashing with domain separation:
  - `DATA_KEY` - for chunk hashes
  - `INTERNAL_NODE_KEY` - for internal Merkle nodes
  - `ZERO_KEY` - for final file hashes
  - `VERIFICATION_KEY` - for term verification

### Merkle Trees
- Variable fan-out (not binary trees)
- Cut points determined by hash values modulo branching factor
- Efficient verification and reconstruction

### Xorb Format
Binary container format with:
- Compressed chunk data (with 8-byte headers)
- CasObjectInfo footer (metadata, hashes, boundaries)
- Support for multiple compression types

## License

See [LICENSE](LICENSE) file for details.

## References

- [XET Protocol Specification](draft-denis-xet-03.txt)
- [BLAKE3 Cryptographic Hash Function](https://github.com/BLAKE3-team/BLAKE3)
- [LZ4 Compression Algorithm](https://github.com/lz4/lz4)

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## Status

This is an early implementation focusing on the core protocol components. The following features are fully functional:

- ✅ Content-defined chunking
- ✅ Cryptographic hashing
- ✅ Merkle tree construction
- ✅ Compression (LZ4, ByteGrouping4)
- ✅ Xorb serialization/deserialization
- ✅ Basic CLI tool

Future work includes:
- Shard format implementation
- HTTP API client
- Complete upload/download flows
- Performance optimizations
- More comprehensive test coverage
