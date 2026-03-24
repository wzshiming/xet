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
- [x] **Shard Format** - Binary metadata structure for file reconstructions
- [x] **HTTP API Client** - Complete client for XET CAS server
- [x] **Upload Flow** - Full upload orchestration with deduplication
- [x] **Download Flow** - File reconstruction and download
- [x] **CLI Tool** - Command-line tool with upload/download commands

## Installation

```bash
go get github.com/wzshiming/xet
```

## Usage

### Command-Line Tool

The XET CLI tool provides three main commands: `info`, `upload`, and `download`.

#### Display File Information

```bash
go build -o xet ./cmd/xet
./xet info <filename>
```

Example:

```bash
$ echo "Hello World!" > test.txt
$ ./xet info test.txt

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

#### Upload Files

Upload files to a XET CAS server:

```bash
./xet upload <file> --url <server-url> [options]

Options:
  --url <url>          CAS server URL (required)
  --token <token>      Authentication token
  --namespace <ns>     Storage namespace (default: default)
  --no-dedup           Disable global deduplication
```

Example:

```bash
$ ./xet upload myfile.txt --url https://xet.example.com --token abc123
Uploading: myfile.txt (1234 bytes)
File hash: 37f11908adc2a375075fef36afed69c14c40775a5d050bce5ed291f5de77a822
Uploading...
✓ Upload complete!
```

#### Download Files

Download files from a XET CAS server:

```bash
./xet download <file-hash> <output> --url <server-url> [options]

Options:
  --url <url>      CAS server URL (required)
  --token <token>  Authentication token
  --cache          Enable chunk caching
```

Example:

```bash
$ ./xet download 37f11908adc2a375075fef36afed69c14c40775a5d050bce5ed291f5de77a822 output.txt --url https://xet.example.com
Downloading file: 37f11908adc2a375075fef36afed69c14c40775a5d050bce5ed291f5de77a822
Downloading...
✓ Download complete! (1234 bytes)
Saved to: output.txt
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

#### Upload and Download Files

```go
import (
    "context"
    "github.com/wzshiming/xet/pkg/api"
    "github.com/wzshiming/xet/pkg/upload"
    "github.com/wzshiming/xet/pkg/download"
)

// Create API client
client := api.NewClient(api.ClientOptions{
    BaseURL:   "https://xet.example.com",
    Token:     "your-token",
    Namespace: "default",
})

// Upload a file
uploadSession := upload.NewSession(upload.SessionOptions{
    Client:            client,
    EnableGlobalDedup: true,
})

fileInfo := upload.ComputeFileInfo(fileData)
err := uploadSession.UploadFiles(context.Background(), []upload.FileUploadInfo{fileInfo})
if err != nil {
    log.Fatal(err)
}
fmt.Printf("File uploaded with hash: %s\n", fileInfo.FileHash.String())

// Download a file
downloadSession := download.NewSession(download.SessionOptions{
    Client:        client,
    EnableCaching: true,
})

data, err := downloadSession.DownloadFile(context.Background(), fileHash)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Downloaded %d bytes\n", len(data))
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
│   ├── shard/        # Shard format
│   │   ├── shard.go          # Shard serialization
│   │   └── shard_test.go     # Shard tests
│   ├── api/          # HTTP API client
│   │   ├── client.go         # API client implementation
│   │   └── types.go          # API types
│   ├── upload/       # Upload orchestration
│   │   └── session.go        # Upload session logic
│   └── download/     # Download orchestration
│       └── session.go        # Download session logic
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

This project has comprehensive test coverage across all major components.

### Running Tests

Run all tests:

```bash
go test ./...
```

Run tests with verbose output:

```bash
go test -v ./...
```

Run tests for a specific package:

```bash
go test ./pkg/api/...      # API client tests
go test ./pkg/upload/...   # Upload session tests
go test ./pkg/download/... # Download session tests
go test ./pkg/xorb/...     # Xorb format tests
go test ./pkg/shard/...    # Shard format tests
```

### Test Coverage

The test suite includes:

- **API Client Tests** (`pkg/api/client_test.go`)
  - HTTP client initialization and configuration
  - File reconstruction queries
  - Xorb and shard uploads
  - Chunk deduplication queries
  - Byte range downloads
  - Error handling

- **Upload Session Tests** (`pkg/upload/session_test.go`)
  - Session initialization with various options
  - Local chunk deduplication
  - File upload orchestration
  - Multi-file uploads
  - Empty file handling

- **Download Session Tests** (`pkg/download/session_test.go`)
  - Session initialization with caching options
  - File download and reconstruction
  - Byte range downloads
  - Chunk caching
  - Multi-chunk file handling

- **Core Protocol Tests**
  - Gearhash chunking algorithm (`pkg/gearhash/gearhash_test.go`)
  - BLAKE3 hashing with domain separation (`pkg/xet/hash_test.go`)
  - Merkle tree construction (`pkg/merkle/merkle_test.go`)
  - Compression (LZ4, ByteGrouping4) (`pkg/xorb/compression_test.go`)
  - Xorb serialization/deserialization (`pkg/xorb/xorb_test.go`)
  - Shard serialization/deserialization (`pkg/shard/shard_test.go`)

### Example: Testing File Upload and Download

Test the CLI tool with a sample file:

```bash
# Build the tool
go build -o xet ./cmd/xet

# Test file information display
echo "Hello, XET Protocol!" > test.txt
./xet info test.txt
```

This will display:
- Chunking information (number of chunks, sizes, hashes)
- Xorb creation details
- File hash computation
- Data integrity verification

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

This is a complete implementation of the XET protocol upload and download flows. All core protocol components are fully functional:

- ✅ Content-defined chunking (Gearhash)
- ✅ Cryptographic hashing (BLAKE3)
- ✅ Merkle tree construction
- ✅ Compression (LZ4, ByteGrouping4)
- ✅ Xorb serialization/deserialization
- ✅ Shard format implementation
- ✅ HTTP API client
- ✅ Upload flow with deduplication
- ✅ Download flow with reconstruction
- ✅ CLI tool with upload/download commands

### Protocol Compliance

This implementation follows the XET Protocol Specification (draft-denis-xet-03) and includes:

#### Upload Protocol (Section 11)
1. ✅ Chunking - Split files using Gearhash
2. ✅ Deduplication - Local session and global API deduplication
3. ✅ Xorb Formation - Group chunks into ~64MB xorbs
4. ✅ Xorb Upload - Upload serialized xorbs to CAS server
5. ✅ Shard Formation - Build file reconstruction metadata
6. ✅ Shard Upload - Register files in the system

#### Download Protocol (Section 12)
1. ✅ Query Reconstruction - Request file reconstruction info
2. ✅ Parse Response - Extract terms and fetch URLs
3. ✅ Download Xorb Data - Fetch xorb ranges via HTTP
4. ✅ Extract Chunks - Deserialize and decompress
5. ✅ Assemble File - Concatenate chunks in order

### API Endpoints Supported

- `GET /api/v1/reconstructions/{file_hash}` - File reconstruction queries
- `POST /api/v1/xorbs/{namespace}/{xorb_hash}` - Upload xorbs
- `POST /api/v1/shards` - Upload shards
- `GET /api/v1/chunks/{namespace}/{chunk_hash}` - Chunk deduplication queries
