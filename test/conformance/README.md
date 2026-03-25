# XET Conformance Testing

This directory contains conformance tests that validate consistency between the Golang implementation in this repository and the Rust reference implementation from [HuggingFace xet-core](https://github.com/huggingface/xet-core).

## Overview

The conformance tests ensure that:
- Chunking produces identical results (chunk boundaries and hashes)
- Hash computations (chunk, xorb, file, range) are consistent
- Xorb serialization/deserialization maintains compatibility

## Structure

```
test/conformance/
├── Cargo.toml              # Rust workspace configuration
├── chunk/                  # Rust chunk tool (from xet-core examples)
├── hash/                   # Rust hash tool (from xet-core examples)
├── xorb-check/             # Rust xorb-check tool (from xet-core examples)
├── go/                     # Go equivalents of Rust tools
│   ├── chunk.go
│   ├── hash.go
│   └── xorb-check.go
├── conformance_test.go     # Test suite comparing Rust vs Go outputs
└── README.md              # This file
```

## Running Tests

### Prerequisites

1. Install Rust toolchain: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
2. Build Rust tools: `cargo build --release`
3. Run Go tests: `go test -v`

### Quick Start

```bash
cd test/conformance

# Build Rust tools
cargo build --release

# Run conformance tests
go test -v
```

## How It Works

The conformance tests work by:

1. **Building Both Implementations**: The test suite builds both Rust and Go tools
2. **Running Identical Inputs**: Test data is fed to both implementations
3. **Comparing Outputs**: Outputs are compared byte-for-byte
4. **Reporting Differences**: Any mismatches are reported as test failures

## Test Coverage

- **Chunking** (TestChunkConformance): Validates that content-defined chunking produces identical chunk boundaries and hashes between Go and Rust implementations
- **Chunk Hash** (TestChunkHashConformance): Validates BLAKE3 DATA_KEY hash computation produces identical results
- **Xorb Hash** (TestHashConformance/xorb): Validates Merkle tree construction and xorb hash computation match between implementations
- **File Hash** (TestHashConformance/file): Validates ZERO_KEY application to xorb hash produces identical results
- **Range Hash** (TestHashConformance/range): Validates VERIFICATION_KEY hash computation matches
- **Xorb Deserialization** (TestXorbCheckConformance): Validates that Go xorb-check tool can correctly deserialize Go-created xorbs and extract chunk information

## Tools

### chunk

Splits input into content-defined chunks using Gearhash and outputs chunk hash + size pairs.

```bash
# Rust
./target/release/chunk --input file.bin --output chunks.txt

# Go
./go/chunk --input file.bin --output chunks.txt
```

Output format: `<hash> <size>` (one per line)

### hash

Computes various hash types from chunk list input.

```bash
# Rust
./target/release/hash --hash-type xorb --input chunks.txt --output hash.txt

# Go
./go/hash --hash-type xorb --input chunks.txt --output hash.txt
```

Hash types: `chunk`, `xorb`, `file`, `range`

### xorb-check

Deserializes xorb files and verifies hash computation.

```bash
# Rust
./target/release/xorb-check --input xorb.bin --output-chunks-stdout

# Go
./go/xorb-check --input xorb.bin --output-chunks-stdout
```

## Adding New Test Cases

To add new test cases:

1. Add test data to the `TestChunkConformance` or `TestHashConformance` functions
2. Run `go test -v` to validate
3. Consider adding edge cases like empty files, single-byte files, or files with specific patterns

## Troubleshooting

If tests fail:

1. **Rebuild Rust tools**: `cargo build --release`
2. **Check tool paths**: Ensure `rustChunkBin`, `goChunkBin`, etc. point to correct locations
3. **Verify dependencies**: Ensure xet-core dependency is accessible
4. **Check tool output**: Run tools manually to inspect differences

## References

- [XET Protocol Specification](https://datatracker.ietf.org/doc/draft-denis-xet/03/)
- [HuggingFace xet-core](https://github.com/huggingface/xet-core)
- [xet-core examples](https://github.com/huggingface/xet-core/tree/main/xet_data/examples)
