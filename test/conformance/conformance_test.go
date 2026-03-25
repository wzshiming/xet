package conformance_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wzshiming/xet/pkg/gearhash"
	"github.com/wzshiming/xet/pkg/xorb"
)

var (
	rustChunkBin     = "./target/release/chunk"
	rustHashBin      = "./target/release/hash"
	rustXorbCheckBin = "./target/release/xorb-check"
	goChunkBin       = "./go/chunk"
	goHashBin        = "./go/hash"
	goXorbCheckBin   = "./go/xorb-check"
)

func TestMain(m *testing.M) {
	// Check if required Rust binaries are present; if not, skip all conformance tests
	if _, err := os.Stat(rustChunkBin); os.IsNotExist(err) {
		fmt.Fprintln(os.Stderr, "skipping conformance tests: Rust chunk binary not found at", rustChunkBin)
		fmt.Fprintln(os.Stderr, "Run 'cargo build --release' to build the Rust tools")
		os.Exit(0)
	}
	if _, err := os.Stat(rustHashBin); os.IsNotExist(err) {
		fmt.Fprintln(os.Stderr, "skipping conformance tests: Rust hash binary not found at", rustHashBin)
		fmt.Fprintln(os.Stderr, "Run 'cargo build --release' to build the Rust tools")
		os.Exit(0)
	}
	if _, err := os.Stat(rustXorbCheckBin); os.IsNotExist(err) {
		fmt.Fprintln(os.Stderr, "skipping conformance tests: Rust xorb-check binary not found at", rustXorbCheckBin)
		fmt.Fprintln(os.Stderr, "Run 'cargo build --release' to build the Rust tools")
		os.Exit(0)
	}

	goDir := filepath.Join(".", "go")

	if err := exec.Command("go", "build", "-o", filepath.Join(goDir, "chunk"), filepath.Join(goDir, "chunk.go")).Run(); err != nil {
		panic(fmt.Sprintf("Failed to build Go chunk tool: %v", err))
	}
	if err := exec.Command("go", "build", "-o", filepath.Join(goDir, "hash"), filepath.Join(goDir, "hash.go")).Run(); err != nil {
		panic(fmt.Sprintf("Failed to build Go hash tool: %v", err))
	}
	if err := exec.Command("go", "build", "-o", filepath.Join(goDir, "xorb-check"), filepath.Join(goDir, "xorb-check.go")).Run(); err != nil {
		panic(fmt.Sprintf("Failed to build Go xorb-check tool: %v", err))
	}

	code := m.Run()

	os.Remove(filepath.Join(goDir, "chunk"))
	os.Remove(filepath.Join(goDir, "hash"))
	os.Remove(filepath.Join(goDir, "xorb-check"))

	os.Exit(code)
}

func TestChunkConformance(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "Empty file",
			data: []byte{},
		},
		{
			name: "Hello World",
			data: []byte("Hello World!"),
		},
		{
			name: "1KB",
			data: makeRepeatedData(1024),
		},
		{
			name: "10KB",
			data: makeRepeatedData(10 * 1024),
		},
		{
			name: "100KB",
			data: makeRepeatedData(100 * 1024),
		},
		{
			name: "1MB",
			data: makeRepeatedData(1024 * 1024),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rustOutput, err := runRustChunk(tt.data)
			if err != nil {
				t.Fatalf("Rust chunk failed: %v", err)
			}

			goOutput, err := runGoChunk(tt.data)
			if err != nil {
				t.Fatalf("Go chunk failed: %v", err)
			}

			if rustOutput != goOutput {
				t.Errorf("Chunk output mismatch:\nRust:\n%s\n\nGo:\n%s", rustOutput, goOutput)
			}

			t.Logf("Chunks match! (%d bytes input, %d lines output)", len(tt.data), strings.Count(rustOutput, "\n"))
		})
	}
}

func TestHashConformance(t *testing.T) {
	testData := makeRepeatedData(100 * 1024)

	rustChunks, err := runRustChunk(testData)
	if err != nil {
		t.Fatalf("Failed to get Rust chunks: %v", err)
	}

	goChunks, err := runGoChunk(testData)
	if err != nil {
		t.Fatalf("Failed to get Go chunks: %v", err)
	}

	if rustChunks != goChunks {
		t.Fatalf("Chunks don't match between Rust and Go")
	}

	hashTypes := []string{"xorb", "file", "range"}

	for _, hashType := range hashTypes {
		t.Run(hashType, func(t *testing.T) {
			rustHash, err := runRustHash(hashType, rustChunks)
			if err != nil {
				t.Fatalf("Rust hash failed: %v", err)
			}

			goHash, err := runGoHash(hashType, goChunks)
			if err != nil {
				t.Fatalf("Go hash failed: %v", err)
			}

			if rustHash != goHash {
				t.Errorf("Hash mismatch for type %s:\nRust: %s\nGo:   %s", hashType, rustHash, goHash)
			}

			t.Logf("Hash type %s matches: %s", hashType, rustHash)
		})
	}
}

func TestChunkHashConformance(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"Empty", []byte{}},
		{"Hello World", []byte("Hello World!")},
		{"1KB", makeRepeatedData(1024)},
		{"64KB", makeRepeatedData(64 * 1024)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rustHash, err := runRustHash("chunk", string(tt.data))
			if err != nil {
				t.Fatalf("Rust hash failed: %v", err)
			}

			goHash, err := runGoHash("chunk", string(tt.data))
			if err != nil {
				t.Fatalf("Go hash failed: %v", err)
			}

			if rustHash != goHash {
				t.Errorf("Chunk hash mismatch:\nRust: %s\nGo:   %s", rustHash, goHash)
			}

			t.Logf("Chunk hash matches: %s", rustHash)
		})
	}
}

func TestXorbCheckConformance(t *testing.T) {
	// This test validates that both Go and Rust xorb-check tools can correctly
	// extract chunk information from xorbs created by either implementation,
	// ensuring full cross-implementation serialization compatibility

	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "Small file - Hello World",
			data: []byte("Hello World!"),
		},
		{
			name: "100KB file",
			data: makeRepeatedData(100 * 1024),
		},
		{
			name: "1MB file",
			data: makeRepeatedData(1024 * 1024),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// First, get the expected chunk information using the chunk tool
			expectedChunks, err := runGoChunk(tt.data)
			if err != nil {
				t.Fatalf("Failed to chunk data: %v", err)
			}

			// Create xorb from test data using Go implementation
			goXorbBytes, err := createXorb(tt.data)
			if err != nil {
				t.Fatalf("Failed to create xorb: %v", err)
			}

			// Test 1: Go xorb-check reading Go-created xorb
			t.Run("Go xorb-check reads Go xorb", func(t *testing.T) {
				goChunks, err := runGoXorbCheck(goXorbBytes)
				if err != nil {
					t.Fatalf("Go xorb-check failed on Go xorb: %v", err)
				}

				if goChunks != expectedChunks {
					t.Errorf("Go xorb-check output doesn't match original chunks:\nExpected:\n%s\n\nGot:\n%s", expectedChunks, goChunks)
				}

				t.Logf("Go xorb-check successfully extracted %d chunks from %d byte Go xorb (original data: %d bytes)",
					strings.Count(goChunks, "\n"), len(goXorbBytes), len(tt.data))
			})

			// Test 2: Rust xorb-check reading Go-created xorb (cross-implementation)
			t.Run("Rust xorb-check reads Go xorb", func(t *testing.T) {
				rustChunks, err := runRustXorbCheck(goXorbBytes)
				if err != nil {
					t.Fatalf("Rust xorb-check failed on Go xorb: %v", err)
				}

				if rustChunks != expectedChunks {
					t.Errorf("Rust xorb-check output doesn't match original chunks:\nExpected:\n%s\n\nGot:\n%s", expectedChunks, rustChunks)
				}

				t.Logf("Rust xorb-check successfully extracted %d chunks from %d byte Go xorb (original data: %d bytes)",
					strings.Count(rustChunks, "\n"), len(goXorbBytes), len(tt.data))
			})
		})
	}
}

func createXorb(data []byte) ([]byte, error) {
	// Use xetc info-style logic to create an xorb
	var chunks []gearhash.Chunk
	err := gearhash.ChunkData(bytes.NewReader(data), func(offset int64, chunk []byte) error {
		buf := make([]byte, len(chunk))
		copy(buf, chunk)
		chunks = append(chunks, gearhash.Chunk{Data: buf, Offset: offset})
		return nil
	})
	if err != nil {
		return nil, err
	}
	xorbObj := xorb.NewXorb()
	for _, chunk := range chunks {
		if err := xorbObj.AddChunk(chunk.Data); err != nil {
			return nil, err
		}
	}
	return xorbObj.SerializeChunksOnly()
}

func runRustXorbCheck(xorbBytes []byte) (string, error) {
	cmd := exec.Command(rustXorbCheckBin, "--output-chunks-stdout")
	cmd.Stdin = bytes.NewReader(xorbBytes)
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%w: %s", err, stderr.String())
	}
	return out.String(), nil
}

func runGoXorbCheck(xorbBytes []byte) (string, error) {
	cmd := exec.Command(goXorbCheckBin, "--output-chunks-stdout")
	cmd.Stdin = bytes.NewReader(xorbBytes)
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%w: %s", err, stderr.String())
	}
	return out.String(), nil
}

func runRustChunk(data []byte) (string, error) {
	cmd := exec.Command(rustChunkBin)
	cmd.Stdin = bytes.NewReader(data)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "", err
	}
	return out.String(), nil
}

func runGoChunk(data []byte) (string, error) {
	cmd := exec.Command(goChunkBin)
	cmd.Stdin = bytes.NewReader(data)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "", err
	}
	return out.String(), nil
}

func runRustHash(hashType string, input string) (string, error) {
	cmd := exec.Command(rustHashBin, "--hash-type", hashType)
	cmd.Stdin = strings.NewReader(input)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "", err
	}
	return strings.TrimSpace(out.String()), nil
}

func runGoHash(hashType string, input string) (string, error) {
	cmd := exec.Command(goHashBin, "--hash-type", hashType)
	cmd.Stdin = strings.NewReader(input)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "", err
	}
	return strings.TrimSpace(out.String()), nil
}

func makeRepeatedData(size int) []byte {
	pattern := []byte("XET Protocol Test Pattern - Conformance Testing - ")
	result := make([]byte, size)
	for i := 0; i < size; i++ {
		result[i] = pattern[i%len(pattern)]
	}
	return result
}
