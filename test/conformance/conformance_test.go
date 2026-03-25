package conformance_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
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
