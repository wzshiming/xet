package encoding_test

import (
	"bytes"
	"os/exec"
	"strings"
	"testing"
)

func TestConformance(t *testing.T) {
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
			data: makeBinaryData(1024),
		},
		{
			name: "10KB",
			data: makeBinaryData(10 * 1024),
		},
		{
			name: "100KB",
			data: makeBinaryData(100 * 1024),
		},
		{
			name: "1MB",
			data: makeBinaryData(1024 * 1024),
		},
		{
			name: "10MB",
			data: makeBinaryData(10 * 1024 * 1024),
		},
		{
			name: "100MB",
			data: makeBinaryData(100 * 1024 * 1024),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rustChunks, err := runRustChunk(tt.data)
			if err != nil {
				t.Fatalf("Rust chunk failed: %v", err)
			}

			goChunks, err := runGoChunk(tt.data)
			if err != nil {
				t.Fatalf("Go chunk failed: %v", err)
			}

			if rustChunks != goChunks {
				t.Errorf("Chunk output mismatch:\nRust:\n%s\n\nGo:\n%s", rustChunks, goChunks)
			}

			hashTypes := []string{"xorb", "file", "range", "chunk"}

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
				})
			}
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

func makeBinaryData(size int) []byte {
	result := make([]byte, size)
	for i := range result {
		result[i] = byte(i % 256)
	}
	return result
}
