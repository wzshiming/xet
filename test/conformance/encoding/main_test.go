package encoding_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

var (
	rustChunkBin = "./target/release/chunk"
	rustHashBin  = "./target/release/hash"
	goChunkBin   = "./go/chunk"
	goHashBin    = "./go/hash"
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

	goDir := filepath.Join(".", "go")

	if err := exec.Command("go", "build", "-o", filepath.Join(goDir, "chunk"), filepath.Join(goDir, "chunk.go")).Run(); err != nil {
		panic(fmt.Sprintf("Failed to build Go chunk tool: %v", err))
	}
	if err := exec.Command("go", "build", "-o", filepath.Join(goDir, "hash"), filepath.Join(goDir, "hash.go")).Run(); err != nil {
		panic(fmt.Sprintf("Failed to build Go hash tool: %v", err))
	}

	code := m.Run()

	os.Remove(filepath.Join(goDir, "chunk"))
	os.Remove(filepath.Join(goDir, "hash"))

	os.Exit(code)
}
