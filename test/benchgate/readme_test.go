package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadmeConfigurationIsValid(t *testing.T) {
	t.Parallel()
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	_, example, found := strings.Cut(string(readme), "```json\n")
	example, _, closed := strings.Cut(example, "```")
	if !found || !closed {
		t.Fatal("README.md shows no JSON configuration")
	}
	path := filepath.Join(t.TempDir(), "gate.json")
	writeFile(t, path, example)
	if _, err := loadConfig(path); err != nil {
		t.Errorf("README.md's configuration: %v", err)
	}
}

func TestReadmeRunsABuiltBinary(t *testing.T) {
	t.Parallel()
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	_, example, found := strings.Cut(string(readme), "```sh\n")
	example, _, closed := strings.Cut(example, "```")
	if !found || !closed {
		t.Fatal("README.md shows no shell example")
	}
	if !strings.Contains(example, " build -o ") || strings.Contains(example, " run .") {
		t.Errorf("README.md's example %q does not build the gate and run the binary", example)
	}
	if !strings.Contains(string(readme), "`go run`") {
		t.Error("README.md does not say why the gate does not run through `go run`")
	}
}
