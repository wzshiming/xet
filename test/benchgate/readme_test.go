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
