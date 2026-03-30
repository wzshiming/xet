package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestHashCommand(t *testing.T) {
	// Build the xetc binary for testing
	tmpDir := t.TempDir()
	binaryPath := filepath.Join(tmpDir, "xetc")

	cmd := exec.Command("go", "build", "-o", binaryPath, ".")
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to build xetc: %v", err)
	}

	tests := []struct {
		name     string
		content  string
		wantHash string
	}{
		{
			name:     "empty file",
			content:  "",
			wantHash: "0000000000000000000000000000000000000000000000000000000000000000",
		},
		{
			name:     "hello world",
			content:  "Hello World!",
			wantHash: "a9dae0ad88b060bdd7e7c87abdcf95b132c95a0414b06d4f6beb68d287b87165",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a test file
			testFile := filepath.Join(tmpDir, "test_"+tt.name+".txt")
			if err := os.WriteFile(testFile, []byte(tt.content), 0644); err != nil {
				t.Fatalf("Failed to create test file: %v", err)
			}

			// Run the hash command
			cmd := exec.Command(binaryPath, "hash", testFile)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("hash command failed: %v, output: %s", err, output)
			}

			// Verify the hash
			gotHash := strings.TrimSpace(string(output))
			if gotHash != tt.wantHash {
				t.Errorf("hash mismatch:\ngot:  %s\nwant: %s", gotHash, tt.wantHash)
			}
		})
	}
}

func TestHashCommandNoFile(t *testing.T) {
	// Build the xetc binary for testing
	tmpDir := t.TempDir()
	binaryPath := filepath.Join(tmpDir, "xetc")

	cmd := exec.Command("go", "build", "-o", binaryPath, ".")
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to build xetc: %v", err)
	}

	// Run hash command without arguments
	cmd = exec.Command(binaryPath, "hash")
	output, err := cmd.CombinedOutput()

	// Should fail with usage message
	if err == nil {
		t.Error("Expected hash command to fail without file argument")
	}

	if !strings.Contains(string(output), "Usage") {
		t.Errorf("Expected usage message in output, got: %s", output)
	}
}

func TestHashCommandInvalidFile(t *testing.T) {
	// Build the xetc binary for testing
	tmpDir := t.TempDir()
	binaryPath := filepath.Join(tmpDir, "xetc")

	cmd := exec.Command("go", "build", "-o", binaryPath, ".")
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to build xetc: %v", err)
	}

	// Run hash command with non-existent file
	cmd = exec.Command(binaryPath, "hash", "/nonexistent/file.txt")
	output, err := cmd.CombinedOutput()

	// Should fail with error message
	if err == nil {
		t.Error("Expected hash command to fail with non-existent file")
	}

	if !strings.Contains(string(output), "Error reading file") {
		t.Errorf("Expected file error message in output, got: %s", output)
	}
}
