package main

import (
	"bytes"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

func TestUsesOnlyTheProvidersAndTheStandardLibrary(t *testing.T) {
	t.Parallel()
	cmd := exec.Command("go", "list", "-f", `{{join .Imports " "}}`, ".")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, &stderr)
	}
	imports := strings.Fields(string(out))
	providers := []string{"github.com/wzshiming/xet/test/benchgate/sample", "github.com/wzshiming/xet/test/benchgate/verdict"}
	for _, p := range providers {
		if !slices.Contains(imports, p) {
			t.Errorf("benchgate does not import %s", p)
		}
	}
	for _, p := range imports {
		if elem, _, _ := strings.Cut(p, "/"); strings.Contains(elem, ".") && !slices.Contains(providers, p) {
			t.Errorf("benchgate imports %s, want only the standard library, sample and verdict", p)
		}
	}
}
