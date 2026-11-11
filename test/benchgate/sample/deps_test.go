package sample_test

import (
	"os/exec"
	"strings"
	"testing"
)

func TestImportsOnlyStandardLibrary(t *testing.T) {
	const self = "github.com/wzshiming/xet/test/benchgate/sample"
	out, err := exec.Command("go", "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}} {{len .GoFiles}}{{end}}", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	for line := range strings.Lines(string(out)) {
		path, files, ok := strings.Cut(strings.TrimSpace(line), " ")
		switch {
		case !ok: // a standard library package
		case path != self:
			t.Errorf("sample depends on %s, want only the standard library", path)
		case files == "0":
			t.Errorf("%s has no Go files besides its tests", path)
		}
	}
}
