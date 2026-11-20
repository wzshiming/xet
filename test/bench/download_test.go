package bench_test

import (
	"path/filepath"
	"testing"
)

// TestDownload runs every row once on the 1 MiB none fixture.
func TestDownload(t *testing.T) {
	h := newHarness(t)
	f := h.fixture(t, benchClasses[0], benchSizes[0])
	for _, r := range rows {
		t.Run(r.name(), func(t *testing.T) {
			h.skipUnavailable(t, r)
			before := h.counts(f)
			dir := h.outDir(t, r, f, 0)
			fellBack := 0
			if h.download(t, r, f, dir) {
				fellBack = 1
			}
			checkSize(t, filepath.Join(dir, f.name), f.size)
			h.finishRow(t, r, f, dir, before, 1, fellBack)
		})
	}
}
