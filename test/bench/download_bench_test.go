// Package bench_test times whole-file downloads over a server × client matrix
// on an in-process fake upstream hub and two xet mirrors of it.
//
// Servers:
//   - plain: the fake upstream; no xet.
//   - assembled: a mirror whose resolve answers lose Link and X-Xet-Hash to a
//     test-side filter, so the server reconstructs every download.
//   - xet: the unfiltered mirror; xet clients reconstruct the file themselves.
//
// Clients:
//   - go-http: net/http following the resolve redirects.
//   - go-xet: a timed HEAD of the resolve URL, then client/hf, or go-http's
//     GET when the answer has no xet-reconstruction-info link.
//   - hf-http: `hf download` with HF_HUB_DISABLE_XET=1.
//   - hf-xet: `hf download` with hf_xet and its own plain-HTTP fallback.
//
// Rows are BenchmarkDownload/size=<1MiB|64MiB|256MiB|1GiB>/class=<none|lz4>/server=<server>/client=<client>.
// Clients are cold, with a fresh hf process or Go transport, output dir and
// caches per download; servers are warm, with page cache and local xorb cache.
//
// The hf rows need the hf CLI on PATH, hf-xet also its hf_xet
// (pip install -U "huggingface_hub[cli,hf_xet]"), and skip without them; -v
// logs the versions in use. Run
//
//	go test ./test/bench -run '^$' -bench Download -benchtime 3x -count 5 -timeout 3h
//
// and compare runs with benchstat. The Download benchmark workflow runs it on
// demand (workflow_dispatch) and on pull requests that touch test/bench.
// A full run peaks at about 8.6 GiB in TMPDIR;
// -bench 'Download/size=(1MiB|64MiB|256MiB)' leaves out the 1 GiB rows, peaks
// at about 2.1 GiB and needs only -timeout 60m.
// Fixtures are built and ingested once per process, and -count repeats only
// the rows. Results are indicative only.
package bench_test

import (
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkDownload(b *testing.B) {
	h := newHarness(b)
	for _, s := range benchSizes {
		b.Run("size="+s.name, func(b *testing.B) {
			for _, c := range benchClasses {
				b.Run("class="+c.name, func(b *testing.B) {
					f := h.fixture(b, c, s)
					for _, r := range rows {
						b.Run(r.name(), func(b *testing.B) {
							h.skipUnavailable(b, r)
							benchRow(b, h, r, f)
						})
					}
				})
			}
		})
	}
}

// benchRow times whole-file downloads of f, each into a fresh output dir.
func benchRow(b *testing.B, h *harness, r row, f *upstreamFile) {
	before := h.counts(f)
	b.SetBytes(f.size)
	var dir string
	n, fellBack := 0, 0
	for b.Loop() {
		b.StopTimer()
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
		dir = h.outDir(b, r, f, n)
		n++
		b.StartTimer()
		if h.download(b, r, f, dir) {
			fellBack++
		}
		b.StopTimer()
		checkSize(b, filepath.Join(dir, f.name), f.size)
		// B.Loop fails when called with the timer stopped.
		b.StartTimer()
	}
	h.finishRow(b, r, f, dir, before, n, fellBack)
}
