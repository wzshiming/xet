// Package bench_test times whole-file downloads over a server × client matrix
// on an in-process fake upstream hub and two xet mirrors of it.
//
// Servers:
//   - plain: the fake upstream; no xet.
//   - assembled: a mirror whose resolve answers lose Link and X-Xet-Hash to a
//     test-side filter, so the server reconstructs every download.
//   - xet: an unfiltered mirror; xet clients reconstruct the file themselves.
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
// Besides sec/op and MB/s, each row reports per GiB (2^30 bytes) of file
// downloaded, over the windows around its downloads: cpu-s/GiB, usr-s/GiB
// and sys-s/GiB, this process's CPU seconds (getrusage RUSAGE_SELF; the
// kernel samples the usr/sys split at clock ticks); minflt/GiB and
// majflt/GiB, its page faults; hf-cpu-s/GiB, the CPU of the hf processes
// waited for inside the windows (RUSAGE_CHILDREN; 0 on go-* rows). cores is
// cpu-s per wall second of the windows, and served/file the response body
// bytes the row's server sent per file byte: at least 1 on class=none, below
// 1 where xet clients fetch lz4 xorbs. The servers run in this process, so
// the cpu-s of go-* rows includes the Go client, while the sec/op and MB/s
// of hf rows include Python start-up, which cpu-s leaves out.
// BenchmarkDownload first prints a benchfmt "Unit <unit> better=lower" line
// per unit. Each row logs the bytes and requests its server answered per
// request class, the most xorb GETs it had in flight and this process's
// context switches. cmd/xetd's request log (CombinedLoggingHandler on
// stdout) is not part of the bench.
//
// Environment:
//   - XET_BENCH_PROFILE_DIR=<absolute dir> writes each row's CPU profile to
//     <dir>/<b.Name() with "/" as "_">.pprof, or .2.pprof, .3.pprof … when
//     the name repeats in one process (-count, -cpu). It spans the row's
//     b.Loop, so it also holds the untimed RemoveAll, outDir and checkSize,
//     the hf process spawns and GC between downloads, but no fixture
//     building and not the final sha256.
//   - XET_BENCH_XORB_CACHE=<n> gives every stack local.WithXorbCacheSize(n):
//     cached xorb file handles before #252, cached xorb mappings after it.
//   - HF_XET_NUM_CONCURRENT_RANGE_GETS and HF_XET_HIGH_PERFORMANCE pass
//     through to the hf CLI; every other HF_* variable is dropped.
//
// The hf rows need the hf CLI on PATH, hf-xet also its hf_xet
// (pip install -U "huggingface_hub[cli,hf_xet]"), and skip without them; -v
// logs the versions in use. Run
//
//	go test ./test/bench -run '^$' -bench Download -benchtime 3x -count 5 -timeout 3h
//
// and compare runs with benchstat. The Download benchmark workflow runs it on
// demand (workflow_dispatch) and on pull requests that touch test/bench; its
// ab job runs chosen rows ABBA on this tree and on 43e6575, master before
// #252, with these bench files.
// A full run peaks at about 8.5 GiB in TMPDIR;
// -bench 'Download/size=(1MiB|64MiB|256MiB)' leaves out the 1 GiB rows, peaks
// at about 2.0 GiB and needs only -timeout 60m.
// Fixtures are built and ingested once per process, and -count repeats only
// the rows. Results are indicative only.
package bench_test

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// rowUnits are the units of the metrics reportRow adds to each row, in its order.
var rowUnits = [...]string{"cpu-s/GiB", "usr-s/GiB", "sys-s/GiB", "minflt/GiB", "majflt/GiB", "served/file", "cores", "hf-cpu-s/GiB"}

func BenchmarkDownload(b *testing.B) {
	for _, u := range rowUnits {
		fmt.Printf("Unit %s better=lower\n", u)
	}
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
	var u usage
	n, fellBack := 0, 0
	stopProfile := startProfile(b)
	defer stopProfile() // a failed download leaves through here
	for b.Loop() {
		b.StopTimer()
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
		dir = h.outDir(b, r, f, n)
		n++
		b.StartTimer()
		start := readUsage(b)
		if h.download(b, r, f, dir) {
			fellBack++
		}
		u.add(start, readUsage(b))
		b.StopTimer()
		checkSize(b, filepath.Join(dir, f.name), f.size)
		// B.Loop fails when called with the timer stopped.
		b.StartTimer()
	}
	stopProfile()
	reportRow(b, r, f, n, u, before, h.counts(f))
	h.finishRow(b, r, f, dir, before, n, fellBack)
}

// reportRow adds rowUnits' metrics of n downloads of f and logs what the row's server sent.
func reportRow(b *testing.B, r row, f *upstreamFile, n int, u usage, before, after map[string]serverCounts) {
	from, to := before[r.server], after[r.server]
	var served int64
	var split []string
	for c := range numClasses {
		sent, reqs := to.bytes[c]-from.bytes[c], to.reqs[c]-from.reqs[c]
		served += sent
		if reqs > 0 {
			split = append(split, fmt.Sprintf("%s %d B/%d req", classNames[c], sent, reqs))
		}
	}
	file := float64(n) * float64(f.size)
	gib := file / (1 << 30)
	cpu := (u.usr + u.sys).Seconds()
	for i, v := range [len(rowUnits)]float64{
		cpu / gib,
		u.usr.Seconds() / gib,
		u.sys.Seconds() / gib,
		float64(u.minflt) / gib,
		float64(u.majflt) / gib,
		float64(served) / file,
		cpu / u.wall.Seconds(),
		u.child.Seconds() / gib,
	} {
		b.ReportMetric(v, rowUnits[i])
	}
	b.Logf("server=%s sent %d B over %d downloads: %s; at most %d xorb GETs in flight; %d voluntary, %d involuntary context switches",
		r.server, served, n, strings.Join(split, ", "), to.peak, u.nvcsw, u.nivcsw)
}

// usage sums download windows: their wall time, this process's CPU, page
// faults and context switches, and the CPU of the children waited for inside.
type usage struct {
	wall, usr, sys, child         time.Duration
	minflt, majflt, nvcsw, nivcsw int64
}

// reading is one getrusage of this process and of its waited-for children.
type reading struct {
	at          time.Time
	self, child syscall.Rusage
}

func readUsage(tb testing.TB) reading {
	tb.Helper()
	var r reading
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r.self); err != nil {
		tb.Fatal(err)
	}
	if err := syscall.Getrusage(syscall.RUSAGE_CHILDREN, &r.child); err != nil {
		tb.Fatal(err)
	}
	r.at = time.Now()
	return r
}

// add adds the window between two readings; Nano and the int64 conversions absorb the per-platform field types.
func (u *usage) add(from, to reading) {
	u.wall += to.at.Sub(from.at)
	u.usr += time.Duration(to.self.Utime.Nano() - from.self.Utime.Nano())
	u.sys += time.Duration(to.self.Stime.Nano() - from.self.Stime.Nano())
	u.child += time.Duration(to.child.Utime.Nano() + to.child.Stime.Nano() - from.child.Utime.Nano() - from.child.Stime.Nano())
	u.minflt += int64(to.self.Minflt) - int64(from.self.Minflt)
	u.majflt += int64(to.self.Majflt) - int64(from.self.Majflt)
	u.nvcsw += int64(to.self.Nvcsw) - int64(from.self.Nvcsw)
	u.nivcsw += int64(to.self.Nivcsw) - int64(from.self.Nivcsw)
}

// profiles counts the CPU profiles startProfile wrote per benchmark name.
var profiles = map[string]int{}

// startProfile writes b's CPU profile into XET_BENCH_PROFILE_DIR, if set, until stop, which may be called again.
func startProfile(b *testing.B) (stop func()) {
	dir := os.Getenv("XET_BENCH_PROFILE_DIR")
	if dir == "" {
		return func() {}
	}
	if !filepath.IsAbs(dir) {
		b.Fatalf("XET_BENCH_PROFILE_DIR=%q is not absolute", dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatal(err)
	}
	name := strings.ReplaceAll(b.Name(), "/", "_")
	profiles[name]++
	if k := profiles[name]; k > 1 {
		name += "." + strconv.Itoa(k)
	}
	out, err := os.Create(filepath.Join(dir, name+".pprof"))
	if err != nil {
		b.Fatal(err)
	}
	if err := pprof.StartCPUProfile(out); err != nil {
		out.Close()
		b.Fatal(err)
	}
	return sync.OnceFunc(func() {
		pprof.StopCPUProfile()
		if err := out.Close(); err != nil {
			b.Error(err)
		}
	})
}
