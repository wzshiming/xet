// Package sample runs the benchmarks of a base and a head source tree on one machine, interleaved round by round, into sample files benchstat reads.
package sample

import (
	"bytes"
	"context"
	"debug/buildinfo"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
)

// Request says what to sample.
type Request struct {
	Base, Head       string // source tree roots
	Groups           []Group
	MeasureIdentical bool   // also measure packages whose base and head test binaries are byte-identical
	Out              string // directory the sample files go to
}

// Group is a set of benchmarks sampled together.
type Group struct {
	Name      string   // names the group's sample files: letters, digits, '_', '-' and '.', not first
	Packages  []string // package directories relative to the tree roots, such as "." or "xorb"
	Bench     string   // -test.bench regular expression
	Benchtime string   // -test.benchtime value, empty for Go's default
	Rounds    int      // samples of each row on each side, at least 1
	Rows      []Row    // if any, only these rows run, in place of Bench, and packages without one are left out
}

// Row names a benchmark result line.
type Row struct {
	Pkg  string // value of the pkg: line
	Name string // first field of the result line, such as BenchmarkFoo/n=1-8
}

// State is what became of a package of a group.
type State string

const (
	Measured    State = "measured"
	Identical   State = "identical"    // base and head test binaries are byte-identical, so not measured
	BaseMissing State = "base missing" // the base tree has no Go files in the package directory
	BaseFailed  State = "base failed"  // the base test binary failed to build or to run, so not measured
)

// Result is what Run did.
type Result struct {
	Groups []GroupResult // in request order
	Env    Environment
}

// GroupResult is a group's outcome.
type GroupResult struct {
	Name       string
	Base, Head string   // sample files, empty when no package of the group was measured
	Packages   []Status // in request order
}

// Status is a package's outcome in a group.
type Status struct {
	Package string
	State   State
	Reason  string // for BaseFailed, what failed with the go command's or the benchmark's output
}

// Unknown stands for an environment value that could not be read.
const Unknown = "unknown"

// Environment records where and how the samples were taken.
type Environment struct {
	CPU       string   // model name, or Unknown
	Cores     int      // logical CPUs
	OSArch    string   // such as linux/amd64
	GoVersion string   // of the go command that built the test binaries, or Unknown
	Loads     []string // before each round: the 1, 5 and 15 minute load averages, or Unknown
	Order     []Step   // builds and runs as performed
}

// Side is one of the two trees.
type Side string

const (
	Base Side = "base"
	Head Side = "head"
)

// Step is a build of a package's test binary or a run of a group's benchmarks in it.
type Step struct {
	Round   int    // from 1; 0 for a build
	Group   string // empty for a build
	Package string
	Side    Side
}

// binaries are a package's test binaries.
type binaries struct {
	path       map[Side]string
	importPath string // from the head binary
	base       State  // BaseMissing or BaseFailed when there is no base binary
	reason     string // why the base build failed
	identical  bool
}

// unit is a package of a group and its samples.
type unit struct {
	group   Group
	pkg     string
	bins    *binaries
	bench   string // -test.bench expression
	state   State
	reason  string // why the base failed
	samples map[Side][]byte
}

type sampler struct {
	req   Request
	tmp   string
	procs int // GOMAXPROCS of every run
	bins  map[string]*binaries
	env   Environment
}

// Run builds the test binaries of every package on both sides, then samples the packages to measure.
func Run(ctx context.Context, req Request) (Result, error) {
	if err := validate(req.Groups); err != nil {
		return Result{}, err
	}
	tmp, err := os.MkdirTemp("", "benchgate-sample-")
	if err != nil {
		return Result{}, err
	}
	defer os.RemoveAll(tmp)
	s := &sampler{req: req, tmp: tmp, procs: runtime.GOMAXPROCS(0), bins: make(map[string]*binaries)}
	s.env = Environment{CPU: cpuModel(), Cores: runtime.NumCPU(), OSArch: runtime.GOOS + "/" + runtime.GOARCH, GoVersion: Unknown}
	for _, g := range req.Groups {
		for _, p := range g.Packages {
			if err := s.build(ctx, g.Name, p); err != nil {
				return Result{}, err
			}
		}
	}

	groups, rounds := s.plan()
	if err := s.sample(ctx, groups, rounds); err != nil {
		return Result{}, err
	}
	return s.write(groups)
}

// write puts each group's samples into its two files when any of its units was measured.
func (s *sampler) write(groups [][]*unit) (Result, error) {
	res := Result{Env: s.env}
	for i, g := range s.req.Groups {
		gr := GroupResult{Name: g.Name}
		var base, head []byte
		measured := false
		for _, u := range groups[i] {
			gr.Packages = append(gr.Packages, Status{Package: u.pkg, State: u.state, Reason: u.reason})
			measured = measured || u.state == Measured
			base = append(base, u.samples[Base]...)
			head = append(head, u.samples[Head]...)
		}
		if measured {
			gr.Base = filepath.Join(s.req.Out, g.Name+".base.txt")
			gr.Head = filepath.Join(s.req.Out, g.Name+".head.txt")
			if err := writeFile(gr.Base, base); err != nil {
				return Result{}, err
			}
			if err := writeFile(gr.Head, head); err != nil {
				return Result{}, err
			}
		}
		res.Groups = append(res.Groups, gr)
	}
	return res, nil
}

func (s *sampler) tree(side Side) string {
	if side == Base {
		return s.req.Base
	}
	return s.req.Head
}

// plan lists each group's units and the number of rounds the measured ones need.
func (s *sampler) plan() ([][]*unit, int) {
	groups := make([][]*unit, len(s.req.Groups))
	rounds := 0
	for i, g := range s.req.Groups {
		for _, p := range g.Packages {
			u := &unit{group: g, pkg: p, bins: s.bins[p], bench: g.Bench, state: Measured, samples: make(map[Side][]byte)}
			if len(g.Rows) > 0 {
				if u.bench = selection(g.Rows, u.bins.importPath, s.procs); u.bench == "" {
					continue
				}
			}
			switch {
			case u.bins.base != "":
				u.state, u.reason = u.bins.base, u.bins.reason
			case u.bins.identical && !s.req.MeasureIdentical:
				u.state = Identical
			default:
				rounds = max(rounds, g.Rounds)
			}
			groups[i] = append(groups[i], u)
		}
	}
	return groups, rounds
}

// sample runs the measured units round by round, both sides of a unit back to back, the first side alternating by round.
func (s *sampler) sample(ctx context.Context, groups [][]*unit, rounds int) error {
	for r := 1; r <= rounds; r++ {
		s.env.Loads = append(s.env.Loads, loadAverages())
		sides := []Side{Base, Head}
		if r%2 == 0 {
			sides = []Side{Head, Base}
		}
		for _, units := range groups {
			for _, u := range units {
				if u.state != Measured || r > u.group.Rounds {
					continue
				}
				for _, side := range sides {
					out, err := s.bench(ctx, u, r, side)
					switch {
					case err != nil && side == Head:
						return fmt.Errorf("group %q, package %q: head run in round %d failed: %w", u.group.Name, u.pkg, r, err)
					case err != nil:
						// The head still runs: a failure on both sides is the head's.
						u.state, u.samples = BaseFailed, nil
						u.reason = strings.TrimSpace(fmt.Sprintf("run in round %d failed: %v", r, err))
					case u.state == Measured:
						u.samples[side] = append(u.samples[side], out...)
					}
				}
			}
		}
	}
	// A cancelled base run would otherwise pass for a base failure.
	return ctx.Err()
}

var groupName = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]*$`)

func validate(groups []Group) error {
	seen := make(map[string]bool)
	for _, g := range groups {
		switch {
		case !groupName.MatchString(g.Name):
			return fmt.Errorf("group %q: want a name of letters, digits, '_', '-' and '.', not first", g.Name)
		case seen[g.Name]:
			return fmt.Errorf("group %q: duplicate name", g.Name)
		case g.Rounds < 1:
			return fmt.Errorf("group %q: %d rounds, want at least 1", g.Name, g.Rounds)
		}
		seen[g.Name] = true
	}
	return nil
}

// build makes package p's test binaries once, head first; group g lists p first.
func (s *sampler) build(ctx context.Context, g, p string) error {
	if s.bins[p] != nil {
		return nil
	}
	n := strconv.Itoa(len(s.bins))
	b := &binaries{path: map[Side]string{Base: filepath.Join(s.tmp, "base-"+n+".test"), Head: filepath.Join(s.tmp, "head-"+n+".test")}}
	s.bins[p] = b
	if err := s.compile(ctx, p, Head); err != nil {
		return fmt.Errorf("group %q, package %q: head build failed: %w", g, p, err)
	}
	if info, err := buildinfo.ReadFile(b.path[Head]); err == nil {
		b.importPath = strings.TrimSuffix(info.Path, ".test")
		if s.env.GoVersion == Unknown {
			s.env.GoVersion = info.GoVersion
		}
	}
	if !hasGoFiles(filepath.Join(s.req.Base, p)) {
		b.base = BaseMissing
	} else if err := s.compile(ctx, p, Base); err != nil {
		b.base, b.reason = BaseFailed, strings.TrimSpace("build failed: "+err.Error())
	} else {
		b.identical = sameBytes(b.path[Base], b.path[Head])
	}
	return nil
}

// compile builds the test binary of package p on one side.
func (s *sampler) compile(ctx context.Context, p string, side Side) error {
	s.env.Order = append(s.env.Order, Step{Package: p, Side: side})
	// -trimpath keeps the tree's location out of the binary.
	cmd := exec.CommandContext(ctx, "go", "test", "-c", "-o", s.bins[p].path[side], "-trimpath", "-vet=off", ".")
	cmd.Dir = filepath.Join(s.tree(side), p)
	// Both sides build with the go command on PATH, whatever toolchain their go.mod names.
	cmd.Env = append(os.Environ(), "GOTOOLCHAIN=local")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w\n%s", cancelled(ctx, err), output)
	}
	// go test -c succeeds without writing a binary for a package without test files.
	if _, err := os.Stat(s.bins[p].path[side]); err != nil {
		return fmt.Errorf("no test binary: %s", bytes.TrimSpace(output))
	}
	return nil
}

// bench runs u's benchmarks once on one side, in the package directory.
func (s *sampler) bench(ctx context.Context, u *unit, round int, side Side) ([]byte, error) {
	s.env.Order = append(s.env.Order, Step{Round: round, Group: u.group.Name, Package: u.pkg, Side: side})
	args := []string{"-test.run=^$", "-test.bench=" + u.bench, "-test.benchmem", "-test.cpu=" + strconv.Itoa(s.procs)}
	if u.group.Benchtime != "" {
		args = append(args, "-test.benchtime="+u.group.Benchtime)
	}
	cmd := exec.CommandContext(ctx, u.bins.path[side], args...)
	cmd.Dir = filepath.Join(s.tree(side), u.pkg)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("%w\n%s%s", cancelled(ctx, err), out, &stderr)
	}
	return out, nil
}

// cancelled is ctx's error once ctx is done, as a killed command reports only how it exited.
func cancelled(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func writeFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// selection is a -test.bench expression for exactly the rows of package pkg, or empty without any;
// Go appends -procs to names unless procs is 1.
func selection(rows []Row, pkg string, procs int) string {
	suffix := ""
	if procs != 1 {
		suffix = "-" + strconv.Itoa(procs)
	}
	var alts []string
	for _, r := range rows {
		if r.Pkg != pkg {
			continue
		}
		// go test matches each slash-separated level of a name on its own.
		var levels []string
		for level := range strings.SplitSeq(strings.TrimSuffix(r.Name, suffix), "/") {
			levels = append(levels, "^"+regexp.QuoteMeta(level)+"$")
		}
		alts = append(alts, strings.Join(levels, "/"))
	}
	return strings.Join(alts, "|")
}

func hasGoFiles(dir string) bool {
	entries, _ := os.ReadDir(dir)
	return slices.ContainsFunc(entries, func(e os.DirEntry) bool {
		return !e.IsDir() && strings.HasSuffix(e.Name(), ".go")
	})
}

// cpuModel reads the CPU model name from where Go's benchmark output takes it on Linux and macOS.
func cpuModel() string {
	var name string
	switch runtime.GOOS {
	case "linux":
		info, _ := os.ReadFile("/proc/cpuinfo")
		for line := range strings.Lines(string(info)) {
			if key, value, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(key) == "model name" {
				name = value
				break
			}
		}
	case "darwin":
		out, _ := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output()
		name = string(out)
	}
	if name = strings.TrimSpace(name); name == "" {
		return Unknown
	}
	return name
}

// loadAverages reads the 1, 5 and 15 minute load averages.
func loadAverages() string {
	var avg string
	switch runtime.GOOS {
	case "linux":
		out, _ := os.ReadFile("/proc/loadavg") // 0.52 0.58 0.59 1/123 4567
		avg = string(out)
	case "darwin":
		out, _ := exec.Command("sysctl", "-n", "vm.loadavg").Output() // { 0.52 0.58 0.59 }
		avg = strings.Trim(strings.TrimSpace(string(out)), "{}")
	}
	f := strings.Fields(avg)
	if len(f) < 3 {
		return Unknown
	}
	return strings.Join(f[:3], " ")
}

func sameBytes(a, b string) bool {
	x, err := os.ReadFile(a)
	if err != nil {
		return false
	}
	y, err := os.ReadFile(b)
	return err == nil && bytes.Equal(x, y)
}
