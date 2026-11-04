package rustref

import (
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// testRoot returns the conformance module root; go test runs in the package directory.
func testRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// testUnsetenv unsets key until the test ends.
func testUnsetenv(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, "")
	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
}

func assertSelected(t *testing.T, tag string) {
	t.Helper()
	if got := Version(); got != tag {
		t.Errorf("Version() = %q, want %q", got, tag)
	}
	manifest, binary, err := resolveReference()
	if err != nil {
		t.Fatalf("resolveReference() error: %v", err)
	}
	dir := filepath.Join(testRoot(t), "versions", tag)
	if want := filepath.Join(dir, "Cargo.toml"); manifest != want {
		t.Errorf("manifest = %s, want %s", manifest, want)
	}
	name := "xet-core-reference"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	if want := filepath.Join(dir, "target", "release", name); binary != want {
		t.Errorf("binary = %s, want %s", binary, want)
	}
}

func assertUnpinned(t *testing.T, err error, tag string) {
	t.Helper()
	if err == nil {
		t.Fatalf("XET_CORE_VERSION=%s: no error", tag)
	}
	for _, want := range append([]string{tag}, slices.Sorted(maps.Keys(pinnedVersions))...) {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not name %s: %v", want, err)
		}
	}
}

func TestNoEnvSelectsDefaultVersion(t *testing.T) {
	testUnsetenv(t, "XET_CORE_REFERENCE_BIN")
	testUnsetenv(t, "XET_CORE_VERSION")
	assertSelected(t, "v1.8.0")
}

func TestXetCoreVersionSelectsPinnedRelease(t *testing.T) {
	testUnsetenv(t, "XET_CORE_REFERENCE_BIN")
	t.Setenv("XET_CORE_VERSION", "v1.7.1")
	assertSelected(t, "v1.7.1")
}

func TestReferenceBinOverridesVersion(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "xet-core-reference")
	if err := os.WriteFile(bin, nil, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XET_CORE_REFERENCE_BIN", bin)
	for _, tag := range []string{"", "v1.7.1", "v1.7.0"} {
		t.Setenv("XET_CORE_VERSION", tag)
		if _, binary, err := resolveReference(); err != nil || binary != bin {
			t.Errorf("XET_CORE_VERSION=%q: resolveReference() = %s, %v; want %s, nil", tag, binary, err, bin)
		}
	}
}

// v1.7.0, the previous pin, stands for a stale XET_CORE_VERSION.
func TestUnpinnedVersionErrorListsPins(t *testing.T) {
	testUnsetenv(t, "XET_CORE_REFERENCE_BIN")
	t.Setenv("XET_CORE_VERSION", "v1.7.0")
	_, _, err := resolveReference()
	assertUnpinned(t, err, "v1.7.0")
}

func TestEntryPointsReportUnpinnedVersion(t *testing.T) {
	testUnsetenv(t, "XET_CORE_REFERENCE_BIN")
	t.Setenv("XET_CORE_VERSION", "v1.7.0")
	_, err := HashChunk(nil)
	assertUnpinned(t, err, "v1.7.0")
	server, err := StartServer()
	if err == nil {
		server.Close()
	}
	assertUnpinned(t, err, "v1.7.0")
}

func TestVersionWithoutVPrefixIsRejected(t *testing.T) {
	testUnsetenv(t, "XET_CORE_REFERENCE_BIN")
	t.Setenv("XET_CORE_VERSION", "1.8.0")
	if manifest, _, err := resolveReference(); err == nil {
		t.Errorf("XET_CORE_VERSION=1.8.0 resolved to %s, want an error", manifest)
	}
}

func TestManifestsPinTableRevs(t *testing.T) {
	rev := regexp.MustCompile(`\brev\s*=\s*"([^"]*)"`)
	for tag, sha := range pinnedVersions {
		data, err := os.ReadFile(filepath.Join(testRoot(t), "versions", tag, "Cargo.toml"))
		if err != nil {
			t.Error(err)
			continue
		}
		pinned := 0
		for line := range strings.SplitSeq(string(data), "\n") {
			if !strings.Contains(line, "huggingface/xet-core") {
				continue
			}
			if m := rev.FindStringSubmatch(line); m == nil || m[1] != sha {
				t.Errorf("versions/%s/Cargo.toml: xet-core dependency not at rev %s: %s", tag, sha, strings.TrimSpace(line))
				continue
			}
			pinned++
		}
		if pinned != 5 {
			t.Errorf("versions/%s/Cargo.toml: %d xet-core dependencies at rev %s, want 5", tag, pinned, sha)
		}
	}
}

func TestLocksPinTableRevs(t *testing.T) {
	xetCoreSource := regexp.MustCompile(`[^"\s(]*huggingface/xet-core[^"\s)]*`)
	for tag, sha := range pinnedVersions {
		data, err := os.ReadFile(filepath.Join(testRoot(t), "versions", tag, "Cargo.lock"))
		if err != nil {
			t.Error(err)
			continue
		}
		lock := string(data)
		source := "git+https://github.com/huggingface/xet-core?rev=" + sha + "#" + sha
		for _, got := range xetCoreSource.FindAllString(lock, -1) {
			if got != source {
				t.Errorf("versions/%s/Cargo.lock: xet-core source %s, want %s", tag, got, source)
			}
		}
		for _, crate := range []string{"hf-xet", "xet-client", "xet-core-structures", "xet-data", "xet-runtime"} {
			entry := fmt.Sprintf("name = %q\nversion = %q\nsource = %q\n", crate, strings.TrimPrefix(tag, "v"), source)
			if strings.Count(lock, fmt.Sprintf("name = %q\n", crate)) != 1 || !strings.Contains(lock, entry) {
				t.Errorf("versions/%s/Cargo.lock: want exactly one %s package:\n%s", tag, crate, entry)
			}
		}
	}
}

func TestVersionDirsMatchTable(t *testing.T) {
	entries, err := os.ReadDir(filepath.Join(testRoot(t), "versions"))
	if err != nil {
		t.Fatal(err)
	}
	var dirs []string
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, entry.Name())
		}
	}
	if tags := slices.Sorted(maps.Keys(pinnedVersions)); !slices.Equal(dirs, tags) {
		t.Errorf("versions/ holds %v, pinnedVersions has %v", dirs, tags)
	}
}

func TestDefaultVersionIsGreatestPin(t *testing.T) {
	semver := regexp.MustCompile(`^v(\d+)\.(\d+)\.(\d+)$`)
	var greatest string
	var greatestParts []int
	for tag := range pinnedVersions {
		m := semver.FindStringSubmatch(tag)
		if m == nil {
			t.Errorf("pinned tag %q is not vMAJOR.MINOR.PATCH", tag)
			continue
		}
		parts := make([]int, 3)
		for i := range parts {
			parts[i], _ = strconv.Atoi(m[i+1])
		}
		if greatest == "" || slices.Compare(parts, greatestParts) > 0 {
			greatest, greatestParts = tag, parts
		}
	}
	if DefaultVersion != greatest {
		t.Errorf("DefaultVersion = %s, want the greatest pinned tag %s", DefaultVersion, greatest)
	}
}

func TestWorkflowTestsEveryPin(t *testing.T) {
	workflow, err := os.ReadFile(filepath.Join(testRoot(t), "..", "..", ".github", "workflows", "test.yml"))
	if err != nil {
		t.Fatal(err)
	}
	for tag := range pinnedVersions {
		if !strings.Contains(string(workflow), tag) {
			t.Errorf(".github/workflows/test.yml does not mention %s", tag)
		}
	}
}

func TestRootManifestRemovedAndUnreferenced(t *testing.T) {
	root := testRoot(t)
	for _, name := range []string{"Cargo.toml", "Cargo.lock"} {
		if _, err := os.Stat(filepath.Join(root, name)); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("test/conformance/%s still exists", name)
		}
	}
	rootManifest := regexp.MustCompile(`test/conformance/Cargo\.(toml|lock)|--manifest-path[= ]+["']?(\./)?Cargo\.toml`)
	// Outside Go and Rust code, a cargo command naming neither --manifest-path nor versions/ builds the root manifest.
	cargoCommand := regexp.MustCompile(`\bcargo\s+[a-z-]+\s+-`)
	for _, dir := range []string{root, filepath.Join(root, "..", "..", ".github")} {
		err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
			switch {
			case err != nil:
				return err
			case entry.IsDir() && entry.Name() == "target":
				return filepath.SkipDir
			case !entry.Type().IsRegular():
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			code := filepath.Ext(path) == ".go" || filepath.Ext(path) == ".rs"
			for line := range strings.SplitSeq(strings.ReplaceAll(string(data), "\\\n", " "), "\n") {
				bare := !code && cargoCommand.MatchString(line) && !strings.Contains(line, "--manifest-path") && !strings.Contains(line, "versions/")
				if bare || rootManifest.MatchString(line) {
					t.Errorf("%s refers to the root manifest: %s", path, strings.TrimSpace(line))
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
