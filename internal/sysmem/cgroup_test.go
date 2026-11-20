package sysmem

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// writeCgroupFile creates the file name, a slash-separated path under root.
func writeCgroupFile(t *testing.T, root, name, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestCgroupLimitWithoutCgroupFilesIsNone(t *testing.T) {
	root := t.TempDir()
	if limit, ok := cgroupLimit(root, []byte("0::/user.slice/app.scope\n")); ok {
		t.Fatalf("cgroupLimit = %d, true; want no limit", limit)
	}
}

func TestCgroupLimitV2MemoryMaxValue(t *testing.T) {
	root := t.TempDir()
	writeCgroupFile(t, root, "user.slice/app.scope/memory.max", "2147483648\n")
	limit, ok := cgroupLimit(root, []byte("0::/user.slice/app.scope\n"))
	if !ok || limit != 2147483648 {
		t.Fatalf("cgroupLimit = %d, %v; want 2147483648, true", limit, ok)
	}
}

func TestCgroupLimitV2MemoryMaxMaxIsNone(t *testing.T) {
	root := t.TempDir()
	writeCgroupFile(t, root, "user.slice/app.scope/memory.max", "max\n")
	if limit, ok := cgroupLimit(root, []byte("0::/user.slice/app.scope\n")); ok {
		t.Fatalf("cgroupLimit = %d, true; want no limit", limit)
	}
}

// cgroupV1Self is /proc/self/cgroup of a process in cgroup v1 hierarchies.
const cgroupV1Self = "12:pids:/docker/abc\n" +
	"11:memory:/docker/abc\n" +
	"10:cpu,cpuacct:/docker/abc\n" +
	"1:name=systemd:/docker/abc\n"

func TestCgroupLimitV1LimitInBytesValue(t *testing.T) {
	root := t.TempDir()
	writeCgroupFile(t, root, "memory/docker/abc/memory.limit_in_bytes", "536870912\n")
	limit, ok := cgroupLimit(root, []byte(cgroupV1Self))
	if !ok || limit != 536870912 {
		t.Fatalf("cgroupLimit = %d, %v; want 536870912, true", limit, ok)
	}
}

func TestCgroupLimitV1UnlimitedSentinelIsNone(t *testing.T) {
	root := t.TempDir()
	writeCgroupFile(t, root, "memory/docker/abc/memory.limit_in_bytes", "9223372036854771712\n")
	if limit, ok := cgroupLimit(root, []byte(cgroupV1Self)); ok {
		t.Fatalf("cgroupLimit = %d, true; want no limit", limit)
	}
}

// A cgroup v1 hierarchy can bind several controllers, memory among them.
func TestCgroupLimitV1MemoryAmongSeveralControllers(t *testing.T) {
	root := t.TempDir()
	writeCgroupFile(t, root, "memory/some/path/memory.limit_in_bytes", "268435456\n")
	limit, ok := cgroupLimit(root, []byte("12:memory,hugetlb:/some/path\n"))
	if !ok || limit != 268435456 {
		t.Fatalf("cgroupLimit = %d, %v; want 268435456, true", limit, ok)
	}
}

func TestCgroupLimitMalformedIsNone(t *testing.T) {
	for _, content := range []string{"", "lots\n", "512M\n", "1.5e9\n", "-1\n", "0\n"} {
		t.Run(strconv.Quote(content), func(t *testing.T) {
			root := t.TempDir()
			writeCgroupFile(t, root, "user.slice/app.scope/memory.max", content)
			if limit, ok := cgroupLimit(root, []byte("0::/user.slice/app.scope\n")); ok {
				t.Fatalf("cgroupLimit = %d, true; want no limit", limit)
			}
		})
	}
}

// A container without its own cgroup namespace finds the cgroup path of
// /proc/self/cgroup missing and its own cgroup mounted at the root.
func TestCgroupLimitFallsBackToRootFiles(t *testing.T) {
	for _, tc := range []struct{ name, self, file string }{
		{"v2", "0::/system.slice/docker-abc.scope\n", "memory.max"},
		{"v1", cgroupV1Self, "memory/memory.limit_in_bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeCgroupFile(t, root, tc.file, "1073741824\n")
			limit, ok := cgroupLimit(root, []byte(tc.self))
			if !ok || limit != 1073741824 {
				t.Fatalf("cgroupLimit = %d, %v; want 1073741824, true", limit, ok)
			}
		})
	}
}

// The root's limit also binds a process in a child cgroup of a container's.
func TestCgroupLimitTakesSmallestOfOwnAndRoot(t *testing.T) {
	for _, tc := range []struct {
		name, own, root string
		want            int64
	}{
		{"own smaller", "1073741824\n", "2147483648\n", 1073741824},
		{"root smaller", "4294967296\n", "2147483648\n", 2147483648},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeCgroupFile(t, root, "app.slice/memory.max", tc.own)
			writeCgroupFile(t, root, "memory.max", tc.root)
			limit, ok := cgroupLimit(root, []byte("0::/app.slice\n"))
			if !ok || limit != tc.want {
				t.Fatalf("cgroupLimit = %d, %v; want %d, true", limit, ok, tc.want)
			}
		})
	}
}

func TestCappedTakesOnlyAFiniteSmallerLimit(t *testing.T) {
	const ram int64 = 16 << 30
	for _, tc := range []struct {
		name    string
		limit   int64
		limited bool
		want    int64
	}{
		{"no limit", 2 << 30, false, ram},
		{"smaller limit", 2 << 30, true, 2 << 30},
		{"larger limit", 32 << 30, true, ram},
		{"equal limit", ram, true, ram},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := capped(ram, tc.limit, tc.limited); got != tc.want {
				t.Fatalf("capped(%d, %d, %v) = %d; want %d", ram, tc.limit, tc.limited, got, tc.want)
			}
		})
	}
}
