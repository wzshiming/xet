package sysmem

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// cgroupUnlimited and larger values mean no limit, which cgroup v1 shows as
// 2^63 rounded down to a page.
const cgroupUnlimited = 1 << 62

// cgroupLimit returns the smallest finite memory limit of the process's cgroup,
// named by procSelfCgroup (/proc/self/cgroup), and of the cgroup at root, the
// cgroup filesystem's mount point, where a container without a cgroup
// namespace sees its own. ok is false when neither has one.
func cgroupLimit(root string, procSelfCgroup []byte) (limit int64, ok bool) {
	var names []string
	for line := range strings.SplitSeq(string(procSelfCgroup), "\n") {
		// hierarchy-ID:controller-list:cgroup-path
		_, rest, _ := strings.Cut(line, ":")
		controllers, dir, found := strings.Cut(rest, ":")
		switch {
		case !found:
		case controllers == "":
			names = append(names, filepath.Join(root, dir, "memory.max"))
		case slices.Contains(strings.Split(controllers, ","), "memory"):
			names = append(names, filepath.Join(root, "memory", dir, "memory.limit_in_bytes"))
		}
	}
	names = append(names, filepath.Join(root, "memory.max"), filepath.Join(root, "memory", "memory.limit_in_bytes"))
	for _, name := range names {
		b, err := os.ReadFile(name)
		if err != nil {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		if err != nil || n <= 0 || n >= cgroupUnlimited {
			continue
		}
		if !ok || n < limit {
			limit, ok = n, true
		}
	}
	return limit, ok
}

// capped returns ram, or limit when limited and smaller.
func capped(ram, limit int64, limited bool) int64 {
	if limited && limit < ram {
		return limit
	}
	return ram
}
