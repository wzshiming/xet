//go:build linux

package sysmem

import (
	"os"

	"golang.org/x/sys/unix"
)

func usable() (int64, bool) {
	var info unix.Sysinfo_t
	if err := unix.Sysinfo(&info); err != nil {
		return 0, false
	}
	self, _ := os.ReadFile("/proc/self/cgroup") // unreadable: only the root's files apply
	limit, limited := cgroupLimit("/sys/fs/cgroup", self)
	return capped(int64(info.Totalram)*int64(info.Unit), limit, limited), true
}
