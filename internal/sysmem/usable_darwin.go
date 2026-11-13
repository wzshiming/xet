//go:build darwin

package sysmem

import "golang.org/x/sys/unix"

func usable() (int64, bool) {
	n, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return 0, false
	}
	return int64(n), true
}
