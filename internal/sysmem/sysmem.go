// Package sysmem reports how much memory the process can use.
package sysmem

// Usable returns the total memory this process can use: the physical RAM or,
// on Linux, the cgroup memory limit when that is smaller. ok is false when
// the size cannot be read.
func Usable() (bytes int64, ok bool) {
	return usable()
}
