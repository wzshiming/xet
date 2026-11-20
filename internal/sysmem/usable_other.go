//go:build !darwin && !linux && !windows

package sysmem

func usable() (int64, bool) {
	return 0, false
}
