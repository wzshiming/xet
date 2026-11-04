//go:build !amd64 && !arm64

package xorb

// interleave4 sets dst[4*i+k] = pk[i] for every i below len(p0).
func interleave4(dst, p0, p1, p2, p3 []byte) { interleave4Generic(dst, p0, p1, p2, p3) }
