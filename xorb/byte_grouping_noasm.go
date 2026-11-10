//go:build !amd64 && !arm64

package xorb

// interleave4 sets dst[4*i+k] = pk[i] for every i below len(p0).
func interleave4(dst, p0, p1, p2, p3 []byte) { interleave4Generic(dst, p0, p1, p2, p3) }

// deinterleave4 sets pk[i] = src[4*i+k] for every i below len(p0).
func deinterleave4(p0, p1, p2, p3, src []byte) { deinterleave4Generic(p0, p1, p2, p3, src) }
