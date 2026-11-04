//go:build amd64 || arm64

package xorb

// interleave4 sets dst[4*i+k] = pk[i] for every i below len(p0).
func interleave4(dst, p0, p1, p2, p3 []byte) {
	n := len(p0)
	if n == 0 {
		return
	}
	// The assembly does no bounds checks: fail here instead of reading or writing out of range.
	_ = p1[n-1]
	_ = p2[n-1]
	_ = p3[n-1]
	_ = dst[4*n-1]
	interleave4Asm(&dst[0], &p0[0], &p1[0], &p2[0], &p3[0], n)
}

// interleave4Asm interleaves n bytes of each of p0, p1, p2 and p3 into 4*n bytes at dst.
//
//go:noescape
func interleave4Asm(dst, p0, p1, p2, p3 *byte, n int)
