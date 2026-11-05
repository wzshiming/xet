package xorb

// interleave4Generic sets dst[4*i+k] = pk[i] for every i below len(p0).
func interleave4Generic(dst, p0, p1, p2, p3 []byte) {
	n := len(p0)
	if n == 0 {
		return
	}
	// Check every length up front: a short plane or dst panics before anything is written, and the loop only checks dst[3].
	_ = p1[n-1]
	_ = p2[n-1]
	_ = p3[n-1]
	_ = dst[4*n-1]
	for i := range p0 {
		_ = dst[3] // one bounds check for the four stores
		dst[0] = p0[i]
		dst[1] = p1[i]
		dst[2] = p2[i]
		dst[3] = p3[i]
		dst = dst[4:]
	}
}

// deinterleave4Generic sets pk[i] = src[4*i+k] for every i below len(p0).
func deinterleave4Generic(p0, p1, p2, p3, src []byte) {
	n := len(p0)
	if n == 0 {
		return
	}
	// Check every length up front: a short plane or src panics before anything is written, and the loop only checks src[3].
	_ = p1[n-1]
	_ = p2[n-1]
	_ = p3[n-1]
	_ = src[4*n-1]
	for i := range p0 {
		_ = src[3] // one bounds check for the four loads
		p0[i] = src[0]
		p1[i] = src[1]
		p2[i] = src[2]
		p3[i] = src[3]
		src = src[4:]
	}
}
