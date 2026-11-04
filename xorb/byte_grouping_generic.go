package xorb

import "encoding/binary"

// interleave4Generic sets dst[4*i+k] = pk[i] for every i below len(p0).
func interleave4Generic(dst, p0, p1, p2, p3 []byte) {
	for i := range p0 {
		binary.LittleEndian.PutUint32(dst[4*i:], uint32(p0[i])|uint32(p1[i])<<8|uint32(p2[i])<<16|uint32(p3[i])<<24)
	}
}
