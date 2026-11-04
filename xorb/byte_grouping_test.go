package xorb

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/wzshiming/xet"
)

// reverseByteGrouping4Reference is reverseByteGrouping4 as of c4bc4eb, kept to pin its rewrite; dst's spare capacity must hold data.
func reverseByteGrouping4Reference(dst, data []byte) []byte {
	if len(data) == 0 {
		return dst
	}

	off := len(dst)
	dst = dst[:off+len(data)]
	result := dst[off:]

	numFullGroups := len(data) / 4
	remainder := len(data) % 4

	readPos := 0

	// Process each byte position within groups (0-3)
	for bytePos := range 4 {
		// Calculate how many groups have this byte position
		numGroups := numFullGroups
		if bytePos < remainder {
			numGroups++
		}

		// Scatter bytes at this position to all groups
		for g := range numGroups {
			if readPos < len(data) {
				result[g*4+bytePos] = data[readPos]
				readPos++
			}
		}
	}

	return dst
}

func TestReverseByteGrouping4MatchesReference(t *testing.T) {
	rng := rand.NewChaCha8([32]byte{4})
	prefix := []byte{0xde, 0xad, 0xbe, 0xef, 0x01}
	for n := range 1026 {
		data := make([]byte, n)
		rng.Read(data)
		want := reverseByteGrouping4Reference(append(make([]byte, 0, len(prefix)+n), prefix...), data)
		dst := append(make([]byte, 0, len(prefix)+n), prefix...)
		if got, err := reverseByteGrouping4(dst, data); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("reverseByteGrouping4(%d-byte prefix with cap %d, %d random bytes) error = %v, or its result differs from the reference", len(prefix), cap(dst), n, err)
		}
	}
}

// interleave4Impls are the implementations built here; interleave4 runs the assembly on amd64 and arm64.
var interleave4Impls = []struct {
	name string
	f    func(dst, p0, p1, p2, p3 []byte)
}{
	{"interleave4", interleave4},
	{"interleave4Generic", interleave4Generic},
}

// TestInterleave4ImplementationsAgree feeds every implementation group counts around the assembly's 16-group step
// up to a 128 KiB chunk, unaligned planes and dst, and planes and dst longer than needed.
func TestInterleave4ImplementationsAgree(t *testing.T) {
	counts := []int{255, 256, 257, xet.TargetChunkSize/4 - 1, xet.TargetChunkSize / 4, xet.MaxChunkSize / 4}
	for n := range 66 {
		counts = append(counts, n)
	}
	pad := bytes.Repeat([]byte{0xA5}, 16)
	rng := rand.NewChaCha8([32]byte{16})
	for _, n := range counts {
		for _, off := range []int{0, 1, 3, 15} {
			// Plane k starts off+k bytes into its buffer and runs k bytes past p0's length.
			var p [4][]byte
			for k := range p {
				buf := make([]byte, off+k+n+k)
				rng.Read(buf)
				p[k] = buf[off+k:]
			}
			want := make([]byte, 4*n)
			for i := range n {
				for k := range p {
					want[4*i+k] = p[k][i]
				}
			}
			for _, impl := range interleave4Impls {
				buf := bytes.Repeat(pad[:1], off+4*n+len(pad))
				impl.f(buf[off:], p[0], p[1], p[2], p[3])
				if !bytes.Equal(buf[off:off+4*n], want) {
					t.Fatalf("%s(%d groups, offset %d) wrote the wrong bytes", impl.name, n, off)
				}
				if !bytes.Equal(buf[:off], pad[:off]) || !bytes.Equal(buf[off+4*n:], pad) {
					t.Fatalf("%s(%d groups, offset %d) wrote outside dst[:%d]", impl.name, n, off, 4*n)
				}
			}
		}
	}
}

// TestInterleave4PanicsOnShortInputs: the assembly must panic like the Go loop instead of reading or writing out of range.
func TestInterleave4PanicsOnShortInputs(t *testing.T) {
	for _, impl := range interleave4Impls {
		for _, n := range []int{1, 16, 33} {
			for short, name := range []string{"p1", "p2", "p3", "dst"} {
				t.Run(fmt.Sprintf("%s/n=%d/%s", impl.name, n, name), func(t *testing.T) {
					p := [4][]byte{make([]byte, n), make([]byte, n), make([]byte, n), make([]byte, n)}
					dst := make([]byte, 4*n)
					if short < 3 {
						p[short+1] = p[short+1][:n-1]
					} else {
						dst = dst[:4*n-1]
					}
					defer func() {
						if recover() == nil {
							t.Errorf("%s with %s one byte short did not panic", impl.name, name)
						}
					}()
					impl.f(dst, p[0], p[1], p[2], p[3])
				})
			}
		}
	}
}

func BenchmarkReverseByteGrouping4(b *testing.B) {
	data := make([]byte, xet.TargetChunkSize)
	rand.NewChaCha8([32]byte{4}).Read(data)
	dst := make([]byte, 0, len(data))
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		if _, err := reverseByteGrouping4(dst, data); err != nil {
			b.Fatal(err)
		}
	}
}
