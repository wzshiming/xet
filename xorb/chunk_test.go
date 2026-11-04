package xorb

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/wzshiming/xet"
)

// Compile-time check of the exported chunk API signatures.
var (
	_ func([]byte) (ChunkHeader, error)                 = ParseChunkHeader
	_ func([]byte, ChunkHeader, []byte) ([]byte, error) = DecodeChunk
)

var chunkClassesForTest = []struct {
	name string
	ct   CompressionType
}{
	{"none", CompressionNone},
	{"lz4", CompressionLZ4},
	{"bg4", CompressionByteGrouping4},
}

// dstSparesForTest are dst capacities past UncompressedSize; LZ4 decodes straight into dst only with a spare byte.
var dstSparesForTest = []struct {
	name  string
	spare int
}{
	{"cap=size", 0},
	{"cap=size+1", 1},
	{"cap=size+64", 64},
}

// payloadForTest returns n deterministic bytes alternating 512-byte runs of random data and little-endian uint32 counters.
func payloadForTest(n int, seed byte) []byte {
	b := make([]byte, n)
	rand.NewChaCha8([32]byte{seed}).Read(b)
	for i := range b {
		if i/512%2 == 1 {
			b[i] = byte((i / 4) >> (8 * (i % 4)))
		}
	}
	return b
}

// chunkFrameForTest returns an 8-byte chunk header followed by payload encoded as ct, and the header it declares.
func chunkFrameForTest(t testing.TB, ct CompressionType, payload []byte) ([]byte, ChunkHeader) {
	t.Helper()
	body := payload
	var err error
	switch ct {
	case CompressionNone:
	case CompressionLZ4:
		body, err = compressLZ4(nil, payload)
	case CompressionByteGrouping4:
		body, err = compressByteGrouping4LZ4(nil, payload)
	default:
		t.Fatalf("chunkFrameForTest: unsupported compression type %d", ct)
	}
	if err != nil {
		t.Fatalf("compress %d bytes as type %d: %v", len(payload), ct, err)
	}
	h := ChunkHeader{CompressedSize: uint32(len(body)), UncompressedSize: uint32(len(payload)), Compression: ct}
	frame := []byte{
		0,
		byte(h.CompressedSize), byte(h.CompressedSize >> 8), byte(h.CompressedSize >> 16),
		byte(ct),
		byte(h.UncompressedSize), byte(h.UncompressedSize >> 8), byte(h.UncompressedSize >> 16),
	}
	return append(frame, body...), h
}

// raceEnabledForTest reports whether the test binary was built with -race, under which sync.Pool drops items at random.
func raceEnabledForTest() bool {
	info, ok := debug.ReadBuildInfo()
	return ok && slices.Contains(info.Settings, debug.BuildSetting{Key: "-race", Value: "true"})
}

func TestChunkHeaderSizeIsEightBytes(t *testing.T) {
	if ChunkHeaderSize != 8 {
		t.Fatalf("ChunkHeaderSize = %d, want 8: version, 3-byte compressed size, compression type, 3-byte uncompressed size", ChunkHeaderSize)
	}
}

func TestParseChunkHeaderFields(t *testing.T) {
	got, err := ParseChunkHeader([]byte{0, 0x03, 0x02, 0x01, 2, 0x05, 0x06, 0x01, 0xff})
	want := ChunkHeader{CompressedSize: 0x010203, UncompressedSize: 0x010605, Compression: CompressionByteGrouping4}
	if err != nil || got != want {
		t.Fatalf("ParseChunkHeader() = %+v, %v; want %+v from the little-endian 24-bit sizes of the first 8 bytes", got, err, want)
	}
}

func TestParseChunkHeaderReadsCompressionByteUnvalidated(t *testing.T) {
	for b, want := range []CompressionType{CompressionNone, CompressionLZ4, CompressionByteGrouping4, 3} {
		got, err := ParseChunkHeader([]byte{0, 5, 0, 0, byte(b), 5, 0, 0})
		if err != nil || got.Compression != want {
			t.Errorf("ParseChunkHeader(type byte %d) = %+v, %v; want Compression %d and no error", b, got, err, want)
		}
	}
}

func TestParseChunkHeaderRejectsShortInput(t *testing.T) {
	header := []byte{0, 1, 0, 0, 0, 1, 0, 0}
	for n := range ChunkHeaderSize {
		if _, err := ParseChunkHeader(header[:n]); err == nil {
			t.Errorf("ParseChunkHeader(%d bytes) error = nil, want an error", n)
		}
	}
}

func TestParseChunkHeaderRejectsUnsupportedVersion(t *testing.T) {
	_, err := ParseChunkHeader([]byte{1, 1, 0, 0, 0, 1, 0, 0})
	if want := "unsupported chunk version: 1"; err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("ParseChunkHeader() error = %v, want %q", err, want)
	}
}

func TestParseChunkHeaderRejectsOversizedUncompressedSize(t *testing.T) {
	size := xet.MaxChunkSize + 1
	_, err := ParseChunkHeader([]byte{0, 1, 0, 0, 0, byte(size), byte(size >> 8), byte(size >> 16)})
	want := fmt.Sprintf("invalid uncompressed chunk size: %d exceeds maximum %d", size, xet.MaxChunkSize)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("ParseChunkHeader() error = %v, want %q", err, want)
	}
}

func TestDecodeChunkMatchesDecoderRead(t *testing.T) {
	for _, class := range chunkClassesForTest {
		t.Run("class="+class.name, func(t *testing.T) {
			for _, n := range []int{1, 2, 3, 4, 4097, 4098, 4099, 4100, xet.MaxChunkSize} {
				t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
					payload := payloadForTest(n, 1)
					frame, want := chunkFrameForTest(t, class.ct, payload)

					p := make([]byte, max(len(frame), xet.MaxChunkSize))
					m, err := NewDecoder(bytes.NewReader(frame), false).Read(p)
					if err != nil || !bytes.Equal(p[:m], payload) {
						t.Fatalf("Decoder.Read() = %d bytes, %v; want the %d-byte payload", m, err, n)
					}

					h, err := ParseChunkHeader(frame[:ChunkHeaderSize])
					if err != nil || h != want {
						t.Fatalf("ParseChunkHeader() = %+v, %v; want %+v", h, err, want)
					}
					dst := make([]byte, xet.MaxChunkSize)
					for i := range dst {
						dst[i] = byte(i*31 + 7)
					}
					before := bytes.Clone(dst)
					got, err := DecodeChunk(dst, h, frame[ChunkHeaderSize:])
					if err != nil || !bytes.Equal(got, p[:m]) {
						t.Fatalf("DecodeChunk() = %d bytes, %v; want the %d bytes Decoder.Read returned", len(got), err, m)
					}
					if class.ct != CompressionNone {
						if &got[0] != &dst[0] {
							t.Fatalf("DecodeChunk() returned memory outside dst, want dst[:%d]", n)
						}
						return
					}
					if &got[0] != &frame[ChunkHeaderSize] {
						t.Errorf("DecodeChunk() of a None chunk returned a copy, want the body frame[%d:] itself", ChunkHeaderSize)
					}
					if !bytes.Equal(dst, before) {
						t.Errorf("DecodeChunk() of a None chunk wrote into dst, want dst ignored")
					}
				})
			}
		})
	}
}

func TestDecodeChunkNoneIgnoresDst(t *testing.T) {
	payload := payloadForTest(4099, 9)
	frame, h := chunkFrameForTest(t, CompressionNone, payload)
	body := frame[ChunkHeaderSize:]
	for _, tc := range []struct {
		name string
		dst  []byte
	}{
		{"nil_dst", nil},
		{"dst_shorter_than_UncompressedSize", bytes.Repeat([]byte{0xA5}, len(payload)-1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeChunk(tc.dst, h, body)
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("DecodeChunk(%d-byte dst, %+v) = %d bytes, %v; want the %d-byte body whatever dst", len(tc.dst), h, len(got), err, len(payload))
			}
			if &got[0] != &body[0] {
				t.Errorf("DecodeChunk(%d-byte dst, %+v) returned a copy, want the body itself", len(tc.dst), h)
			}
			if !bytes.Equal(tc.dst, bytes.Repeat([]byte{0xA5}, len(tc.dst))) {
				t.Errorf("DecodeChunk(%d-byte dst, %+v) wrote into dst, want dst ignored", len(tc.dst), h)
			}
		})
	}
}

func TestDecodeChunkLZ4DecodesAlikeWithAndWithoutSpareByte(t *testing.T) {
	for _, n := range []int{1, 4099, xet.MaxChunkSize} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			payload := payloadForTest(n, 5)
			frame, h := chunkFrameForTest(t, CompressionLZ4, payload)
			for _, c := range dstSparesForTest {
				t.Run(c.name, func(t *testing.T) {
					dst := make([]byte, n, n+c.spare)
					// dst[n] is scratch on the direct path: pierrec/lz4 may write it even on success.
					past := dst[min(n+1, cap(dst)):cap(dst)]
					for i := range past {
						past[i] = 0xA5
					}
					got, err := DecodeChunk(dst, h, frame[ChunkHeaderSize:])
					if err != nil || !bytes.Equal(got, payload) {
						t.Fatalf("DecodeChunk(dst len %d cap %d) = %d bytes, %v; want the %d-byte payload", n, cap(dst), len(got), err, n)
					}
					if &got[0] != &dst[0] {
						t.Errorf("DecodeChunk(dst len %d cap %d) returned memory outside dst, want dst[:%d]", n, cap(dst), n)
					}
					if !bytes.Equal(past, bytes.Repeat([]byte{0xA5}, len(past))) {
						t.Errorf("DecodeChunk(dst len %d cap %d) wrote into dst[%d:%d], want nothing past the scratch byte dst[%d] written on success", n, cap(dst), n+1, cap(dst), n)
					}
				})
			}
		})
	}
}

func TestDecodeChunkDoesNotAllocate(t *testing.T) {
	if raceEnabledForTest() {
		t.Skip("sync.Pool drops items at random under the race detector")
	}
	// lz4 and bg4 allow the one lz4stream.FrameDataBlock pierrec/lz4 allocates per frame; no public Reset path avoids it.
	maxAllocs := map[CompressionType]float64{CompressionNone: 0, CompressionLZ4: 1, CompressionByteGrouping4: 1}
	payload := payloadForTest(64<<10, 2)
	for _, class := range chunkClassesForTest {
		t.Run("class="+class.name, func(t *testing.T) {
			frame, h := chunkFrameForTest(t, class.ct, payload)
			body := frame[ChunkHeaderSize:]
			for _, c := range dstSparesForTest {
				t.Run(c.name, func(t *testing.T) {
					dst := make([]byte, len(payload), len(payload)+c.spare)
					if got, err := DecodeChunk(dst, h, body); err != nil || !bytes.Equal(got, payload) {
						t.Fatalf("warm-up DecodeChunk() = %d bytes, %v; want the %d-byte payload", len(got), err, len(payload))
					}
					allocs := testing.AllocsPerRun(1000, func() {
						if _, err := DecodeChunk(dst, h, body); err != nil {
							t.Fatalf("DecodeChunk() error = %v", err)
						}
					})
					if want := maxAllocs[class.ct]; allocs > want {
						t.Fatalf("DecodeChunk() made %v heap allocations per call with warm pools, want at most %v", allocs, want)
					}
				})
			}
		})
	}
}

// assertDecodeChunkFails checks that DecodeChunk fails with want in its message, returns nil and leaves dst untouched, with and without spare capacity.
func assertDecodeChunkFails(t *testing.T, h ChunkHeader, body []byte, dstLen int, want string) {
	t.Helper()
	size := int(h.UncompressedSize)
	for _, dstCap := range []int{dstLen, dstLen + 64} {
		dst := make([]byte, dstLen, dstCap)
		full := dst[:dstCap]
		for i := range full {
			full[i] = byte(i*31 + 7)
		}
		before := bytes.Clone(full)
		got, err := DecodeChunk(dst, h, body)
		if err == nil || !strings.Contains(err.Error(), want) || got != nil {
			t.Errorf("DecodeChunk(%+v, %d-byte body, dst len %d cap %d) = %d bytes, %v; want nil and an error containing %q", h, len(body), dstLen, dstCap, len(got), err, want)
		}
		// Past the pre-decode checks, LZ4 decodes straight into a dst with a spare byte and may leave output in dst[:size+1].
		from := 0
		if h.Compression == CompressionLZ4 && len(body) == int(h.CompressedSize) && size <= xet.MaxChunkSize && dstLen >= size && dstCap > size {
			from = size + 1
		}
		if !bytes.Equal(full[from:], before[from:]) {
			t.Errorf("failed DecodeChunk(%+v, %d-byte body, dst len %d cap %d) wrote into dst[%d:%d]", h, len(body), dstLen, dstCap, from, dstCap)
		}
	}
}

func TestDecodeChunkErrors(t *testing.T) {
	payload := payloadForTest(4099, 3)
	n := len(payload)
	for _, class := range chunkClassesForTest {
		frame, h := chunkFrameForTest(t, class.ct, payload)
		body := frame[ChunkHeaderSize:]
		t.Run("class="+class.name, func(t *testing.T) {
			t.Run("body_shorter_than_CompressedSize", func(t *testing.T) {
				assertDecodeChunkFails(t, h, body[:len(body)-1], n, "")
			})
			t.Run("body_longer_than_CompressedSize", func(t *testing.T) {
				assertDecodeChunkFails(t, h, append(bytes.Clone(body), 0), n, "")
			})
			t.Run("UncompressedSize_above_decoded_length", func(t *testing.T) {
				long := h
				long.UncompressedSize++
				assertDecodeChunkFails(t, long, body, n+1, fmt.Sprintf("decompressed size mismatch: got %d, expected %d", n, n+1))
			})
			if class.ct == CompressionNone {
				t.Run("UncompressedSize_below_body_length", func(t *testing.T) {
					short := h
					short.UncompressedSize--
					assertDecodeChunkFails(t, short, body, n+1, fmt.Sprintf("decompressed size mismatch: got %d, expected %d", n, n-1))
				})
				return
			}
			t.Run("dst_shorter_than_UncompressedSize", func(t *testing.T) {
				assertDecodeChunkFails(t, h, body, n-1, fmt.Sprintf("output buffer too small: need %d bytes, have %d", n, n-1))
			})
			t.Run("corrupt_LZ4_frame", func(t *testing.T) {
				corrupt := bytes.Clone(body)
				corrupt[0] ^= 0xFF // LZ4 frame magic
				assertDecodeChunkFails(t, h, corrupt, n, "failed to decompress LZ4: ")
			})
		})
	}
	t.Run("unknown_compression", func(t *testing.T) {
		h := ChunkHeader{CompressedSize: 5, UncompressedSize: 5, Compression: 3}
		assertDecodeChunkFails(t, h, []byte("hello"), 5, "unsupported compression type: 3")
	})
}

func TestDecodeChunkRejectsFrameDecodingPastUncompressedSize(t *testing.T) {
	payload := payloadForTest(4100, 6)
	for _, class := range chunkClassesForTest {
		if class.ct == CompressionNone {
			continue
		}
		t.Run("class="+class.name, func(t *testing.T) {
			frame, h := chunkFrameForTest(t, class.ct, payload)
			h.UncompressedSize = 4000
			assertDecodeChunkFails(t, h, frame[ChunkHeaderSize:], int(h.UncompressedSize), "decompressed size mismatch")
		})
	}
}

func TestDecodeChunkRejectsCorruptContentChecksum(t *testing.T) {
	payload := payloadForTest(4100, 7)
	for _, class := range chunkClassesForTest {
		if class.ct == CompressionNone {
			continue
		}
		t.Run("class="+class.name, func(t *testing.T) {
			// Xorbs stored before frames matched xet-core's carry an xxh32 content checksum.
			src := payload
			if class.ct == CompressionByteGrouping4 {
				src = applyByteGrouping4(nil, payload)
			}
			var buf bytes.Buffer
			w := lz4.NewWriter(&buf)
			if err := w.Apply(lz4.ChecksumOption(true), lz4.BlockSizeOption(lz4.Block64Kb)); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Write(src); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			body := buf.Bytes()
			h := ChunkHeader{CompressedSize: uint32(len(body)), UncompressedSize: uint32(len(payload)), Compression: class.ct}
			if got, err := DecodeChunk(make([]byte, h.UncompressedSize), h, body); err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("DecodeChunk(%+v) of the intact frame = %d bytes, %v; want the %d-byte payload", h, len(got), err, len(payload))
			}
			body[len(body)-1] ^= 0xFF // last byte of the frame's xxh32 content checksum
			assertDecodeChunkFails(t, h, body, int(h.UncompressedSize), "checksum")
		})
	}
}

func TestDecodeChunkRejectsBytesAfterFrameEnd(t *testing.T) {
	payload := payloadForTest(4100, 8)
	// The reader takes the first 4 trailing bytes for the magic number of a concatenated frame.
	trailers := [][]byte{bytes.Repeat([]byte{0xAA}, 4), bytes.Repeat([]byte{0xAA}, 8), make([]byte, 4)}
	for _, class := range chunkClassesForTest {
		if class.ct == CompressionNone {
			continue
		}
		t.Run("class="+class.name, func(t *testing.T) {
			frame, h := chunkFrameForTest(t, class.ct, payload)
			for _, trailer := range trailers {
				t.Run(fmt.Sprintf("trailer=%x", trailer), func(t *testing.T) {
					body := append(bytes.Clone(frame[ChunkHeaderSize:]), trailer...)
					h.CompressedSize = uint32(len(body))
					assertDecodeChunkFails(t, h, body, int(h.UncompressedSize), "bad magic number")
				})
			}
		})
	}
}

func TestDecodeChunkConcurrentMixedClasses(t *testing.T) {
	type sample struct {
		payload []byte
		body    []byte
		h       ChunkHeader
	}
	var samples []sample
	for i, n := range []int{3, 4097, 64 << 10} {
		payload := payloadForTest(n, byte(10+i))
		for _, class := range chunkClassesForTest {
			frame, h := chunkFrameForTest(t, class.ct, payload)
			samples = append(samples, sample{payload, frame[ChunkHeaderSize:], h})
		}
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for g := range 64 {
		wg.Go(func() {
			<-start
			dst := make([]byte, xet.MaxChunkSize)
			for i := range samples {
				s := samples[(g+i)%len(samples)]
				if got, err := DecodeChunk(dst, s.h, s.body); err != nil || !bytes.Equal(got, s.payload) {
					t.Errorf("goroutine %d: DecodeChunk(%+v) = %d bytes, %v; want its %d-byte payload", g, s.h, len(got), err, len(s.payload))
					return
				}
			}
		})
	}
	close(start)
	wg.Wait()
}

func BenchmarkDecodeChunk(b *testing.B) {
	payload := payloadForTest(64<<10, 4)
	for _, class := range chunkClassesForTest {
		b.Run("class="+class.name, func(b *testing.B) {
			frame, h := chunkFrameForTest(b, class.ct, payload)
			body := frame[ChunkHeaderSize:]
			dst := make([]byte, len(payload))
			if got, err := DecodeChunk(dst, h, body); err != nil || !bytes.Equal(got, payload) {
				b.Fatalf("DecodeChunk() = %d bytes, %v; want the %d-byte payload", len(got), err, len(payload))
			}
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := DecodeChunk(dst, h, body); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
