package xorb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
)

func TestLZ4CompressionDecompression(t *testing.T) {
	// Test with various sizes to ensure io.ReadFull works correctly
	testSizes := []int{
		100,     // Small
		10000,   // Medium
		100000,  // Large
		1000000, // Very large (1MB)
	}

	for _, size := range testSizes {
		t.Run(string(rune(size)), func(t *testing.T) {
			// Create test data
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i % 256)
			}

			// Compress
			compressed, err := compressLZ4(nil, data)
			if err != nil {
				t.Fatalf("Compression failed: %v", err)
			}

			// Decompress
			decompressed, err := decompressLZ4(make([]byte, 0, size+1), compressed, size)
			if err != nil {
				t.Fatalf("Decompression failed: %v", err)
			}

			// Verify
			if len(decompressed) != size {
				t.Errorf("Size mismatch: got %d, expected %d", len(decompressed), size)
			}

			if !bytes.Equal(data, decompressed) {
				t.Error("Decompressed data doesn't match original")
			}
		})
	}
}

func TestByteGrouping4Compression(t *testing.T) {
	// Test ByteGrouping4 + LZ4 with large data
	testSizes := []int{
		100,
		10000,
		100000,
	}

	for _, size := range testSizes {
		t.Run(string(rune(size)), func(t *testing.T) {
			// Create test data with structured pattern (good for ByteGrouping4)
			data := make([]byte, size)
			for i := 0; i < size; i += 4 {
				data[i] = 0x01
				if i+1 < size {
					data[i+1] = 0x02
				}
				if i+2 < size {
					data[i+2] = 0x03
				}
				if i+3 < size {
					data[i+3] = 0x04
				}
			}

			// Compress with ByteGrouping4
			compressed, err := compressByteGrouping4LZ4(nil, data)
			if err != nil {
				t.Fatalf("Compression failed: %v", err)
			}

			// Decompress
			decompressed, err := decompressByteGrouping4LZ4(make([]byte, 0, size), compressed, size)
			if err != nil {
				t.Fatalf("Decompression failed: %v", err)
			}

			// Verify
			if len(decompressed) != size {
				t.Errorf("Size mismatch: got %d, expected %d", len(decompressed), size)
			}

			if !bytes.Equal(data, decompressed) {
				t.Error("Decompressed data doesn't match original")
			}
		})
	}
}

func TestByteGrouping4Transform(t *testing.T) {
	// Test the transform and reverse with various sizes
	testData := [][]byte{
		[]byte("Hello"),                                  // Not multiple of 4
		[]byte("Hello World!"),                           // Multiple of 4
		[]byte("ABCDEFGHIJKLMNOP"),                       // 16 bytes
		[]byte{0x01, 0x02, 0x03},                         // 3 bytes
		[]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, // 7 bytes
	}

	for i, data := range testData {
		t.Run(string(rune(i)), func(t *testing.T) {
			// Apply transform
			transformed, err := applyByteGrouping4(make([]byte, 0, len(data)), data)
			if err != nil {
				t.Fatalf("Transform failed: %v", err)
			}

			if len(transformed) != len(data) {
				t.Errorf("Transform changed size: got %d, expected %d", len(transformed), len(data))
			}

			// Reverse transform
			reversed, err := reverseByteGrouping4(make([]byte, 0, len(data)), transformed)
			if err != nil {
				t.Fatalf("Reverse transform failed: %v", err)
			}

			if len(reversed) != len(data) {
				t.Errorf("Reverse changed size: got %d, expected %d", len(reversed), len(data))
			}

			if !bytes.Equal(data, reversed) {
				t.Errorf("Round-trip failed. Original: %v, Got: %v", data, reversed)
			}
		})
	}
}

func TestCompressChunk(t *testing.T) {
	// Test that compressChunk works correctly with the scheme chooseCompression picks
	testCases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"small", []byte("Hello World!")},
		{"random", make([]byte, 1000)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Fill random data for that test case
			if tc.name == "random" {
				for i := range tc.data {
					tc.data[i] = byte(i % 256)
				}
			}

			compressed, compressionType, err := compressChunk(nil, tc.data, chooseCompression(tc.data))
			if err != nil {
				t.Fatalf("Compression failed: %v", err)
			}

			// Decompress based on type
			var decompressed []byte
			switch compressionType {
			case CompressionNone:
				decompressed = compressed
			case CompressionLZ4:
				decompressed, err = decompressLZ4(make([]byte, 0, len(tc.data)+1), compressed, len(tc.data))
			case CompressionByteGrouping4:
				decompressed, err = decompressByteGrouping4LZ4(make([]byte, 0, len(tc.data)), compressed, len(tc.data))
			default:
				t.Fatalf("Unknown compression type: %d", compressionType)
			}

			if err != nil {
				t.Fatalf("Decompression failed: %v", err)
			}

			if !bytes.Equal(tc.data, decompressed) {
				t.Error("Decompressed data doesn't match original")
			}
		})
	}
}

// TestCompressLZ4WritesXetCoreFrames pins the frames of xet-core's lz4_flex FrameEncoder::new: FrameInfo::default() with the Auto block size.
func TestCompressLZ4WritesXetCoreFrames(t *testing.T) {
	codecs := []struct {
		name       string
		compress   func(dst, data []byte) ([]byte, error)
		decompress func(dst, data []byte, n int) ([]byte, error)
	}{
		{"lz4", compressLZ4, decompressLZ4},
		{"bg4", compressByteGrouping4LZ4, decompressByteGrouping4LZ4},
	}
	// BD bits 4-6 hold the block maximum size id: 4 = 64 KB, 5 = 256 KB, 7 = 4 MB.
	// bg4 inputs must fit the pooled chunk buffer its transform writes into.
	sizes := []struct {
		n       int
		bd      byte
		lz4Only bool
	}{
		{1, 0x40, false},
		{100, 0x40, false},
		{64 << 10, 0x40, false},
		{64<<10 + 1, 0x50, false},
		{xet.MaxChunkSize, 0x50, false},
		{256 << 10, 0x50, true},
		{256<<10 + 1, 0x70, true},
	}
	// Rounds over all sizes make later calls reuse pooled writers, also after other block sizes.
	for call := 1; call <= 3; call++ {
		for _, size := range sizes {
			for _, c := range codecs {
				if size.lz4Only && c.name != "lz4" {
					continue
				}
				t.Run(fmt.Sprintf("%s/n=%d/call=%d", c.name, size.n, call), func(t *testing.T) {
					data := payloadForTest(size.n, byte(size.n))
					frame, err := c.compress(nil, data)
					if err != nil {
						t.Fatalf("compress %d bytes: %v", size.n, err)
					}
					if frame[4] != 0x60 {
						t.Errorf("FLG = %#02x, want 0x60: version 1, independent blocks, no block or content checksum, no content size, no dictionary", frame[4])
					}
					if frame[5] != size.bd {
						t.Errorf("BD = %#02x, want %#02x, the block size lz4_flex picks for %d bytes", frame[5], size.bd, size.n)
					}
					if end := frame[len(frame)-4:]; !bytes.Equal(end, []byte{0, 0, 0, 0}) {
						t.Errorf("frame ends with %x, want the end mark 00000000 and no content checksum", end)
					}
					if got, err := c.decompress(make([]byte, 0, size.n+1), frame, size.n); err != nil || !bytes.Equal(got, data) {
						t.Errorf("round trip = %d bytes, %v; want the %d-byte input", len(got), err, size.n)
					}
				})
			}
		}
	}
}

func TestAppendToDestBufferReuse(t *testing.T) {
	// Test that append-to-dest mode correctly reuses pre-allocated buffers.
	data := make([]byte, 10000)
	for i := range data {
		data[i] = byte(i % 256)
	}

	t.Run("compressLZ4", func(t *testing.T) {
		dst := make([]byte, 0, 65536)
		result, err := compressLZ4(dst, data)
		if err != nil {
			t.Fatal(err)
		}
		// Decompress and verify
		decompressed, err := decompressLZ4(make([]byte, 0, len(data)+1), result, len(data))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, decompressed) {
			t.Error("Decompressed data doesn't match original")
		}
	})

	t.Run("decompressLZ4_reuse", func(t *testing.T) {
		compressed, err := compressLZ4(nil, data)
		if err != nil {
			t.Fatal(err)
		}
		// Reuse a buffer across multiple decompressions
		buf := make([]byte, 0, len(data)+1)
		for range 3 {
			buf, err = decompressLZ4(buf[:0], compressed, len(data))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(data, buf) {
				t.Error("Decompressed data doesn't match original")
			}
		}
	})

	t.Run("byteGrouping4_roundtrip_reuse", func(t *testing.T) {
		transformBuf := make([]byte, 0, len(data))
		reverseBuf := make([]byte, 0, len(data))
		for range 3 {
			var err error
			if transformBuf, err = applyByteGrouping4(transformBuf[:0], data); err != nil {
				t.Fatal(err)
			}
			if reverseBuf, err = reverseByteGrouping4(reverseBuf[:0], transformBuf); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(data, reverseBuf) {
				t.Error("Round-trip with buffer reuse failed")
			}
		}
	})

	t.Run("DecodeChunk_reuse", func(t *testing.T) {
		compressed, ct, err := compressChunk(nil, data, chooseCompression(data))
		if err != nil {
			t.Fatal(err)
		}
		h := ChunkHeader{CompressedSize: uint32(len(compressed)), UncompressedSize: uint32(len(data)), Compression: ct}
		buf := make([]byte, len(data)+1)
		for range 3 {
			got, err := DecodeChunk(buf, h, compressed)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(data, got) {
				t.Error("Decompressed data doesn't match original")
			}
		}
	})

	t.Run("compressChunk_reuse", func(t *testing.T) {
		for _, scheme := range []CompressionType{CompressionLZ4, CompressionByteGrouping4} {
			want, wantType, err := compressChunk(nil, data, scheme)
			if err != nil {
				t.Fatal(err)
			}
			var buf []byte
			for range 3 {
				var ct CompressionType
				buf, ct, err = compressChunk(buf[:0], data, scheme)
				if err != nil || ct != wantType || !bytes.Equal(buf, want) {
					t.Fatalf("compressChunk(reused dst, scheme %d) = type %d, %d bytes, %v; want type %d and the %d bytes of a nil dst", scheme, ct, len(buf), err, wantType, len(want))
				}
			}
		}
	})
}

// TestCodecsRejectDstWithoutSpareCapacity: a dst one byte short fails untouched; an exact one is filled in place.
func TestCodecsRejectDstWithoutSpareCapacity(t *testing.T) {
	data := payloadForTest(4099, 12)
	lz4Frame, err := compressLZ4(nil, data)
	if err != nil {
		t.Fatal(err)
	}
	bg4Frame, err := compressByteGrouping4LZ4(nil, data)
	if err != nil {
		t.Fatal(err)
	}
	prefix := []byte{0xde, 0xad, 0xbe, 0xef, 0x01}
	for _, c := range []struct {
		name string
		need int
		run  func(dst []byte) ([]byte, error)
	}{
		{"decompressLZ4", len(data) + 1, func(dst []byte) ([]byte, error) { return decompressLZ4(dst, lz4Frame, len(data)) }},
		{"decompressByteGrouping4LZ4", len(data), func(dst []byte) ([]byte, error) { return decompressByteGrouping4LZ4(dst, bg4Frame, len(data)) }},
		{"applyByteGrouping4", len(data), func(dst []byte) ([]byte, error) { return applyByteGrouping4(dst, data) }},
		{"reverseByteGrouping4", len(data), func(dst []byte) ([]byte, error) { return reverseByteGrouping4(dst, data) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			short := append(make([]byte, 0, len(prefix)+c.need-1), prefix...)
			full := short[:cap(short)]
			for i := len(prefix); i < len(full); i++ {
				full[i] = 0xA5
			}
			before := bytes.Clone(full)
			got, err := c.run(short)
			want := fmt.Sprintf("output buffer too small: need capacity %d, have %d", len(prefix)+c.need, len(prefix)+c.need-1)
			if err == nil || err.Error() != want || !bytes.Equal(got, prefix) {
				t.Errorf("%s(dst with %d spare bytes) = %d bytes, %v; want dst unchanged and %q", c.name, c.need-1, len(got), err, want)
			}
			if !bytes.Equal(full, before) {
				t.Errorf("failed %s(dst with %d spare bytes) wrote into dst", c.name, c.need-1)
			}

			exact := append(make([]byte, 0, len(prefix)+c.need), prefix...)
			got, err = c.run(exact)
			if err != nil || &got[0] != &exact[0] || !bytes.Equal(got[:len(prefix)], prefix) {
				t.Errorf("%s(dst with %d spare bytes) = %d bytes, %v; want the output appended to dst in place", c.name, c.need, len(got), err)
			}
		})
	}
}

func BenchmarkCompressLZ4(b *testing.B) {
	data := make([]byte, 65536)
	for i := range data {
		data[i] = byte(i % 256)
	}
	b.ResetTimer()
	b.ReportAllocs()
	var buf []byte
	for b.Loop() {
		var err error
		buf, err = compressLZ4(buf[:0], data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecompressLZ4(b *testing.B) {
	data := make([]byte, 65536)
	for i := range data {
		data[i] = byte(i % 256)
	}
	compressed, err := compressLZ4(nil, data)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	buf := make([]byte, 0, len(data)+1)
	for b.Loop() {
		buf, err = decompressLZ4(buf[:0], compressed, len(data))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// selectionClassesForTest are BenchmarkCompressChunk's chunks and the type each must get; bg4 and text are
// genBG4 and genLZ4 of decode_bench_test.go.
var selectionClassesForTest = []struct {
	name string
	ct   CompressionType
	fill func(src *rand.ChaCha8, p []byte)
}{
	{"random", CompressionNone, func(src *rand.ChaCha8, p []byte) {
		_, _ = src.Read(p)
	}},
	{"bg4", CompressionByteGrouping4, func(src *rand.ChaCha8, p []byte) {
		r := rand.New(src)
		hi := r.Uint32()
		for i := range len(p) / 4 {
			if r.IntN(64) == 0 {
				hi++
			}
			binary.LittleEndian.PutUint32(p[4*i:], hi<<16|r.Uint32N(1<<16))
		}
	}},
	// A rough stand-in for bf16 weights: a random low byte and a high byte in 0x3c-0x3e.
	{"bf16-like", CompressionByteGrouping4, func(src *rand.ChaCha8, p []byte) {
		r := rand.New(src)
		for i := range len(p) / 2 {
			binary.LittleEndian.PutUint16(p[2*i:], uint16(0x3c+r.IntN(3))<<8|uint16(r.IntN(256)))
		}
	}},
	{"text", CompressionLZ4, func(src *rand.ChaCha8, p []byte) {
		r := rand.New(src)
		words := strings.Fields("lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua")
		for i := 0; i < len(p); {
			i += copy(p[i:], words[r.IntN(len(words))])
			if i < len(p) {
				p[i] = ' '
				i++
			}
		}
	}},
}

func BenchmarkCompressChunk(b *testing.B) {
	for _, class := range selectionClassesForTest {
		b.Run("class="+class.name, func(b *testing.B) {
			data := selectionChunkForTest(b, class.name, xet.TargetChunkSize)
			scheme := chooseCompression(data)
			buf, ct, err := compressChunk(nil, data, scheme)
			if err != nil || ct != class.ct {
				b.Fatalf("compressChunk(class %s) = type %d, %v; want type %d", class.name, ct, err, class.ct)
			}
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			for b.Loop() {
				buf, _, _ = compressChunk(buf[:0], data, scheme)
			}
		})
	}
}

// selectionChunkForTest returns n bytes of the selectionClassesForTest class named class, seeded with its name.
func selectionChunkForTest(tb testing.TB, class string, n int) []byte {
	tb.Helper()
	for _, c := range selectionClassesForTest {
		if c.name == class {
			var seed [32]byte
			copy(seed[:], class)
			data := make([]byte, n)
			c.fill(rand.NewChaCha8(seed), data)
			return data
		}
	}
	tb.Fatalf("no selection class %q", class)
	return nil
}

func TestBG4MaxKLDivergenceMatchesXetCore(t *testing.T) {
	// max_kl_div of BG4Predictor::bg4_recommended (xet-core v1.8.0 bg4_prediction.rs) on the same 64 KiB chunks.
	for _, c := range []struct {
		class string
		want  float64
	}{
		{"random", 2.4972461959398473e-5},
		{"bg4", 0.03637494117851614},
		{"bf16-like", 0.17244787130345923},
	} {
		// Written so that a NaN fails too.
		if got := bg4MaxKLDivergence(selectionChunkForTest(t, c.class, xet.TargetChunkSize)); !(math.Abs(got-c.want) <= 1e-12*c.want) {
			t.Errorf("bg4MaxKLDivergence(class %s) = %v, want xet-core's %v", c.class, got, c.want)
		}
	}
}

// The random chunk with bf16 high bytes 0x3c-0x3e at position 3 of its first k groups: k = 3751 and 3752, one byte
// apart, straddle xet-core's 0.02, so the first gets LZ4 and the second ByteGrouping4.
func TestChooseCompressionThresholdMatchesXetCore(t *testing.T) {
	// max_kl_div of BG4Predictor::bg4_recommended (xet-core v1.8.0 bg4_prediction.rs) on the same chunks.
	for _, c := range []struct {
		k      int
		wantKL float64
		want   CompressionType
	}{
		{3751, 0.0199939157191288, CompressionLZ4},
		{3752, 0.020004114314403257, CompressionByteGrouping4},
	} {
		data := selectionChunkForTest(t, "random", xet.TargetChunkSize)
		for i := range c.k {
			data[4*i+3] = 0x3c + byte(i%3)
		}
		if got := bg4MaxKLDivergence(data); !(math.Abs(got-c.wantKL) <= 1e-12*c.wantKL) {
			t.Errorf("bg4MaxKLDivergence(k=%d) = %v, want xet-core's %v", c.k, got, c.wantKL)
		}
		if got := chooseCompression(data); got != c.want {
			t.Errorf("chooseCompression(k=%d, max KL %v) = type %d, want %d", c.k, c.wantKL, got, c.want)
		}
	}
}

// The high bytes of a bf16-like chunk leave empty popcount bins, so their positions' KL divergences are NaN (0·ln 0),
// which Rust's f64::max skips: the low bytes' positions still recommend BG4.
func TestChooseCompressionSkipsPositionsWithEmptyPopcountBins(t *testing.T) {
	data := selectionChunkForTest(t, "bf16-like", xet.TargetChunkSize)
	for i := 1; i < len(data); i += 2 {
		if n := bits.OnesCount8(data[i]); n != 4 && n != 5 {
			t.Fatalf("high byte %d has popcount %d, want only 4 and 5 in its positions", i, n)
		}
	}
	if got := chooseCompression(data); got != CompressionByteGrouping4 {
		t.Errorf("chooseCompression(bf16-like chunk) = type %d, want ByteGrouping4 (%d)", got, CompressionByteGrouping4)
	}
}

// popcountHistograms counts what xet-core's add_data_reference counts byte by byte, whatever the length and bytes.
func TestPopcountHistogramsMatchStraightforwardLoop(t *testing.T) {
	inputs := map[string][]byte{}
	for _, class := range selectionClassesForTest {
		chunk := selectionChunkForTest(t, class.name, xet.MaxChunkSize)
		for _, n := range []int{0, 1, 2, 3, 4, 5, 6, 7, 507, 508, 509, 510, 511, 1015, 1016, 1017, 4097, xet.TargetChunkSize - 1, xet.TargetChunkSize, xet.TargetChunkSize + 2, xet.MaxChunkSize - 3, xet.MaxChunkSize} {
			inputs[fmt.Sprintf("%s/n=%d", class.name, n)] = chunk[:n]
		}
	}
	// As 257 is 1 mod 4, i%257 puts every byte value at every position mod 4.
	everyValue := make([]byte, 4*257)
	for i := range everyValue {
		everyValue[i] = byte(i % 257)
	}
	inputs["every byte value"] = everyValue
	for _, v := range []byte{0x00, 0x0f, 0xfe, 0xff} {
		inputs[fmt.Sprintf("run of %#02x", v)] = bytes.Repeat([]byte{v}, xet.MaxChunkSize-1)
	}
	for name, data := range inputs {
		var want [4][9]uint32
		for i, b := range data {
			want[i%4][bits.OnesCount8(b)]++
		}
		if got := popcountHistograms(data); got != want {
			t.Errorf("popcountHistograms(%s) = %v, want %v", name, got, want)
		}
	}
}
