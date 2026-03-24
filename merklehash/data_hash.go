package merklehash

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"lukechampine.com/blake3"
)

// DataHash is a 256-bit hash stored as [4]uint64.
// Values are stored as the numeric interpretation of little-endian bytes,
// matching Rust's [u64; 4] on little-endian platforms.
type DataHash [4]uint64

// MerkleHash is an alias for DataHash.
type MerkleHash = DataHash

// HMACKey is an alias for DataHash.
type HMACKey = DataHash

// HashWithSize pairs a DataHash with a size value.
type HashWithSize struct {
	Hash DataHash
	Size uint64
}

var dataKey = [32]byte{
	102, 151, 245, 119, 91, 149, 80, 222,
	49, 53, 203, 172, 165, 151, 24, 28,
	157, 228, 33, 16, 155, 235, 43, 88,
	180, 208, 176, 75, 147, 173, 242, 41,
}

var internalNodeHashKey = [32]byte{
	1, 126, 197, 199, 165, 71, 41, 150,
	253, 148, 102, 102, 180, 138, 2, 230,
	93, 221, 83, 111, 55, 199, 109, 210,
	248, 99, 82, 230, 74, 83, 113, 63,
}

// Hex returns the 64-character lowercase hex representation.
func (d DataHash) Hex() string {
	return fmt.Sprintf("%016x%016x%016x%016x", d[0], d[1], d[2], d[3])
}

// String returns the hex representation.
func (d DataHash) String() string {
	return d.Hex()
}

// Base64 returns the URL-safe base64 encoding with no padding.
func (d DataHash) Base64() string {
	return base64.RawURLEncoding.EncodeToString(d.AsBytes())
}

// AsBytes returns the 32-byte little-endian representation.
func (d DataHash) AsBytes() []byte {
	b := make([]byte, 32)
	binary.LittleEndian.PutUint64(b[0:8], d[0])
	binary.LittleEndian.PutUint64(b[8:16], d[1])
	binary.LittleEndian.PutUint64(b[16:24], d[2])
	binary.LittleEndian.PutUint64(b[24:32], d[3])
	return b
}

// IsZero returns true if all elements are zero.
func (d DataHash) IsZero() bool {
	return d[0] == 0 && d[1] == 0 && d[2] == 0 && d[3] == 0
}

// Rem returns d[3] modulo divisor, matching Rust's self[3].to_le() % rhs.
func (d DataHash) Rem(divisor uint64) uint64 {
	return d[3] % divisor
}

// Marker returns a DataHash with all bits set to 1.
func Marker() DataHash {
	return DataHash{^uint64(0), ^uint64(0), ^uint64(0), ^uint64(0)}
}

// FromHex parses a 64-character hex string into a DataHash.
func FromHex(h string) (DataHash, error) {
	if len(h) != 64 {
		return DataHash{}, fmt.Errorf("merklehash: hex string must be 64 characters, got %d", len(h))
	}
	var d DataHash
	for i := 0; i < 4; i++ {
		b, err := hex.DecodeString(h[i*16 : (i+1)*16])
		if err != nil {
			return DataHash{}, fmt.Errorf("merklehash: invalid hex: %w", err)
		}
		// Hex was produced by %016x of the numeric u64 value (big-endian digit order),
		// so decode as big-endian to recover the numeric value.
		d[i] = binary.BigEndian.Uint64(b)
	}
	return d, nil
}

// FromBase64 parses a URL-safe base64 (no padding) string into a DataHash.
func FromBase64(b64 string) (DataHash, error) {
	b, err := base64.RawURLEncoding.DecodeString(b64)
	if err != nil {
		return DataHash{}, fmt.Errorf("merklehash: invalid base64: %w", err)
	}
	return FromSlice(b)
}

// FromSlice creates a DataHash from a 32-byte slice.
func FromSlice(value []byte) (DataHash, error) {
	if len(value) != 32 {
		return DataHash{}, fmt.Errorf("merklehash: slice must be 32 bytes, got %d", len(value))
	}
	var d DataHash
	d[0] = binary.LittleEndian.Uint64(value[0:8])
	d[1] = binary.LittleEndian.Uint64(value[8:16])
	d[2] = binary.LittleEndian.Uint64(value[16:24])
	d[3] = binary.LittleEndian.Uint64(value[24:32])
	return d, nil
}

// ComputeDataHash computes a BLAKE3 keyed hash of data using the DATA_KEY.
func ComputeDataHash(data []byte) DataHash {
	return keyedHash(dataKey, data)
}

// ComputeInternalNodeHash computes a BLAKE3 keyed hash using the INTERNAL_NODE_HASH key.
func ComputeInternalNodeHash(data []byte) DataHash {
	return keyedHash(internalNodeHashKey, data)
}

// HMAC computes a BLAKE3 keyed hash of this hash's bytes using key as the BLAKE3 key.
func (d DataHash) HMAC(key DataHash) DataHash {
	var k [32]byte
	copy(k[:], key.AsBytes())
	return keyedHash(k, d.AsBytes())
}

// Less returns true if d is lexicographically less than other when compared as bytes.
func (d DataHash) Less(other DataHash) bool {
	db := d.AsBytes()
	ob := other.AsBytes()
	for i := 0; i < 32; i++ {
		if db[i] < ob[i] {
			return true
		}
		if db[i] > ob[i] {
			return false
		}
	}
	return false
}

// Equal returns true if both hashes are identical.
func (d DataHash) Equal(other DataHash) bool {
	return d == other
}

// Compare returns -1, 0, or 1.
func (d DataHash) Compare(other DataHash) int {
	db := d.AsBytes()
	ob := other.AsBytes()
	for i := 0; i < 32; i++ {
		if db[i] < ob[i] {
			return -1
		}
		if db[i] > ob[i] {
			return 1
		}
	}
	return 0
}

func keyedHash(key [32]byte, data []byte) DataHash {
	h := blake3.New(32, key[:])
	h.Write(data)
	sum := h.Sum(nil)
	d, _ := FromSlice(sum)
	return d
}
