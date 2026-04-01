package xet

// EncodeSHA256ForMetadata converts a raw SHA-256 digest to the xet-core metadata format.
// xet-core stores non-empty file SHA-256 values with each 8-byte segment reversed.
func EncodeSHA256ForMetadata(sum [32]byte) [32]byte {
	var encoded [32]byte
	for segment := range len(sum) / 8 {
		start := segment * 8
		for offset := range 8 {
			encoded[start+offset] = sum[start+7-offset]
		}
	}
	return encoded
}
