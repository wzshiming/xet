package download

// ReconstructionResponse represents the response from the file reconstruction API
type ReconstructionResponse struct {
	OffsetIntoFirstRange int64                       `json:"offset_into_first_range"`
	Terms                []Term                      `json:"terms"`
	FetchInfo            map[string][]FetchInfoEntry `json:"fetch_info"`
}

// Term represents a single term in the file reconstruction
type Term struct {
	Hash           string     `json:"hash"`
	UnpackedLength uint64     `json:"unpacked_length"`
	Range          ChunkRange `json:"range"`
}

// ChunkRange represents a chunk index range [start, end)
type ChunkRange struct {
	Start uint32 `json:"start"`
	End   uint32 `json:"end"` // Exclusive
}

// FetchInfoEntry represents fetch information for downloading xorb data
type FetchInfoEntry struct {
	Range    ChunkRange `json:"range"`
	URL      string     `json:"url"`
	URLRange ByteRange  `json:"url_range"`
}

// ByteRange represents a byte range [start, end] (inclusive)
type ByteRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"` // Inclusive
}

// ReconstructionResponseV2 represents the V2 response from the file reconstruction API
// It uses a multi-range optimized format for fetching xorb data
type ReconstructionResponseV2 struct {
	OffsetIntoFirstRange int64                            `json:"offset_into_first_range"`
	Terms                []Term                           `json:"terms"`
	Xorbs                map[string][]XorbMultiRangeFetch `json:"xorbs"`
}

// XorbMultiRangeFetch represents a signed multi-range fetch entry covering multiple byte ranges for a xorb
type XorbMultiRangeFetch struct {
	URL    string                `json:"url"`
	Ranges []XorbRangeDescriptor `json:"ranges"`
}

// XorbRangeDescriptor describes a chunk/byte range within a xorb
type XorbRangeDescriptor struct {
	Chunks ChunkRange `json:"chunks"`
	Bytes  ByteRange  `json:"bytes"`
}
