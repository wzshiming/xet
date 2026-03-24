package merklehash

import (
	"fmt"
	"strings"
)

const aggregatedHashesMeanTreeBranchingFactor = 4

// nextMergeCut finds the next cut point in a sequence of hashes.
func nextMergeCut(hashes []HashWithSize) int {
	n := len(hashes)
	if n <= 2 {
		return n
	}
	end := 2*aggregatedHashesMeanTreeBranchingFactor + 1
	if end > n {
		end = n
	}
	for i := 2; i < end; i++ {
		if hashes[i].Hash.Rem(aggregatedHashesMeanTreeBranchingFactor) == 0 {
			return i + 1
		}
	}
	return end
}

// mergedHashOfSequence computes a single merged hash and total size from a sequence.
func mergedHashOfSequence(hashes []HashWithSize) HashWithSize {
	var totalSize uint64
	var b strings.Builder
	for _, hs := range hashes {
		fmt.Fprintf(&b, "%s : %d\n", hs.Hash.Hex(), hs.Size)
		totalSize += hs.Size
	}
	buf := []byte(b.String())
	return HashWithSize{
		Hash: ComputeInternalNodeHash(buf),
		Size: totalSize,
	}
}

// aggregatedNodeHash iteratively collapses a slice of HashWithSize down to a single hash.
// Each outer iteration processes the entire array left-to-right, finding all merge cuts
// and producing a shorter array. This repeats until only one element remains.
func aggregatedNodeHash(chunks []HashWithSize) DataHash {
	if len(chunks) == 0 {
		return DataHash{}
	}

	// Work on a copy to avoid mutating the caller's slice.
	work := make([]HashWithSize, len(chunks))
	copy(work, chunks)

	for len(work) > 1 {
		writeIdx := 0
		readIdx := 0

		for readIdx < len(work) {
			cut := nextMergeCut(work[readIdx:])
			nextCut := readIdx + cut
			work[writeIdx] = mergedHashOfSequence(work[readIdx:nextCut])
			writeIdx++
			readIdx = nextCut
		}

		work = work[:writeIdx]
	}

	return work[0].Hash
}

// XorbHash computes the aggregated hash for XORB data.
func XorbHash(chunks []HashWithSize) DataHash {
	return aggregatedNodeHash(chunks)
}

// FileHashWithSalt computes the aggregated file hash with an HMAC salt.
// Returns zero hash for empty input, matching the Rust implementation.
func FileHashWithSalt(chunks []HashWithSize, salt [32]byte) DataHash {
	if len(chunks) == 0 {
		return DataHash{}
	}
	h := aggregatedNodeHash(chunks)
	key, _ := FromSlice(salt[:])
	return h.HMAC(key)
}

// FileHash computes the aggregated file hash with a zero salt.
func FileHash(chunks []HashWithSize) DataHash {
	var zeroSalt [32]byte
	return FileHashWithSalt(chunks, zeroSalt)
}
