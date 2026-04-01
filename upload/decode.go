package upload

import (
	"fmt"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// DecodeXorb deserializes a xorb from an upload request body and verifies
// that the computed hash matches the expected hash. Used by the server when
// receiving xorb uploads.
func DecodeXorb(body io.Reader, expectedHash xet.Hash) (*xorb.Xorb, error) {
	xorbObj, err := xorb.Decode(body, true)
	if err != nil {
		return nil, fmt.Errorf("invalid xorb format: %w", err)
	}

	if xorbObj.Hash != expectedHash {
		return nil, fmt.Errorf("hash mismatch: xorb has %s, expected %s", xorbObj.Hash.String(), expectedHash.String())
	}

	return xorbObj, nil
}

// DecodeShard deserializes and validates a shard from an upload request body.
// Used by the server when receiving shard uploads.
func DecodeShard(body io.Reader) (*shard.Shard, error) {
	shardObj, err := shard.Decode(body)
	if err != nil {
		return nil, err
	}

	if err := validateShardUpload(shardObj); err != nil {
		return nil, err
	}

	return shardObj, nil
}

func validateShardUpload(shardObj *shard.Shard) error {
	if shardObj.Header.FooterSize != 0 {
		return fmt.Errorf("invalid shard upload: footer must be omitted")
	}

	casByHash := make(map[xet.Hash]shard.CASBlock, len(shardObj.CASInfos))
	for i, casBlock := range shardObj.CASInfos {
		if casBlock.CASFlags != 0 {
			return fmt.Errorf("invalid CAS block %d: CAS flags must be 0", i)
		}

		var unpackedTotal uint32
		for j, chunk := range casBlock.Chunks {
			if chunk.Flags&^shard.ChunkGlobalDedupEligible != 0 {
				return fmt.Errorf("invalid CAS block %d chunk %d: unknown chunk flags set", i, j)
			}
			if chunk.ByteRangeStart != unpackedTotal {
				return fmt.Errorf("invalid CAS block %d chunk %d: non-contiguous byte range start", i, j)
			}
			unpackedTotal += chunk.UnpackedSegBytes
		}

		if unpackedTotal != casBlock.NumBytesInCAS {
			return fmt.Errorf("invalid CAS block %d: NumBytesInCAS mismatch", i)
		}

		casByHash[casBlock.CASHash] = casBlock
	}

	const allowedFileFlags = shard.FileWithVerification | shard.FileWithMetadataExt
	for i, fileBlock := range shardObj.Files {
		if fileBlock.Flags&^allowedFileFlags != 0 {
			return fmt.Errorf("invalid file block %d: unknown file flags set", i)
		}

		if fileBlock.Flags&shard.FileWithVerification != 0 {
			if len(fileBlock.Verification) != len(fileBlock.Entries) {
				return fmt.Errorf("invalid file block %d: verification entry count mismatch", i)
			}
		} else if len(fileBlock.Verification) != 0 {
			return fmt.Errorf("invalid file block %d: verification entries present without flag", i)
		}

		if fileBlock.Flags&shard.FileWithMetadataExt != 0 {
			if fileBlock.MetadataExt == nil {
				return fmt.Errorf("invalid file block %d: metadata extension missing", i)
			}
		} else if fileBlock.MetadataExt != nil {
			return fmt.Errorf("invalid file block %d: metadata extension present without flag", i)
		}

		for j, entry := range fileBlock.Entries {
			if entry.CASFlags != 0 {
				return fmt.Errorf("invalid file block %d entry %d: CAS flags must be 0", i, j)
			}
			if entry.ChunkIndexEnd <= entry.ChunkIndexStart {
				return fmt.Errorf("invalid file block %d entry %d: invalid chunk index range", i, j)
			}

			casBlock, ok := casByHash[entry.CASHash]
			if !ok {
				continue
			}

			if entry.ChunkIndexEnd > uint32(len(casBlock.Chunks)) {
				return fmt.Errorf("invalid file block %d entry %d: chunk index out of bounds", i, j)
			}

			var termBytes uint32
			for k := entry.ChunkIndexStart; k < entry.ChunkIndexEnd; k++ {
				termBytes += casBlock.Chunks[k].UnpackedSegBytes
			}
			if termBytes != entry.UnpackedSegBytes {
				return fmt.Errorf("invalid file block %d entry %d: unpacked bytes mismatch", i, j)
			}
		}
	}

	return nil
}
