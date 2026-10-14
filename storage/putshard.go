package storage

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/pool"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// PutShardPhase is the commit stage a PutShardProgress report belongs to.
type PutShardPhase uint8

const (
	PutShardVerifying PutShardPhase = iota + 1
	PutShardWriting
	PutShardIndexing
)

// PutShardProgress receives PutShard's progress: Verifying counts the CAS
// blocks and file terms checked out of total, while Writing and Indexing
// report (0, 0) once as they start.
type PutShardProgress func(phase PutShardPhase, done, total int)

// PutShard verifies s against the stored xorbs and commits it: the shard
// object named by the SHA-256 of its encoding, then its chunk, SHA-256 and
// file index entries. A shard whose every file is already indexed is a no-op.
// It reports whether the shard object was newly written; progress may be nil.
func PutShard(ctx context.Context, st Storage, s *shard.Shard, progress PutShardProgress) (bool, error) {
	alreadyExists := len(s.Files) > 0
	for _, file := range s.Files {
		shardHash, err := st.GetFileIndexEntry(ctx, file.FileHash)
		if err != nil {
			return false, fmt.Errorf("check shard: %w", err)
		}
		if shardHash == "" {
			alreadyExists = false
			break
		}
	}
	if alreadyExists {
		return false, nil
	}

	if progress == nil {
		progress = func(PutShardPhase, int, int) {}
	}
	if err := prepareShard(ctx, s, st, progress); err != nil {
		return false, err
	}

	encoded, shardHash, err := EncodeShard(s)
	if err != nil {
		return false, fmt.Errorf("serialize shard: %w", err)
	}

	progress(PutShardWriting, 0, 0)
	inserted, err := st.PutShardObject(ctx, shardHash, encoded)
	if err != nil {
		return false, err
	}

	progress(PutShardIndexing, 0, 0)
	return inserted, putShardIndexes(ctx, st, s, shardHash)
}

// Empty files use zero digests instead of the SHA-256 of an empty stream.
func computeFileHashes(ctx context.Context, fileBlock *shard.FileBlock, xorbs Storage, termDone func()) (digest [32]byte, fileHash xet.FileHash, err error) {
	if len(fileBlock.Entries) == 0 {
		return digest, fileHash, nil
	}

	h := sha256.New()
	buf := make([]byte, xet.MaxChunkSize)
	var chunkHashes []xet.ChunkHash
	var chunkSizes []uint64
	for _, entry := range fileBlock.Entries {
		if err := ctx.Err(); err != nil {
			return digest, fileHash, err
		}

		offsets, err := xorbs.GetXorbChunkOffsets(ctx, entry.CASHash)
		if err != nil {
			return digest, fileHash, fmt.Errorf("locate xorb chunks: %w", err)
		}
		start, end, err := xorb.ChunkDataRangeFromOffsets(offsets, entry.ChunkIndexStart, entry.ChunkIndexEnd)
		if err != nil {
			return digest, fileHash, fmt.Errorf("locate xorb chunks: %w", err)
		}
		rc, err := xorbs.GetXorbRangeReadCloser(ctx, entry.CASHash, start, end)
		if err != nil {
			return digest, fileHash, fmt.Errorf("read xorb chunks: %w", err)
		}
		decoder := xorb.NewDecoder(rc, false)
		written, err := io.CopyBuffer(h, decoder, buf)
		rc.Close()
		if err != nil {
			return digest, fileHash, fmt.Errorf("decode xorb chunks: %w", err)
		}
		if written != int64(entry.UnpackedSegBytes) {
			return digest, fileHash, fmt.Errorf("reconstructed term has %d bytes, expected %d", written, entry.UnpackedSegBytes)
		}
		hashes, sizes := decoder.Chunks()
		chunkHashes = append(chunkHashes, hashes...)
		chunkSizes = append(chunkSizes, sizes...)
		termDone()
	}
	copy(digest[:], h.Sum(nil))
	fileHash = xet.ComputeFileHash(chunkHashes, chunkSizes)
	return digest, fileHash, nil
}

// verifyCASBlock checks the declared chunk sequence against every chunk of the stored xorb.
func verifyCASBlock(ctx context.Context, cb *shard.CASBlock, xorbs Storage) error {
	rc, err := xorbs.GetXorbReadSeekCloser(ctx, cb.CASHash)
	if err != nil {
		return fmt.Errorf("open xorb: %w", err)
	}
	defer rc.Close()
	buf := pool.GetChunkBuf()
	defer pool.PutChunkBuf(buf)
	name := cb.CASHash.String()
	decoder := xorb.NewDecoder(rc, false)
	for i := 0; ; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := decoder.Read(buf[:])
		if err != nil && err != io.EOF {
			return fmt.Errorf("decode xorb %s chunk %d: %w", name, i, err)
		}
		if (err == io.EOF) != (i == len(cb.Chunks)) {
			return fmt.Errorf("%w: xorb %s does not have the %d declared chunks", ErrInvalidShard, name, len(cb.Chunks))
		}
		if err == io.EOF {
			return nil
		}
		hashes, _ := decoder.Chunks()
		if hashes[i] != cb.Chunks[i].ChunkHash {
			return fmt.Errorf("%w: xorb %s chunk %d hash mismatch", ErrInvalidShard, name, i)
		}
		if n != int(cb.Chunks[i].UnpackedSegBytes) {
			return fmt.Errorf("%w: xorb %s chunk %d has %d bytes, %d declared", ErrInvalidShard, name, i, n, cb.Chunks[i].UnpackedSegBytes)
		}
	}
}

func prepareShard(ctx context.Context, s *shard.Shard, xorbs Storage, progress PutShardProgress) error {
	if len(s.Files) == 0 {
		return fmt.Errorf("shard has no file blocks")
	}
	done, total := 0, len(s.CASInfos)
	for _, file := range s.Files {
		total += len(file.Entries)
	}
	progress(PutShardVerifying, done, total)
	verified := func() {
		done++
		progress(PutShardVerifying, done, total)
	}
	for i := range s.CASInfos {
		if err := verifyCASBlock(ctx, &s.CASInfos[i], xorbs); err != nil {
			return err
		}
		verified()
	}
	for i := range s.Files {
		computed, fileHash, err := computeFileHashes(ctx, &s.Files[i], xorbs, verified)
		if err != nil {
			return fmt.Errorf("compute SHA-256 for file %s: %w", s.Files[i].FileHash.String(), err)
		}
		if s.Files[i].MetadataExt != nil && s.Files[i].MetadataExt.SHA256Hash != shard.NewSHA256Hash(computed) {
			return fmt.Errorf("%w: SHA-256 mismatch for file %s", ErrInvalidShard, s.Files[i].FileHash.String())
		}
		if s.Files[i].FileHash != fileHash {
			return fmt.Errorf("%w: file hash mismatch for file %s", ErrInvalidShard, s.Files[i].FileHash.String())
		}

		s.Files[i].MetadataExt = &shard.FileMetadataExt{SHA256Hash: shard.NewSHA256Hash(computed)}
		s.Files[i].Flags |= shard.FileWithMetadataExt
	}
	return nil
}

// File indexes commit last so partial writes remain retryable.
func putShardIndexes(ctx context.Context, st Storage, s *shard.Shard, shardHash string) error {
	for _, casBlock := range s.CASInfos {
		for _, chunk := range casBlock.Chunks {
			if err := st.PutIndexEntry(ctx, "index/chunks", chunk.ChunkHash.String(), shardHash); err != nil {
				return fmt.Errorf("write chunk index: %w", err)
			}
		}
	}
	for _, file := range s.Files {
		if err := st.PutIndexEntry(ctx, "index/sha256", file.MetadataExt.SHA256Hash.String(), shardHash); err != nil {
			return fmt.Errorf("write SHA-256 index for file %s: %w", file.FileHash.String(), err)
		}
	}
	for _, file := range s.Files {
		if err := st.PutIndexEntry(ctx, "index/files", file.FileHash.String(), shardHash); err != nil {
			return fmt.Errorf("write file index for file %s: %w", file.FileHash.String(), err)
		}
	}
	return nil
}
