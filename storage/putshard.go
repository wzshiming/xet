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

// Empty files use zero digests instead of the SHA-256 of an empty stream.
func ComputeFileHashes(ctx context.Context, fileBlock *shard.FileBlock, xorbs Storage) (digest [32]byte, fileHash xet.FileHash, err error) {
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

		offsets, err := xorbs.GetXorbChunkOffsets(ctx, "", entry.CASHash)
		if err != nil {
			return digest, fileHash, fmt.Errorf("locate xorb chunks: %w", err)
		}
		start, end, err := xorb.ChunkDataRangeFromOffsets(offsets, entry.ChunkIndexStart, entry.ChunkIndexEnd)
		if err != nil {
			return digest, fileHash, fmt.Errorf("locate xorb chunks: %w", err)
		}
		rc, err := xorbs.GetXorbRangeReadCloser(ctx, "", entry.CASHash, start, end)
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
	}
	copy(digest[:], h.Sum(nil))
	fileHash = xet.ComputeFileHash(chunkHashes, chunkSizes)
	return digest, fileHash, nil
}

// verifyCASBlock checks the declared chunk sequence against every chunk of the stored xorb.
func verifyCASBlock(ctx context.Context, cb *shard.CASBlock, xorbs Storage) error {
	rc, err := xorbs.GetXorbReadSeekCloser(ctx, "", cb.CASHash)
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

func PrepareShard(ctx context.Context, s *shard.Shard, xorbs Storage) error {
	if len(s.Files) == 0 {
		return fmt.Errorf("shard has no file blocks")
	}
	for i := range s.CASInfos {
		if err := verifyCASBlock(ctx, &s.CASInfos[i], xorbs); err != nil {
			return err
		}
	}
	for i := range s.Files {
		computed, fileHash, err := ComputeFileHashes(ctx, &s.Files[i], xorbs)
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
func PutShardIndexes(ctx context.Context, s *shard.Shard, shardHash string, put func(ctx context.Context, kind, name string, value []byte) error) error {
	value := []byte(shardHash)
	for _, casBlock := range s.CASInfos {
		for _, chunk := range casBlock.Chunks {
			if err := put(ctx, "index/chunks", chunk.ChunkHash.String(), value); err != nil {
				return fmt.Errorf("write chunk index: %w", err)
			}
		}
	}
	for _, file := range s.Files {
		if err := put(ctx, "index/sha256", file.MetadataExt.SHA256Hash.String(), value); err != nil {
			return fmt.Errorf("write SHA-256 index for file %s: %w", file.FileHash.String(), err)
		}
	}
	for _, file := range s.Files {
		if err := put(ctx, "index/files", file.FileHash.String(), value); err != nil {
			return fmt.Errorf("write file index for file %s: %w", file.FileHash.String(), err)
		}
	}
	return nil
}
