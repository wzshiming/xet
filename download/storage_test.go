package download

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"mime/multipart"
	"net/http"
	"path"
	"slices"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/memory"
)

// storageClient serves xorb downloads straight from a storage.Storage.
type storageClient struct{ st storage.Storage }

func (c storageClient) DownloadXorbWithURL(ctx context.Context, url string, header http.Header) (io.ReadCloser, error) {
	xorbHash, err := xet.ParseXorbHash(path.Base(url))
	if err != nil {
		return nil, err
	}
	var start, end int64
	if _, err := fmt.Sscanf(header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil {
		return nil, fmt.Errorf("parse range %q: %w", header.Get("Range"), err)
	}
	return c.st.GetXorbRangeReadCloser(ctx, "default", xorbHash, start, end)
}

func (storageClient) DownloadXorbsMultipartWithURL(context.Context, string, http.Header) (*multipart.Reader, io.Closer, error) {
	return nil, nil, errors.New("not implemented")
}

func TestReadersMatchStorageReconstructedFile(t *testing.T) {
	ctx := context.Background()
	st := memory.NewStorage()
	random := make([]byte, 3<<19)
	_, _ = rand.NewChaCha8([32]byte{}).Read(random)
	first := slices.Concat(random[:1<<20], random[:1<<19])
	second := slices.Concat(first[:len(first)/2], random[1<<20:])
	if _, err := storage.PutFile(ctx, st, "default", bytes.NewReader(first)); err != nil {
		t.Fatal(err)
	}
	fileHash, err := storage.PutFile(ctx, st, "default", bytes.NewReader(second))
	if err != nil {
		t.Fatal(err)
	}
	sh, err := st.GetShard(ctx, fileHash)
	if err != nil {
		t.Fatal(err)
	}
	var entries []shard.FileDataSequenceEntry
	for _, file := range sh.Files {
		if file.FileHash == fileHash {
			entries = file.Entries
		}
	}
	var size int64
	xorbs := map[xet.XorbHash]bool{}
	for _, entry := range entries {
		size += int64(entry.UnpackedSegBytes)
		xorbs[entry.CASHash] = true
	}
	// Second shares only its leading half with first, so its terms span two xorbs.
	if len(entries) < 2 || len(xorbs) < 2 {
		t.Fatalf("second file has %d terms over %d xorbs, want at least 2 of each", len(entries), len(xorbs))
	}
	boundary := int64(entries[0].UnpackedSegBytes)

	readers := map[string]func(rangeHeader string, opts ...Option) (io.ReadCloser, int64, error){
		"v1": func(rangeHeader string, opts ...Option) (io.ReadCloser, int64, error) {
			resp, err := BuildReconstructionResponseV1(ctx, st, "default", sh, fileHash, rangeHeader)
			if err != nil {
				return nil, 0, err
			}
			r, err := NewReaderV1(ctx, storageClient{st}, resp, opts...)
			return r, ExpectedLengthV1(resp), err
		},
		"v2": func(rangeHeader string, opts ...Option) (io.ReadCloser, int64, error) {
			resp, err := BuildReconstructionResponseV2(ctx, st, "default", sh, fileHash, rangeHeader)
			if err != nil {
				return nil, 0, err
			}
			r, err := NewReaderV2(ctx, storageClient{st}, resp, opts...)
			return r, ExpectedLengthV2(resp), err
		},
	}
	for name, newReader := range readers {
		for _, offset := range []int64{0, 1, boundary - 1, boundary, boundary + 1, size - 1} {
			t.Run(fmt.Sprintf("%s/offset=%d", name, offset), func(t *testing.T) {
				rangeHeader, opts := "", []Option{WithCacheManager(NewCacheManager(t.TempDir(), 0))}
				if offset == 0 {
					opts = append(opts, WithExpectedFileHash(fileHash))
				} else {
					rangeHeader = fmt.Sprintf("bytes=%d-", offset)
				}
				r, length, err := newReader(rangeHeader, opts...)
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				got, err := io.ReadAll(r)
				if err != nil {
					t.Fatal(err)
				}

				rc, err := st.GetReconstructedFile(ctx, "default", sha256.Sum256(second))
				if err != nil {
					t.Fatal(err)
				}
				defer rc.Close()
				if _, err := rc.Seek(offset, io.SeekStart); err != nil {
					t.Fatal(err)
				}
				want, err := io.ReadAll(rc)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("reader returned %d bytes, differing from GetReconstructedFile's %d", len(got), len(want))
				}
				if int64(len(got)) != length {
					t.Fatalf("reader returned %d bytes, expected length %d", len(got), length)
				}
			})
		}
	}
}
