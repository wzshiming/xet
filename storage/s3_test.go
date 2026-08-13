package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// newTestS3Storage returns an S3Storage backed by an in-process gofakes3.
func newTestS3Storage(t *testing.T, opts ...S3Option) *S3Storage {
	t.Helper()
	backend := s3mem.New()
	if err := backend.CreateBucket("test-bucket"); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(srv.Close)

	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(srv.URL),
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		UsePathStyle: true,
		// Keep request bodies un-chunked for the fake server.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	ss, err := NewS3Storage(context.Background(), append([]S3Option{
		WithS3Client(client),
		WithS3Bucket("test-bucket"),
	}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return ss
}

func TestS3StorageXorbRoundTrip(t *testing.T) {
	ctx := context.Background()
	ss := newTestS3Storage(t)

	chunks := [][]byte{
		bytes.Repeat([]byte{1}, 1000),
		[]byte("second chunk"),
		bytes.Repeat([]byte("abc"), 700),
	}
	encoded, xorbHash := encodeTestXorb(t, true, chunks...)

	if ok, err := ss.HasXorb(ctx, "default", xorbHash); err != nil || ok {
		t.Fatalf("HasXorb() before put = %v, %v", ok, err)
	}
	if inserted, err := ss.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded)); err != nil || !inserted {
		t.Fatalf("PutXorb() = %v, %v", inserted, err)
	}
	if inserted, err := ss.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded)); err != nil || inserted {
		t.Fatalf("PutXorb() repeat = %v, %v", inserted, err)
	}
	if ok, err := ss.HasXorb(ctx, "default", xorbHash); err != nil || !ok {
		t.Fatalf("HasXorb() after put = %v, %v", ok, err)
	}

	// Reject an invalid xorb and leave no object behind.
	bad := append([]byte(nil), encoded...)
	bad[10] ^= 0xff
	otherHash := xet.XorbHash{42}
	if _, err := ss.PutXorb(ctx, "default", otherHash, bytes.NewReader(bad)); err == nil {
		t.Fatal("PutXorb() accepted a corrupted xorb")
	}
	if ok, err := ss.HasXorb(ctx, "default", otherHash); err != nil || ok {
		t.Fatalf("HasXorb() after failed put = %v, %v", ok, err)
	}

	// Full sequential read.
	rsc, err := ss.GetXorbReadSeekCloser(ctx, "default", xorbHash)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rsc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, encoded) {
		t.Fatalf("read %d bytes, want %d", len(got), len(encoded))
	}

	// Seek from end (http.ServeContent sizing) and ranged re-read.
	size, err := rsc.Seek(0, io.SeekEnd)
	if err != nil || size != int64(len(encoded)) {
		t.Fatalf("Seek(0, End) = %d, %v", size, err)
	}
	if _, err := rsc.Seek(100, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	part := make([]byte, 50)
	if _, err := io.ReadFull(rsc, part); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(part, encoded[100:150]) {
		t.Fatal("ranged read mismatch")
	}
	if err := rsc.Close(); err != nil {
		t.Fatal(err)
	}

	// Chunk ranges must match those scanned from the raw chunk data.
	reference, _ := encodeTestXorb(t, false, chunks...)
	numChunks := uint32(len(chunks))
	for start := uint32(0); start < numChunks; start++ {
		for end := start + 1; end <= numChunks; end++ {
			wantStart, wantEnd, err := xorb.ChunkDataRange(bytes.NewReader(reference), start, end)
			if err != nil {
				t.Fatalf("ChunkDataRange(%d, %d): %v", start, end, err)
			}
			gotStart, gotEnd, err := ss.GetXorbDataRange(ctx, "default", xorbHash, start, end)
			if err != nil {
				t.Fatalf("GetXorbDataRange(%d, %d): %v", start, end, err)
			}
			if gotStart != wantStart || gotEnd != wantEnd {
				t.Fatalf("range [%d, %d) = [%d, %d], want [%d, %d]", start, end, gotStart, gotEnd, wantStart, wantEnd)
			}
		}
	}

	if _, err := ss.GetXorbReadSeekCloser(ctx, "default", xet.XorbHash{9}); err == nil {
		t.Fatal("GetXorbReadSeekCloser() found a missing xorb")
	}
}

func TestS3StorageShardRoundTrip(t *testing.T) {
	ctx := context.Background()
	ss := newTestS3Storage(t, WithS3Prefix("some/prefix"))

	parts := [][]byte{[]byte("first part "), []byte("and the second part")}
	fileData := bytes.Join(parts, nil)
	shardObj := shard.NewShard()
	fileBlock := shard.FileBlock{}
	var chunkHashes []xet.ChunkHash
	var chunkSizes []uint64
	for _, part := range parts {
		var encoded bytes.Buffer
		encoder := xorb.NewEncoder(&encoded, true)
		if _, err := encoder.Write(part); err != nil {
			t.Fatal(err)
		}
		if err := encoder.Close(); err != nil {
			t.Fatal(err)
		}
		xorbHash := encoder.SummoryHash()
		if _, err := ss.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded.Bytes())); err != nil {
			t.Fatal(err)
		}
		chunkHash := xet.ComputeChunkHash(part)
		chunkHashes = append(chunkHashes, chunkHash)
		chunkSizes = append(chunkSizes, uint64(len(part)))
		fileBlock.Entries = append(fileBlock.Entries, shard.FileDataSequenceEntry{
			CASHash: xorbHash, UnpackedSegBytes: uint32(len(part)), ChunkIndexEnd: 1,
		})
		shardObj.AddCASBlock(shard.CASBlock{
			CASHash: xorbHash,
			Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: chunkHash, UnpackedSegBytes: uint32(len(part))}},
		})
	}
	fileHash := xet.ComputeFileHash(chunkHashes, chunkSizes)
	fileBlock.FileHash = fileHash
	shardObj.AddFile(fileBlock)

	if inserted, err := ss.PutShard(ctx, shardObj); err != nil || !inserted {
		t.Fatalf("PutShard() = %v, %v", inserted, err)
	}
	if inserted, err := ss.PutShard(ctx, shardObj); err != nil || inserted {
		t.Fatalf("PutShard() repeat = %v, %v", inserted, err)
	}

	for key := range listObjectKeys(t, ss) {
		if !strings.HasPrefix(key, "some/prefix/") {
			t.Fatalf("object key %q missing prefix", key)
		}
	}

	// A fresh storage over the same bucket must resolve everything without
	// any in-memory state.
	fresh, err := NewS3Storage(ctx,
		WithS3Client(ss.client),
		WithS3Bucket(ss.bucket),
		WithS3Prefix(ss.prefix),
	)
	if err != nil {
		t.Fatal(err)
	}

	loaded, err := fresh.GetShard(ctx, fileHash)
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	if loaded.Files[0].FileHash != fileHash {
		t.Fatal("loaded wrong shard")
	}

	byChunk, err := fresh.GetShardByChunkHash(ctx, "default", chunkHashes[0])
	if err != nil {
		t.Fatalf("GetShardByChunkHash: %v", err)
	}
	if byChunk.Files[0].FileHash != fileHash {
		t.Fatal("chunk index resolved wrong shard")
	}
	if _, err := fresh.GetShardByChunkHash(ctx, "default", xet.ChunkHash{99}); err == nil {
		t.Fatal("GetShardByChunkHash() found a missing chunk")
	}

	digest := sha256.Sum256(fileData)
	gotFileHash, err := fresh.GetFileHashBySHA256(ctx, "default", digest)
	if err != nil {
		t.Fatalf("GetFileHashBySHA256: %v", err)
	}
	if gotFileHash != fileHash {
		t.Fatal("SHA-256 index resolved wrong file hash")
	}

	f, err := fresh.GetReconstructedFile(ctx, "default", digest)
	if err != nil {
		t.Fatalf("GetReconstructedFile: %v", err)
	}
	defer f.Close()
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, fileData) {
		t.Fatalf("reconstructed = %q, want %q", got, fileData)
	}

	// Range read across the entry boundary through the seek interface.
	start := len(parts[0]) - 2
	if _, err := f.Seek(int64(start), io.SeekStart); err != nil {
		t.Fatal(err)
	}
	window := make([]byte, 6)
	if _, err := io.ReadFull(f, window); err != nil {
		t.Fatal(err)
	}
	if want := fileData[start : start+6]; !bytes.Equal(window, want) {
		t.Fatalf("range = %q, want %q", window, want)
	}
}

func listObjectKeys(t *testing.T, ss *S3Storage) map[string]struct{} {
	t.Helper()
	out, err := ss.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(ss.bucket),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Contents) == 0 {
		t.Fatal("no objects stored")
	}
	keys := make(map[string]struct{}, len(out.Contents))
	for _, obj := range out.Contents {
		keys[aws.ToString(obj.Key)] = struct{}{}
	}
	return keys
}

func TestS3StorageGetXorbURL(t *testing.T) {
	ctx := context.Background()
	ss := newTestS3Storage(t)

	encoded, xorbHash := encodeTestXorb(t, true, []byte("presign me"))
	if _, err := ss.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded)); err != nil {
		t.Fatal(err)
	}

	u := ss.GetXorbURL("default", xorbHash)
	if !strings.HasPrefix(u, "http") || !strings.Contains(u, "X-Amz-Signature") {
		t.Fatalf("GetXorbURL() = %q, want a presigned absolute URL", u)
	}

	// Presigned URLs must serve ranged GETs with no extra auth headers,
	// matching how clients fetch reconstruction terms.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Range", "bytes=2-5")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusPartialContent)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, encoded[2:6]) {
		t.Fatalf("range body = %q, want %q", body, encoded[2:6])
	}

	// Disabled presigning falls back to the server-served xorb path.
	plain := newTestS3Storage(t, WithS3Presign(false))
	if got, want := plain.GetXorbURL("ns", xorbHash), "/v1/xorbs/ns/"+xorbHash.String(); got != want {
		t.Fatalf("GetXorbURL() with presign disabled = %q, want %q", got, want)
	}
}
