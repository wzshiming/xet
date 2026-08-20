package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

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
	for start := range numChunks {
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

// putToPresignedURL PUTs raw bytes to a presigned URL like a redirected
// client would: plain HTTP, no SDK, no extra headers.
func putToPresignedURL(t *testing.T, url string, data []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("direct PUT status %s: %s", resp.Status, body)
	}
}

func TestS3StorageDirectXorbUploadAndValidate(t *testing.T) {
	ctx := context.Background()
	ss := newTestS3Storage(t)

	encoded, xorbHash := encodeTestXorb(t, true, []byte("direct upload chunk"))

	uploadURL, err := ss.XorbUploadURL(ctx, "default", xorbHash, int64(len(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	if uploadURL == "" {
		t.Fatal("XorbUploadURL returned no URL with presign enabled")
	}
	// Plain PUT clients (and gofakes3/MinIO) cannot supply SDK checksum
	// headers, so none may be folded into the signature.
	if strings.Contains(strings.ToLower(uploadURL), "checksum") {
		t.Fatalf("presigned PUT URL requires checksum headers: %s", uploadURL)
	}

	if err := ss.ValidateXorb(ctx, "default", xorbHash); !errors.Is(err, ErrXorbNotFound) {
		t.Fatalf("ValidateXorb before upload = %v, want ErrXorbNotFound", err)
	}

	putToPresignedURL(t, uploadURL, encoded)

	if ok, err := ss.HasXorb(ctx, "default", xorbHash); err != nil || !ok {
		t.Fatalf("HasXorb after direct PUT = %v, %v", ok, err)
	}
	if err := ss.ValidateXorb(ctx, "default", xorbHash); err != nil {
		t.Fatalf("ValidateXorb after direct PUT: %v", err)
	}

	// Corrupt bytes are only caught at validation time.
	bad := append([]byte(nil), encoded...)
	bad[10] ^= 0xff
	badHash := xet.XorbHash{42}
	badURL, err := ss.XorbUploadURL(ctx, "default", badHash, int64(len(bad)))
	if err != nil {
		t.Fatal(err)
	}
	putToPresignedURL(t, badURL, bad)
	if err := ss.ValidateXorb(ctx, "default", badHash); !errors.Is(err, ErrXorbInvalid) {
		t.Fatalf("ValidateXorb of corrupt xorb = %v, want ErrXorbInvalid", err)
	}
	// Invalid bytes are deleted so the key is not poisoned: the next upload
	// attempt sees the xorb as absent instead of skipping it as stored.
	if ok, err := ss.HasXorb(ctx, "default", badHash); err != nil || ok {
		t.Fatalf("HasXorb after failed validation = %v, %v; want false", ok, err)
	}

	// Validated hashes are cached, so a shard retry does not re-read the
	// object. gofakes3 ignores the signed If-None-Match condition, so this
	// replay overwrite succeeds here; real S3 rejects it with 412.
	putToPresignedURL(t, uploadURL, bad)
	if err := ss.ValidateXorb(ctx, "default", xorbHash); err != nil {
		t.Fatalf("ValidateXorb after successful validation = %v, want nil (cached)", err)
	}
}

func TestS3StoragePutXorbMarksValidated(t *testing.T) {
	ctx := context.Background()
	ss := newTestS3Storage(t)

	encoded, xorbHash := encodeTestXorb(t, true, []byte("validated by put"))
	if inserted, err := ss.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded)); err != nil || !inserted {
		t.Fatalf("PutXorb() = %v, %v", inserted, err)
	}

	// PutXorb already validated the stream; ValidateXorb must not re-read,
	// so corrupting the object behind the storage's back goes unnoticed.
	if err := ss.putObject(ctx, ss.objectKey("xorbs", xorbHash.String()), bytes.NewReader([]byte("garbage"))); err != nil {
		t.Fatal(err)
	}
	if err := ss.ValidateXorb(ctx, "default", xorbHash); err != nil {
		t.Fatalf("ValidateXorb after PutXorb = %v, want nil (cached)", err)
	}
}

func TestS3StorageXorbUploadURLDisabledWithoutPresign(t *testing.T) {
	ss := newTestS3Storage(t, WithS3Presign(false))
	url, err := ss.XorbUploadURL(context.Background(), "default", xet.XorbHash{1}, 1)
	if err != nil || url != "" {
		t.Fatalf("XorbUploadURL with presign off = %q, %v; want empty", url, err)
	}
}

// With presigning disabled nothing reaches the store unvalidated, so
// shard-time verification degrades to an existence check instead of a full
// object read.
func TestS3StorageValidateXorbWithoutPresignChecksExistenceOnly(t *testing.T) {
	ctx := context.Background()
	ss := newTestS3Storage(t, WithS3Presign(false))

	if err := ss.ValidateXorb(ctx, "default", xet.XorbHash{1}); !errors.Is(err, ErrXorbNotFound) {
		t.Fatalf("ValidateXorb missing = %v, want ErrXorbNotFound", err)
	}

	// Bytes that would fail content validation pass: they can only have been
	// stored through PutXorb, which validated them on ingress.
	xorbHash := xet.XorbHash{2}
	if err := ss.putObject(ctx, ss.objectKey("xorbs", xorbHash.String()), bytes.NewReader([]byte("garbage"))); err != nil {
		t.Fatal(err)
	}
	if err := ss.ValidateXorb(ctx, "default", xorbHash); err != nil {
		t.Fatalf("ValidateXorb = %v, want nil (existence only)", err)
	}
}

// deleteObjectIfMatch must fall back to an unconditional delete when the
// store rejects the conditional request (leaving the object would poison the
// key), but must respect a failed condition: 412 means the object changed.
func TestS3StorageDeleteObjectIfMatchFallback(t *testing.T) {
	for _, test := range []struct {
		name        string
		condStatus  int
		wantIfMatch []string
	}{
		{name: "unsupported falls back", condStatus: http.StatusNotImplemented, wantIfMatch: []string{`"etag"`, ""}},
		{name: "changed object is kept", condStatus: http.StatusPreconditionFailed, wantIfMatch: []string{`"etag"`}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var mu sync.Mutex
			var gotIfMatch []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodDelete {
					t.Errorf("unexpected %s request", r.Method)
					http.Error(w, "unexpected method", http.StatusBadRequest)
					return
				}
				mu.Lock()
				gotIfMatch = append(gotIfMatch, r.Header.Get("If-Match"))
				mu.Unlock()
				if r.Header.Get("If-Match") != "" {
					w.WriteHeader(test.condStatus)
					return
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			t.Cleanup(srv.Close)

			client := s3.New(s3.Options{
				BaseEndpoint:               aws.String(srv.URL),
				Region:                     "us-east-1",
				Credentials:                credentials.NewStaticCredentialsProvider("test", "test", ""),
				UsePathStyle:               true,
				RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
				ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
			})
			ss, err := NewS3Storage(context.Background(), WithS3Client(client), WithS3Bucket("test-bucket"))
			if err != nil {
				t.Fatal(err)
			}

			ss.deleteObjectIfMatch(context.Background(), "xorbs/ab/cd", `"etag"`)

			mu.Lock()
			defer mu.Unlock()
			if len(gotIfMatch) != len(test.wantIfMatch) {
				t.Fatalf("DELETE If-Match sequence = %q, want %q", gotIfMatch, test.wantIfMatch)
			}
			for i, want := range test.wantIfMatch {
				if gotIfMatch[i] != want {
					t.Fatalf("DELETE If-Match sequence = %q, want %q", gotIfMatch, test.wantIfMatch)
				}
			}
		})
	}
}

// A transfer that dies mid-stream must not be reported as invalid content:
// invalid is terminal for the shard upload while transport errors retry.
func TestS3StorageValidateXorbTransientReadErrorIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Promise more bytes than are sent, then close the connection.
		w.Header().Set("Content-Length", "4096")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte{0})
	}))
	t.Cleanup(srv.Close)

	client := s3.New(s3.Options{
		BaseEndpoint:               aws.String(srv.URL),
		Region:                     "us-east-1",
		Credentials:                credentials.NewStaticCredentialsProvider("test", "test", ""),
		UsePathStyle:               true,
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	ss, err := NewS3Storage(context.Background(), WithS3Client(client), WithS3Bucket("test-bucket"))
	if err != nil {
		t.Fatal(err)
	}

	err = ss.ValidateXorb(context.Background(), "default", xet.XorbHash{1})
	if err == nil {
		t.Fatal("ValidateXorb succeeded on a truncated transfer")
	}
	if errors.Is(err, ErrXorbInvalid) || errors.Is(err, ErrXorbNotFound) {
		t.Fatalf("ValidateXorb = %v, want a retryable transport error", err)
	}
}

// putTestShard uploads one single-chunk xorb per part and returns a shard
// describing the concatenation of parts as one file.
func putTestShard(t *testing.T, ctx context.Context, ss *S3Storage, parts [][]byte) (*shard.Shard, xet.FileHash) {
	t.Helper()
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
	return shardObj, fileHash
}

// newIndexedShard returns a shard with one file and one CAS block.
func newIndexedShard(fileHash xet.FileHash) *shard.Shard {
	s := shard.NewShard()
	s.AddFile(shard.FileBlock{FileHash: fileHash})
	s.AddCASBlock(shard.CASBlock{
		CASHash: xet.XorbHash{2},
		Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: xet.ChunkHash{3}}},
	})
	return s
}

// shardObjectKey returns the single stored shard key.
func shardObjectKey(t *testing.T, ss *S3Storage) string {
	t.Helper()
	var found string
	for key := range listObjectKeys(t, ss) {
		if !strings.Contains(key, "shards/") {
			continue
		}
		if found != "" {
			t.Fatalf("more than one shard object: %q and %q", found, key)
		}
		found = key
	}
	if found == "" {
		t.Fatal("no shard object stored")
	}
	return found
}

// TestS3ShardNameIsDeterministicContentHash mirrors the FileStorage assertion:
// the object name is the SHA-256 of the stored bytes and does not vary with the
// creation time embedded in the (unstored) footer.
func TestS3ShardNameIsDeterministicContentHash(t *testing.T) {
	ctx := context.Background()

	var names []string
	for _, creationTime := range []uint64{1, 1 << 30} {
		ss := newTestS3Storage(t)
		s := newIndexedShard(xet.FileHash{1})
		s.SetFooter(time.Unix(int64(creationTime), 0))
		if inserted, err := ss.PutShard(ctx, s); err != nil || !inserted {
			t.Fatalf("PutShard() = %v, %v", inserted, err)
		}

		key := shardObjectKey(t, ss)
		data, err := ss.getObject(ctx, key)
		if err != nil {
			t.Fatalf("read stored shard: %v", err)
		}
		if footerSize := binary.LittleEndian.Uint64(data[40:48]); footerSize != 0 {
			t.Fatalf("stored FooterSize = %d, want 0", footerSize)
		}
		name := strings.ReplaceAll(strings.TrimPrefix(key, "shards/"), "/", "")
		sum := sha256.Sum256(data)
		if want := hex.EncodeToString(sum[:]); name != want {
			t.Fatalf("shard name %q != sha256 of stored bytes %q", name, want)
		}
		names = append(names, name)
	}
	if names[0] != names[1] {
		t.Fatalf("identical content produced different names: %q vs %q", names[0], names[1])
	}
}

// TestS3FooteredShardObjectStaysReadable covers objects written before shards
// went footerless.
func TestS3FooteredShardObjectStaysReadable(t *testing.T) {
	ctx := context.Background()
	ss := newTestS3Storage(t)

	fileHash := xet.FileHash{7}
	const creationTime = 1700000000
	data, name := legacyShardBytes(t, newIndexedShard(fileHash), creationTime)

	if err := ss.putObject(ctx, ss.objectKey("shards", name), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if err := ss.putIndexObject(ctx, ss.objectKey("index/files", fileHash.String()), []byte(name)); err != nil {
		t.Fatal(err)
	}

	loaded, err := ss.GetShard(ctx, fileHash)
	if err != nil {
		t.Fatalf("GetShard on a footered object: %v", err)
	}
	if loaded.Files[0].FileHash != fileHash {
		t.Fatal("loaded the wrong shard")
	}
	if loaded.Footer == nil || loaded.Footer.ShardCreationTimestamp != creationTime {
		t.Fatalf("stored footer was not preserved: %+v", loaded.Footer)
	}
}

func TestS3StorageShardRoundTrip(t *testing.T) {
	ctx := context.Background()
	ss := newTestS3Storage(t, WithS3Prefix("some/prefix"))

	parts := [][]byte{[]byte("first part "), []byte("and the second part")}
	fileData := bytes.Join(parts, nil)
	shardObj, fileHash := putTestShard(t, ctx, ss, parts)

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

	byChunk, err := fresh.GetShardByChunkHash(ctx, "default", xet.ComputeChunkHash(parts[0]))
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

	u, err := ss.GetXorbURL("default", xorbHash)
	if err != nil {
		t.Fatalf("GetXorbURL() error: %v", err)
	}
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
	if got, err := plain.GetXorbURL("ns", xorbHash); err != nil || got != "/v1/xorbs/ns/"+xorbHash.String() {
		t.Fatalf("GetXorbURL() with presign disabled = %q, %v", got, err)
	}

	// A distinct presign endpoint moves only the URL host, not the API client.
	public := newTestS3Storage(t, WithS3PresignEndpoint("http://public.example:9000"))
	got, err := public.GetXorbURL("default", xorbHash)
	if err != nil {
		t.Fatalf("GetXorbURL() with presign endpoint error: %v", err)
	}
	if !strings.HasPrefix(got, "http://public.example:9000/") || !strings.Contains(got, "X-Amz-Signature") {
		t.Fatalf("GetXorbURL() with presign endpoint = %q, want presigned URL at public.example", got)
	}
}

// flakyHTTPClient fails requests matching the configured predicate and
// forwards everything else.
type flakyHTTPClient struct {
	mu   sync.Mutex
	fail func(*http.Request) bool
}

func (fc *flakyHTTPClient) setFail(fail func(*http.Request) bool) {
	fc.mu.Lock()
	fc.fail = fail
	fc.mu.Unlock()
}

func (fc *flakyHTTPClient) Do(req *http.Request) (*http.Response, error) {
	fc.mu.Lock()
	fail := fc.fail
	fc.mu.Unlock()
	if fail != nil && fail(req) {
		return nil, fmt.Errorf("injected failure: %s %s", req.Method, req.URL.Path)
	}
	return http.DefaultClient.Do(req)
}

// TestS3StoragePutShardRetryAfterPartialFailure kills PutShard midway through
// its index writes and verifies a retry fully repairs the shard instead of
// reporting "already exists" with indexes missing.
func TestS3StoragePutShardRetryAfterPartialFailure(t *testing.T) {
	ctx := context.Background()
	backend := s3mem.New()
	if err := backend.CreateBucket("test-bucket"); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(srv.Close)

	fc := &flakyHTTPClient{}
	client := s3.New(s3.Options{
		BaseEndpoint:               aws.String(srv.URL),
		Region:                     "us-east-1",
		Credentials:                credentials.NewStaticCredentialsProvider("test", "test", ""),
		UsePathStyle:               true,
		HTTPClient:                 fc,
		Retryer:                    aws.NopRetryer{},
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	ss, err := NewS3Storage(ctx, WithS3Client(client), WithS3Bucket("test-bucket"))
	if err != nil {
		t.Fatal(err)
	}

	parts := [][]byte{[]byte("retry me")}
	shardObj, fileHash := putTestShard(t, ctx, ss, parts)

	// First attempt dies while writing SHA-256 indexes, after the shard
	// object and chunk indexes are already stored.
	fc.setFail(func(req *http.Request) bool {
		return req.Method == http.MethodPut && strings.Contains(req.URL.Path, "/index/sha256/")
	})
	if _, err := ss.PutShard(ctx, shardObj); err == nil {
		t.Fatal("PutShard() succeeded despite injected failure")
	}

	// The partial shard must not count as existing, in cache or in S3.
	if exists, err := ss.hasFile(ctx, fileHash); err != nil || exists {
		t.Fatalf("hasFile() after partial failure = %v, %v", exists, err)
	}

	fc.setFail(nil)
	if _, err := ss.PutShard(ctx, shardObj); err != nil {
		t.Fatalf("PutShard() retry: %v", err)
	}

	// A fresh storage must resolve every index written by the retry.
	fresh, err := NewS3Storage(ctx, WithS3Client(client), WithS3Bucket("test-bucket"))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(parts[0])
	gotFileHash, err := fresh.GetFileHashBySHA256(ctx, "default", digest)
	if err != nil {
		t.Fatalf("GetFileHashBySHA256 after retry: %v", err)
	}
	if gotFileHash != fileHash {
		t.Fatal("SHA-256 index resolved wrong file hash")
	}
	if _, err := fresh.GetShard(ctx, fileHash); err != nil {
		t.Fatalf("GetShard after retry: %v", err)
	}
	if _, err := fresh.GetShardByChunkHash(ctx, "default", xet.ComputeChunkHash(parts[0])); err != nil {
		t.Fatalf("GetShardByChunkHash after retry: %v", err)
	}
}
