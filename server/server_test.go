package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/upload"
	"github.com/wzshiming/xet/xorb"
)

func TestXetBridgeExtractsCompleteFileBySHA256(t *testing.T) {
	ctx := context.Background()
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}

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
		if _, err := stor.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded.Bytes())); err != nil {
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
	fileBlock.FileHash = xet.ComputeFileHash(chunkHashes, chunkSizes)
	shardObj.AddFile(fileBlock)
	if _, err := stor.PutShard(ctx, shardObj); err != nil {
		t.Fatal(err)
	}

	digest := sha256.Sum256(fileData)
	handler := NewHandler(WithStorage(stor))
	req := httptest.NewRequest(http.MethodGet, "/xet-bridge/"+hex.EncodeToString(digest[:]), nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	resp := rec.Result()
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %q", resp.StatusCode, got)
	}
	if !bytes.Equal(got, fileData) {
		t.Fatalf("body = %q, want %q", got, fileData)
	}
	if resp.ContentLength != int64(len(fileData)) {
		t.Fatalf("Content-Length = %d, want %d", resp.ContentLength, len(fileData))
	}

	t.Run("HEAD", func(t *testing.T) {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodHead, req.URL.Path, nil))
		resp := rec.Result()
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
		}
		if len(body) != 0 {
			t.Fatalf("body = %q, want empty", body)
		}
		if resp.ContentLength != int64(len(fileData)) {
			t.Fatalf("Content-Length = %d, want %d", resp.ContentLength, len(fileData))
		}
	})

	t.Run("range crossing reconstruction entries", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet, req.URL.Path, nil)
		start, end := len(parts[0])-2, len(parts[0])+3
		request.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, request)
		resp := rec.Result()
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusPartialContent {
			t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusPartialContent)
		}
		if want := fileData[start : end+1]; !bytes.Equal(body, want) {
			t.Fatalf("body = %q, want %q", body, want)
		}
		if got, want := resp.Header.Get("Content-Range"), fmt.Sprintf("bytes %d-%d/%d", start, end, len(fileData)); got != want {
			t.Fatalf("Content-Range = %q, want %q", got, want)
		}
	})
}

func TestXetBridgeRejectsInvalidAndUnknownSHA256(t *testing.T) {
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(stor))
	for _, test := range []struct {
		path string
		want int
	}{
		{path: "/xet-bridge/not-a-digest", want: http.StatusBadRequest},
		{path: "/xet-bridge/" + string(bytes.Repeat([]byte{'0'}, 64)), want: http.StatusNotFound},
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, test.path, nil))
		if rec.Code != test.want {
			t.Errorf("GET %s: status = %d, want %d", test.path, rec.Code, test.want)
		}
	}
}

// encodeXorbForTest returns the encoded xorb bytes and hash for one chunk.
func encodeXorbForTest(t *testing.T, chunk []byte) ([]byte, xet.XorbHash) {
	t.Helper()
	var encoded bytes.Buffer
	encoder := xorb.NewEncoder(&encoded, true)
	if _, err := encoder.Write(chunk); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes(), encoder.SummoryHash()
}

// buildSingleFileShard stores one single-chunk xorb per part in stor and
// returns a shard describing the concatenation of parts as one file.
func buildSingleFileShard(t *testing.T, ctx context.Context, stor storage.Storage, parts ...[]byte) *shard.Shard {
	t.Helper()
	shardObj := shard.NewShard()
	fileBlock := shard.FileBlock{}
	var chunkHashes []xet.ChunkHash
	var chunkSizes []uint64
	for _, part := range parts {
		encoded, xorbHash := encodeXorbForTest(t, part)
		if _, err := stor.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded)); err != nil {
			t.Fatal(err)
		}
		chunkHash := xet.ComputeChunkHash(part)
		chunkHashes = append(chunkHashes, chunkHash)
		chunkSizes = append(chunkSizes, uint64(len(part)))
		fileBlock.Entries = append(fileBlock.Entries, shard.FileDataSequenceEntry{
			CASHash: xorbHash, UnpackedSegBytes: uint32(len(part)), ChunkIndexEnd: 1,
		})
		shardObj.AddCASBlock(shard.CASBlock{
			CASHash:       xorbHash,
			NumBytesInCAS: uint32(len(part)),
			Chunks:        []shard.CASChunkSequenceEntry{{ChunkHash: chunkHash, UnpackedSegBytes: uint32(len(part))}},
		})
	}
	fileBlock.FileHash = xet.ComputeFileHash(chunkHashes, chunkSizes)
	shardObj.AddFile(fileBlock)
	return shardObj
}

func encodeShardForTest(t *testing.T, shardObj *shard.Shard) []byte {
	t.Helper()
	r, err := shardObj.Encode(false)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// directUploadStorage exposes a fixed direct-upload URL over a real storage.
type directUploadStorage struct {
	storage.Storage
	uploadURL string
}

func (d *directUploadStorage) XorbUploadURL(context.Context, string, xet.XorbHash, int64) (string, error) {
	return d.uploadURL, nil
}

// readTracker flags whether the request body was ever read.
type readTracker struct{ read bool }

func (r *readTracker) Read([]byte) (int, error) {
	r.read = true
	return 0, io.EOF
}

func TestUploadXorbRedirectsToDirectUploadURL(t *testing.T) {
	ctx := context.Background()
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	const uploadURL = "https://s3.example/xorbs/ab/cd?X-Amz-Signature=sig"
	handler := NewHandler(WithStorage(&directUploadStorage{Storage: stor, uploadURL: uploadURL}))

	encoded, xorbHash := encodeXorbForTest(t, []byte("redirected xorb"))

	body := &readTracker{}
	req := httptest.NewRequest(http.MethodPost, "/v1/xorbs/default/"+xorbHash.String(), body)
	req.ContentLength = int64(len(encoded))
	req.Header.Set(upload.HeaderDirectUpload, upload.DirectUploadAccept)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d (%s), want 307", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Location"); got != uploadURL {
		t.Fatalf("Location = %q, want %q", got, uploadURL)
	}
	if body.read {
		t.Fatal("request body was read before redirecting")
	}

	// An already stored xorb short-circuits to 200 was_inserted=false.
	if _, err := stor.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded)); err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest(http.MethodPost, "/v1/xorbs/default/"+xorbHash.String(), bytes.NewReader(encoded))
	req.Header.Set(upload.HeaderDirectUpload, upload.DirectUploadAccept)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status for existing xorb = %d, want 200", rec.Code)
	}
	var resp struct {
		WasInserted bool `json:"was_inserted"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.WasInserted {
		t.Fatal("was_inserted = true for existing xorb, want false")
	}
}

func TestUploadXorbFallsBackToPutWhenDirectUploadUnavailable(t *testing.T) {
	ctx := context.Background()
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	// XorbUploadURL returns "", e.g. S3 storage with presigning disabled.
	handler := NewHandler(WithStorage(&directUploadStorage{Storage: stor}))

	encoded, xorbHash := encodeXorbForTest(t, []byte("through the server"))
	req := httptest.NewRequest(http.MethodPost, "/v1/xorbs/default/"+xorbHash.String(), bytes.NewReader(encoded))
	req.Header.Set(upload.HeaderDirectUpload, upload.DirectUploadAccept)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (%s), want 200", rec.Code, rec.Body.String())
	}
	var resp struct {
		WasInserted bool `json:"was_inserted"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if !resp.WasInserted {
		t.Fatal("was_inserted = false, want true")
	}
	if ok, err := stor.HasXorb(ctx, "default", xorbHash); err != nil || !ok {
		t.Fatalf("HasXorb after fallback upload = %v, %v", ok, err)
	}
}

// Clients that do not advertise redirect support (e.g. xet-core) must be
// served by the streaming path even when the storage offers direct upload.
func TestUploadXorbWithoutOptInStreamsThroughServer(t *testing.T) {
	ctx := context.Background()
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(&directUploadStorage{Storage: stor, uploadURL: "https://s3.example/xorbs/ab/cd?X-Amz-Signature=sig"}))

	encoded, xorbHash := encodeXorbForTest(t, []byte("no opt-in"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/xorbs/default/"+xorbHash.String(), bytes.NewReader(encoded)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (%s), want 200", rec.Code, rec.Body.String())
	}
	if ok, err := stor.HasXorb(ctx, "default", xorbHash); err != nil || !ok {
		t.Fatalf("HasXorb after streaming upload = %v, %v", ok, err)
	}
}

// A shard may reference a xorb only from file entries (data uploaded
// earlier, so no CAS block in this shard); those references must be
// verified too, or direct uploads would commit unvalidated.
func TestUploadShardVerifiesFileEntryReferencedXorbs(t *testing.T) {
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	shardObj := shard.NewShard()
	shardObj.AddFile(shard.FileBlock{
		FileHash: xet.FileHash{1},
		Entries:  []shard.FileDataSequenceEntry{{CASHash: xet.XorbHash{2}, UnpackedSegBytes: 4, ChunkIndexEnd: 1}},
	})
	body := encodeShardForTest(t, shardObj)
	handler := NewHandler(WithStorage(&validateErrStorage{Storage: stor, validateErr: storage.ErrXorbNotFound}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/shards", bytes.NewReader(body)))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "referenced xorb not uploaded") {
		t.Fatalf("v1 status = %d (%s), want 400 not-uploaded", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v2/shards", bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("v2 status = %d, want 200", rec.Code)
	}
	events := bufio.NewReader(rec.Body)
	for {
		event := readShardUploadWireEvent(t, events)
		if event.Type == "result" {
			t.Fatal("shard with unverified file-entry reference was accepted")
		}
		if event.Type == "error" {
			if !strings.Contains(event.Message, "referenced xorb not uploaded") {
				t.Fatalf("error message = %q, want not-uploaded", event.Message)
			}
			return
		}
	}
}

// validateErrStorage overrides shard-time xorb validation with a fixed error.
type validateErrStorage struct {
	storage.Storage
	validateErr error
}

func (v *validateErrStorage) ValidateXorb(context.Context, string, xet.XorbHash) error {
	return v.validateErr
}

func TestUploadShardVerifiesXorbsAtShardTime(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name        string
		validateErr error
		wantStatus  int
		wantBody    string
	}{
		{name: "valid", wantStatus: http.StatusOK},
		{name: "missing", validateErr: storage.ErrXorbNotFound, wantStatus: http.StatusBadRequest, wantBody: "referenced xorb not uploaded"},
		{name: "corrupt", validateErr: fmt.Errorf("%w: chunk 0 hash mismatch", storage.ErrXorbInvalid), wantStatus: http.StatusBadRequest, wantBody: "invalid xorb content"},
		{name: "transient", validateErr: errors.New("connection reset"), wantStatus: http.StatusInternalServerError, wantBody: "failed to verify"},
	} {
		t.Run(test.name, func(t *testing.T) {
			stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			body := encodeShardForTest(t, buildSingleFileShard(t, ctx, stor, []byte("shard time validation")))
			handler := NewHandler(WithStorage(&validateErrStorage{Storage: stor, validateErr: test.validateErr}))

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/shards", bytes.NewReader(body)))
			if rec.Code != test.wantStatus {
				t.Fatalf("status = %d (%s), want %d", rec.Code, rec.Body.String(), test.wantStatus)
			}
			if test.wantBody != "" && !strings.Contains(rec.Body.String(), test.wantBody) {
				t.Fatalf("body = %q, want containing %q", rec.Body.String(), test.wantBody)
			}
		})
	}
}

func TestUploadShardV2ReportsXorbValidationFailure(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name          string
		validateErr   error
		wantMessage   string
		wantRetryable bool
	}{
		{name: "missing", validateErr: storage.ErrXorbNotFound, wantMessage: "referenced xorb not uploaded"},
		{name: "corrupt", validateErr: fmt.Errorf("%w: chunk 0 hash mismatch", storage.ErrXorbInvalid), wantMessage: "invalid xorb content"},
		{name: "transient", validateErr: errors.New("connection reset"), wantMessage: "failed to verify referenced xorb", wantRetryable: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			body := encodeShardForTest(t, buildSingleFileShard(t, ctx, stor, []byte("v2 validation")))
			handler := NewHandler(WithStorage(&validateErrStorage{Storage: stor, validateErr: test.validateErr}))

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v2/shards", bytes.NewReader(body)))
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rec.Code)
			}

			events := bufio.NewReader(rec.Body)
			for {
				event := readShardUploadWireEvent(t, events)
				if event.Type == "result" {
					t.Fatal("shard was accepted despite failing xorb validation")
				}
				if event.Type != "error" {
					continue
				}
				if !strings.Contains(event.Message, test.wantMessage) {
					t.Fatalf("error message = %q, want containing %q", event.Message, test.wantMessage)
				}
				if event.Retryable != test.wantRetryable {
					t.Fatalf("retryable = %v, want %v", event.Retryable, test.wantRetryable)
				}
				return
			}
		})
	}
}

func TestUploadShardV2FullyValidatesXorbs(t *testing.T) {
	ctx := context.Background()
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	shardObj := buildSingleFileShard(t, ctx, stor, []byte("part one "), []byte("and part two"))
	body := encodeShardForTest(t, shardObj)
	handler := NewHandler(WithStorage(stor))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v2/shards", bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	events := bufio.NewReader(rec.Body)
	var lastValidating shardUploadWireEvent
	for {
		event := readShardUploadWireEvent(t, events)
		if event.Type == "validating" {
			lastValidating = event
			continue
		}
		if event.Type == "error" {
			t.Fatalf("error frame: %+v", event)
		}
		if event.Type == "result" {
			break
		}
	}
	if lastValidating.Verified != 2 || lastValidating.Total != 2 {
		t.Fatalf("last validating frame = %+v, want verified=2 total=2", lastValidating)
	}
	if _, err := stor.GetShard(ctx, shardObj.Files[0].FileHash); err != nil {
		t.Fatalf("shard not stored: %v", err)
	}
}
