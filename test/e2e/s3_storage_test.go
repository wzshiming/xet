package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/upload"
	"github.com/wzshiming/xet/xorb"
)

// TestS3StorageUploadRestartAndDownload runs the full upload/download cycle
// against an in-process S3 double (gofakes3) through the production option
// path, with presigned transfers on and off. Both storage instances share one
// backend, so the restart below proves nothing is served from memory.
func TestS3StorageUploadRestartAndDownload(t *testing.T) {
	for _, presign := range []bool{true, false} {
		t.Run(fmt.Sprintf("presign=%t", presign), func(t *testing.T) {
			testS3StorageUploadRestartAndDownload(t, presign)
		})
	}
}

func testS3StorageUploadRestartAndDownload(t *testing.T, presign bool) {
	ctx := context.Background()

	const bucket = "xet-e2e"
	backend := s3mem.New()
	if err := backend.CreateBucket(bucket); err != nil {
		t.Fatal(err)
	}
	s3Server := httptest.NewServer(gofakes3.New(backend).Server())
	defer s3Server.Close()
	endpoint := s3Server.URL

	// NewS3Storage resolves credentials and checksum behavior from the AWS
	// env chain; gofakes3 cannot parse the aws-chunked bodies the SDK sends
	// when default request checksums are on.
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
	t.Setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

	// Constructed through the production option path, not an injected client.
	newStorage := func() storage.Storage {
		stor, err := storage.NewS3Storage(ctx,
			storage.WithS3Bucket(bucket),
			storage.WithS3Prefix("xet-data"),
			storage.WithS3Endpoint(endpoint),
			storage.WithS3PathStyle(true),
			storage.WithS3Presign(presign),
		)
		if err != nil {
			t.Fatal(err)
		}
		return stor
	}

	uploadServer := httptest.NewServer(server.NewHandler(server.WithStorage(newStorage())))
	uploadClient, err := client.NewClient(
		client.WithBaseURL(uploadServer.URL),
		client.WithCacheDir(t.TempDir()),
	)
	if err != nil {
		t.Fatal(err)
	}

	if !presign {
		// With presigning off there is no direct-upload URL: an opt-in xorb
		// POST must be served by the streaming path, not redirected.
		var encoded bytes.Buffer
		enc := xorb.NewEncoder(&encoded, true)
		if _, err := enc.Write([]byte("streamed through the server")); err != nil {
			t.Fatal(err)
		}
		if err := enc.Close(); err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequest(http.MethodPost, uploadServer.URL+"/v1/xorbs/default/"+enc.SummoryHash().String(), bytes.NewReader(encoded.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set(upload.HeaderDirectUpload, upload.DirectUploadAccept)
		noRedirect := &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
		resp, err := noRedirect.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("opt-in xorb POST with presign off = %d, want 200 (streaming)", resp.StatusCode)
		}
		var uploadResp struct {
			WasInserted bool `json:"was_inserted"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
			t.Fatal(err)
		}
		if !uploadResp.WasInserted {
			t.Fatal("was_inserted = false, want true for streamed upload")
		}
	}

	files := []struct {
		name string
		data []byte
		hash xet.FileHash
	}{
		{name: "small", data: []byte("Hello, S3!")},
		{name: "multi-chunk", data: deterministicData(3*128*1024 + 7919)},
		{name: "large", data: deterministicData(4 * 1024 * 1024)},
	}
	for i := range files {
		hash, err := uploadClient.UploadFile(ctx, bytes.NewReader(files[i].data))
		if err != nil {
			t.Fatalf("upload %s: %v", files[i].name, err)
		}
		files[i].hash = hash
	}
	uploadServer.Close()

	// Fresh storage over the same bucket: nothing may be served from memory.
	downloadServer := httptest.NewServer(server.NewHandler(server.WithStorage(newStorage())))
	defer downloadServer.Close()
	downloadClient, err := client.NewClient(
		client.WithBaseURL(downloadServer.URL),
		client.WithCacheDir(t.TempDir()),
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, file := range files {
		t.Run(file.name, func(t *testing.T) {
			// Reconstruction hands out presigned S3 URLs with presigning on,
			// and CAS-server routes (resolved against the request host) with
			// presigning off.
			var reconstruction struct {
				FetchInfo map[string][]struct {
					URL string `json:"url"`
				} `json:"fetch_info"`
			}
			resp := doRequest(t, http.MethodGet, downloadServer.URL+"/v1/reconstructions/"+file.hash.String(), nil)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("reconstruction status = %d", resp.StatusCode)
			}
			if err := json.NewDecoder(resp.Body).Decode(&reconstruction); err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if len(reconstruction.FetchInfo) == 0 {
				t.Fatal("reconstruction has no fetch info")
			}
			for _, entries := range reconstruction.FetchInfo {
				for _, entry := range entries {
					if presign {
						if !strings.HasPrefix(entry.URL, endpoint+"/") || !strings.Contains(entry.URL, "X-Amz-Signature") {
							t.Fatalf("fetch URL = %q, want presigned URL at %s", entry.URL, endpoint)
						}
					} else if !strings.HasPrefix(entry.URL, downloadServer.URL+"/v1/xorbs/") {
						t.Fatalf("fetch URL = %q, want CAS xorb route at %s", entry.URL, downloadServer.URL)
					}
				}
			}

			// Full xet download path: reconstruction plus ranged xorb terms.
			out, err := os.Create(filepath.Join(t.TempDir(), "out"))
			if err != nil {
				t.Fatal(err)
			}
			defer out.Close()
			if err := downloadClient.DownloadFile(ctx, file.hash, out); err != nil {
				t.Fatalf("download: %v", err)
			}
			got, err := os.ReadFile(out.Name())
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, file.data) {
				t.Fatalf("downloaded %d bytes, want %d", len(got), len(file.data))
			}

			// Bridge path: complete file and a byte range.
			digest := sha256.Sum256(file.data)
			url := downloadServer.URL + "/xet-bridge/" + hex.EncodeToString(digest[:])
			resp = doRequest(t, http.MethodGet, url, nil)
			assertResponse(t, resp, http.StatusOK, file.data)

			if len(file.data) > 1024 {
				start, end := 100, 1023
				headers := http.Header{"Range": []string{fmt.Sprintf("bytes=%d-%d", start, end)}}
				resp = doRequest(t, http.MethodGet, url, headers)
				assertResponse(t, resp, http.StatusPartialContent, file.data[start:end+1])
			}
		})
	}
}

// TestS3DirectXorbUploadAndShardTimeValidation drives the direct-upload
// contract over raw HTTP: the xorb POST redirects to a presigned S3 PUT URL
// without consuming the body, the shard is rejected while the xorb is missing
// or corrupt, and accepted once valid bytes are in the object store.
func TestS3DirectXorbUploadAndShardTimeValidation(t *testing.T) {
	ctx := context.Background()

	const bucket = "xet-direct"
	backend := s3mem.New()
	if err := backend.CreateBucket(bucket); err != nil {
		t.Fatal(err)
	}
	s3Server := httptest.NewServer(gofakes3.New(backend).Server())
	defer s3Server.Close()

	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
	t.Setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

	stor, err := storage.NewS3Storage(ctx,
		storage.WithS3Bucket(bucket),
		storage.WithS3Endpoint(s3Server.URL),
		storage.WithS3PathStyle(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	casServer := httptest.NewServer(server.NewHandler(server.WithStorage(stor)))
	defer casServer.Close()

	// One single-chunk xorb and a shard referencing it.
	data := []byte("direct upload payload")
	var encoded bytes.Buffer
	encoder := xorb.NewEncoder(&encoded, true)
	if _, err := encoder.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	xorbHash := encoder.SummoryHash()
	chunkHash := xet.ComputeChunkHash(data)
	shardObj := shard.NewShard()
	shardObj.AddCASBlock(shard.CASBlock{
		CASHash:       xorbHash,
		NumBytesInCAS: uint32(len(data)),
		Chunks:        []shard.CASChunkSequenceEntry{{ChunkHash: chunkHash, UnpackedSegBytes: uint32(len(data))}},
	})
	shardObj.AddFile(shard.FileBlock{
		FileHash: xet.ComputeFileHash([]xet.ChunkHash{chunkHash}, []uint64{uint64(len(data))}),
		Entries:  []shard.FileDataSequenceEntry{{CASHash: xorbHash, UnpackedSegBytes: uint32(len(data)), ChunkIndexEnd: 1}},
	})
	shardReader, err := shardObj.Encode(false)
	if err != nil {
		t.Fatal(err)
	}
	shardBody, err := io.ReadAll(shardReader)
	if err != nil {
		t.Fatal(err)
	}

	noRedirect := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	postShard := func() *http.Response {
		t.Helper()
		resp, err := http.Post(casServer.URL+"/v1/shards", "application/octet-stream", bytes.NewReader(shardBody))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}
	putDirect := func(uploadURL string, payload []byte) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPut, uploadURL, bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		// Mirror the Go client: the presigned URL is signed as create-only.
		req.Header.Set("If-None-Match", "*")
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
	postXorb := func() *http.Response {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, casServer.URL+"/v1/xorbs/default/"+xorbHash.String(), bytes.NewReader(encoded.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set(upload.HeaderDirectUpload, upload.DirectUploadAccept)
		resp, err := noRedirect.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	// A shard referencing a never-uploaded xorb is rejected.
	resp := postShard()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), "referenced xorb not uploaded") {
		t.Fatalf("shard status before xorb upload = %d (%s), want 400 not-uploaded", resp.StatusCode, body)
	}

	// The xorb POST answers 307 with a presigned PUT URL at the object store.
	resp = postXorb()
	resp.Body.Close()
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("xorb POST status = %d, want 307", resp.StatusCode)
	}
	uploadURL := resp.Header.Get("Location")
	if !strings.HasPrefix(uploadURL, s3Server.URL+"/") || !strings.Contains(uploadURL, "X-Amz-Signature") {
		t.Fatalf("Location = %q, want presigned URL at %s", uploadURL, s3Server.URL)
	}

	// Corrupt bytes are accepted by S3 but rejected at shard time.
	bad := append([]byte(nil), encoded.Bytes()...)
	bad[10] ^= 0xff
	putDirect(uploadURL, bad)
	resp = postShard()
	body, _ = io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), "invalid xorb content") {
		t.Fatalf("shard status with corrupt xorb = %d (%s), want 400 invalid content", resp.StatusCode, body)
	}
	// The rejected bytes were deleted, so the key is not poisoned for retries.
	if ok, err := stor.HasXorb(ctx, "default", xorbHash); err != nil || ok {
		t.Fatalf("HasXorb after rejected shard = %v, %v; want false", ok, err)
	}

	// Valid bytes make the shard acceptable.
	putDirect(uploadURL, encoded.Bytes())
	resp = postShard()
	body, _ = io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("shard status with valid xorb = %d (%s), want 200", resp.StatusCode, body)
	}

	// Re-posting the stored xorb short-circuits without a redirect.
	resp = postXorb()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("xorb POST for stored xorb = %d, want 200", resp.StatusCode)
	}
	var uploadResp struct {
		WasInserted bool `json:"was_inserted"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		t.Fatal(err)
	}
	if uploadResp.WasInserted {
		t.Fatal("was_inserted = true for already stored xorb, want false")
	}
}
