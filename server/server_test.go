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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/xorb"
)

func authorizerFixture(t *testing.T) (storage.Storage, xet.FileHash, xet.XorbHash, xet.ChunkHash, [32]byte, []byte, []byte) {
	t.Helper()
	ctx := context.Background()
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("a real file for authorization")
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
	fileHash := xet.ComputeFileHash([]xet.ChunkHash{chunkHash}, []uint64{uint64(len(data))})
	digest := sha256.Sum256(data)
	if _, err := stor.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded.Bytes())); err != nil {
		t.Fatal(err)
	}
	shardObj := shard.NewShard()
	shardObj.AddCASBlock(shard.CASBlock{
		CASHash:       xorbHash,
		NumBytesInCAS: uint32(len(data)),
		Chunks:        []shard.CASChunkSequenceEntry{{ChunkHash: chunkHash, UnpackedSegBytes: uint32(len(data))}},
	})
	shardObj.AddFile(shard.FileBlock{
		FileHash:    fileHash,
		Flags:       shard.FileWithMetadataExt,
		MetadataExt: &shard.FileMetadataExt{SHA256Hash: shard.SHA256Hash(digest)},
		Entries:     []shard.FileDataSequenceEntry{{CASHash: xorbHash, UnpackedSegBytes: uint32(len(data)), ChunkIndexEnd: 1}},
	})
	reader, err := shardObj.Encode(false)
	if err != nil {
		t.Fatal(err)
	}
	shardBytes, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stor.PutShard(ctx, shardObj); err != nil {
		t.Fatal(err)
	}
	return stor, fileHash, xorbHash, chunkHash, digest, encoded.Bytes(), shardBytes
}

type recordingAuthorizer struct {
	calls []auth.Grant
	err   error
}

func (authorizer *recordingAuthorizer) Authorize(r *http.Request, grant auth.Grant) error {
	authorizer.calls = append(authorizer.calls, grant)
	return authorizer.err
}

func TestRoutesConsultAuthorizer(t *testing.T) {
	stor, fileHash, xorbHash, chunkHash, digest, xorbBytes, shardBytes := authorizerFixture(t)
	unknown, err := xet.ParseFileHash(strings.Repeat("ab", 32))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		method string
		path   string
		body   []byte
		want   []auth.Grant
	}{
		{"read v1", "GET", "/v1/reconstructions/" + fileHash.String(), nil, []auth.Grant{{Permission: auth.Read, File: fileHash, Targeted: true}}},
		{"read v2", "GET", "/v2/reconstructions/" + fileHash.String(), nil, []auth.Grant{{Permission: auth.Read, File: fileHash, Targeted: true}}},
		{"read batch", "GET", "/reconstructions?file_id=" + fileHash.String() + "&file_id=" + fileHash.String(), nil, []auth.Grant{{Permission: auth.Read, File: fileHash, Targeted: true}, {Permission: auth.Read, File: fileHash, Targeted: true}}},
		{"read empty batch", "GET", "/reconstructions", nil, []auth.Grant{{Permission: auth.Read}}},
		{"read unknown batch", "GET", "/reconstructions?file_id=" + unknown.String(), nil, []auth.Grant{{Permission: auth.Read, File: unknown, Targeted: true}}},
		{"has xorb", "HEAD", "/v1/xorbs/default/" + xorbHash.String(), nil, []auth.Grant{{Permission: auth.Write}}},
		{"upload xorb", "POST", "/v1/xorbs/default/" + xorbHash.String(), xorbBytes, []auth.Grant{{Permission: auth.Write}}},
		{"query chunk", "GET", "/v1/chunks/default/" + chunkHash.String(), nil, []auth.Grant{{Permission: auth.Write}}},
		{"query chunks batch", "POST", "/v1/chunks/default:query", fmt.Appendf(nil, `{"chunk_hashes":[%q,"nothex"]}`, chunkHash.String()), []auth.Grant{{Permission: auth.Write}}},
		{"write v1", "POST", "/v1/shards", shardBytes, []auth.Grant{{Permission: auth.Write}, {Permission: auth.Write, File: fileHash, SHA256: digest, Targeted: true}}},
		{"write legacy", "POST", "/shards", shardBytes, []auth.Grant{{Permission: auth.Write}, {Permission: auth.Write, File: fileHash, SHA256: digest, Targeted: true}}},
		{"write v2", "POST", "/v2/shards", shardBytes, []auth.Grant{{Permission: auth.Write}, {Permission: auth.Write, File: fileHash, SHA256: digest, Targeted: true}}},
		{"download xorb", "GET", "/v1/xorbs/default/" + xorbHash.String(), nil, nil},
		{"bridge get", "GET", "/xet-bridge/" + hex.EncodeToString(digest[:]), nil, nil},
		{"bridge head", "HEAD", "/xet-bridge/" + hex.EncodeToString(digest[:]), nil, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			authorizer := &recordingAuthorizer{}
			if len(test.want) == 0 {
				authorizer.err = auth.ErrForbidden
			}
			rec := httptest.NewRecorder()
			NewHandler(WithStorage(stor), WithAuthorizer(authorizer)).ServeHTTP(rec, httptest.NewRequest(test.method, test.path, bytes.NewReader(test.body)))
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d: %s", rec.Code, rec.Body)
			}
			if !reflect.DeepEqual(authorizer.calls, test.want) {
				t.Fatalf("authorizations = %+v, want %+v", authorizer.calls, test.want)
			}
			if test.path == "/v1/chunks/default:query" {
				var response batchChunkDedupQueryResponse
				if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
					t.Fatal(err)
				}
				want := []batchChunkDedupResult{{ChunkHash: chunkHash.String(), Found: true, XorbHash: xorbHash.String()}, {ChunkHash: "nothex"}}
				if !reflect.DeepEqual(response.Results, want) {
					t.Fatalf("results = %+v, want %+v", response.Results, want)
				}
			}
			if test.path == "/v2/shards" {
				events := bufio.NewReader(rec.Body)
				var last shardUploadWireEvent
				for _, err := events.Peek(1); err == nil; _, err = events.Peek(1) {
					last = readShardUploadWireEvent(t, events)
				}
				if last.Type != "result" {
					t.Fatalf("terminal event = %+v", last)
				}
			}
		})
	}
}

func TestBoundTokensMatchRequestTarget(t *testing.T) {
	stor, fileHash, xorbHash, _, digest, xorbBytes, shardBytes := authorizerFixture(t)
	issuer, err := auth.NewIssuer(nil, time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	readToken, _, err := issuer.Sign(auth.Grant{Permission: auth.Read, File: fileHash})
	if err != nil {
		t.Fatal(err)
	}
	other := strings.Repeat("ab", 32)
	for _, test := range []struct {
		name   string
		url    string
		status int
	}{
		{"read v1 match", "/v1/reconstructions/" + fileHash.String(), 200},
		{"read v2 match", "/v2/reconstructions/" + fileHash.String(), 200},
		{"read mismatch", "/v1/reconstructions/" + other, 403},
		{"read batch mismatch", "/reconstructions?file_id=" + fileHash.String() + "&file_id=" + other, 403},
		{"read batch match", "/reconstructions?file_id=" + fileHash.String(), 200},
		{"read empty v1", "/v1/reconstructions/" + (xet.FileHash{}).String(), 403},
		{"read empty v2", "/v2/reconstructions/" + (xet.FileHash{}).String(), 403},
		{"read empty batch", "/reconstructions?file_id=" + fileHash.String() + "&file_id=" + (xet.FileHash{}).String(), 403},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest("GET", test.url, nil)
			request.Header.Set("Authorization", "Bearer "+readToken)
			rec := httptest.NewRecorder()
			NewHandler(WithStorage(stor), WithAuthorizer(issuer)).ServeHTTP(rec, request)
			if rec.Code != test.status {
				t.Fatalf("status = %d, want %d: %s", rec.Code, test.status, rec.Body)
			}
			var response map[string]json.RawMessage
			err := json.NewDecoder(rec.Body).Decode(&response)
			if test.status == http.StatusOK && err != nil {
				t.Fatal(err)
			}
			if test.status == http.StatusForbidden && (err == nil || len(response) != 0) {
				t.Fatalf("denied reconstruction returned JSON: %+v", response)
			}
		})
	}

	writeToken, _, err := issuer.Sign(auth.Grant{Permission: auth.Write, SHA256: digest})
	if err != nil {
		t.Fatal(err)
	}
	t.Run("sha256 xorb", func(t *testing.T) {
		target, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest("POST", "/v1/xorbs/default/"+xorbHash.String(), bytes.NewReader(xorbBytes))
		request.Header.Set("Authorization", "Bearer "+writeToken)
		rec := httptest.NewRecorder()
		NewHandler(WithStorage(target), WithAuthorizer(issuer)).ServeHTTP(rec, request)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d: %s", rec.Code, rec.Body)
		}
		if exists, err := target.HasXorb(context.Background(), "default", xorbHash); err != nil || !exists {
			t.Fatalf("uploaded xorb exists = %v, err = %v", exists, err)
		}
	})

	fileToken, _, err := issuer.Sign(auth.Grant{Permission: auth.Write, File: fileHash})
	if err != nil {
		t.Fatal(err)
	}
	otherDigest := digest
	otherDigest[0] ^= 1
	for _, test := range []struct {
		name       string
		url        string
		token      string
		metadata   *shard.FileMetadataExt
		allowed    bool
		extraEmpty bool
	}{
		{"sha256 shard match", "/v1/shards", writeToken, &shard.FileMetadataExt{SHA256Hash: shard.SHA256Hash(digest)}, true, false},
		{"sha256 shard mismatch", "/v1/shards", writeToken, &shard.FileMetadataExt{SHA256Hash: shard.SHA256Hash(otherDigest)}, false, false},
		{"sha256 shard missing metadata", "/v1/shards", writeToken, nil, false, false},
		{"sha256 v2 shard mismatch", "/v2/shards", writeToken, &shard.FileMetadataExt{SHA256Hash: shard.SHA256Hash(otherDigest)}, false, false},
		{"file shard match", "/v1/shards", fileToken, &shard.FileMetadataExt{SHA256Hash: shard.SHA256Hash(digest)}, true, false},
		{"sha256 shard extra empty file", "/v1/shards", writeToken, &shard.FileMetadataExt{SHA256Hash: shard.SHA256Hash(digest)}, false, true},
		{"sha256 legacy shard extra empty file", "/shards", writeToken, &shard.FileMetadataExt{SHA256Hash: shard.SHA256Hash(digest)}, false, true},
		{"sha256 v2 shard extra empty file", "/v2/shards", writeToken, &shard.FileMetadataExt{SHA256Hash: shard.SHA256Hash(digest)}, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			target, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := target.PutXorb(context.Background(), "default", xorbHash, bytes.NewReader(xorbBytes)); err != nil {
				t.Fatal(err)
			}
			shardObj := shard.NewShard()
			if err := shardObj.Decode(bytes.NewReader(shardBytes), false); err != nil {
				t.Fatal(err)
			}
			shardObj.Files[0].MetadataExt = test.metadata
			if test.metadata == nil {
				shardObj.Files[0].Flags &^= shard.FileWithMetadataExt
			}
			if test.extraEmpty {
				shardObj.AddFile(shard.FileBlock{})
			}
			if err := shardObj.Validate(); err != nil {
				t.Fatal(err)
			}
			encoded, err := shardObj.Encode(false)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(encoded)
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest("POST", test.url, bytes.NewReader(body))
			request.Header.Set("Authorization", "Bearer "+test.token)
			rec := httptest.NewRecorder()
			NewHandler(WithStorage(target), WithAuthorizer(issuer)).ServeHTTP(rec, request)
			if test.url == "/v2/shards" {
				if rec.Code != http.StatusOK {
					t.Fatalf("status = %d: %s", rec.Code, rec.Body)
				}
				events := bufio.NewReader(rec.Body)
				var last shardUploadWireEvent
				for _, err := events.Peek(1); err == nil; _, err = events.Peek(1) {
					last = readShardUploadWireEvent(t, events)
					if last.Type == "committing" || last.Type == "result" {
						t.Fatalf("denied shard reached commit: %+v", last)
					}
				}
				if last.Type != "error" || last.Message != auth.ErrForbidden.Error() || last.Retryable {
					t.Fatalf("terminal event = %+v", last)
				}
			} else {
				want := http.StatusForbidden
				if test.allowed {
					want = http.StatusOK
				}
				if rec.Code != want {
					t.Fatalf("status = %d, want %d: %s", rec.Code, want, rec.Body)
				}
			}
			if _, err := target.GetShard(context.Background(), fileHash); (err == nil) != test.allowed {
				t.Fatalf("GetShard() error = %v, want stored = %v", err, test.allowed)
			}
			if test.extraEmpty {
				if _, err := target.GetShard(context.Background(), xet.FileHash{}); err == nil {
					t.Fatal("denied empty file was stored")
				}
			}
		})
	}
}

func TestShardUploadHashMismatchIsNotRetryable(t *testing.T) {
	_, _, xorbHash, _, _, xorbBytes, shardBytes := authorizerFixture(t)
	for _, endpoint := range []string{"/shards", "/v1/shards", "/v2/shards"} {
		for _, mismatch := range []string{"file hash mismatch", "SHA-256 mismatch"} {
			t.Run(endpoint+"/"+mismatch, func(t *testing.T) {
				ctx := context.Background()
				stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := stor.PutXorb(ctx, "default", xorbHash, bytes.NewReader(xorbBytes)); err != nil {
					t.Fatal(err)
				}
				shardObj := shard.NewShard()
				if err := shardObj.Decode(bytes.NewReader(shardBytes), false); err != nil {
					t.Fatal(err)
				}
				if mismatch == "file hash mismatch" {
					shardObj.Files[0].FileHash[0] ^= 1
				} else {
					shardObj.Files[0].MetadataExt.SHA256Hash[0] ^= 1
				}
				if err := shardObj.Validate(); err != nil {
					t.Fatal(err)
				}
				encoded, err := shardObj.Encode(false)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(encoded)
				if err != nil {
					t.Fatal(err)
				}
				response := httptest.NewRecorder()
				NewHandler(WithStorage(stor)).ServeHTTP(response, httptest.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body)))
				if endpoint == "/v2/shards" {
					if response.Code != http.StatusOK {
						t.Fatalf("status = %d, want 200", response.Code)
					}
					events := bufio.NewReader(response.Body)
					var last shardUploadWireEvent
					for _, err := events.Peek(1); err == nil; _, err = events.Peek(1) {
						last = readShardUploadWireEvent(t, events)
						if last.Type == "result" {
							t.Fatal("invalid shard reported success")
						}
					}
					if last.Type != "error" || last.Retryable || !strings.Contains(last.Message, mismatch) {
						t.Fatalf("terminal event = %+v, want non-retryable %q", last, mismatch)
					}
				} else if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), mismatch) {
					t.Fatalf("response = %d %q, want 400 %q", response.Code, response.Body.String(), mismatch)
				}
				if _, err := stor.GetShard(ctx, shardObj.Files[0].FileHash); err == nil {
					t.Fatal("invalid shard was stored")
				}
			})
		}
	}
}

func TestShardAuthorizationDeniedBeforeBody(t *testing.T) {
	_, fileHash, _, _, _, _, shardBytes := authorizerFixture(t)
	for _, endpoint := range []string{"/v1/shards", "/shards", "/v2/shards"} {
		t.Run(endpoint, func(t *testing.T) {
			target, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			probe := &shardV2TestStorage{Storage: target, putStarted: make(chan struct{}), putContinue: make(chan struct{})}
			close(probe.putContinue)
			authorizer := &recordingAuthorizer{err: auth.ErrForbidden}
			body := &unreadBody{t: t}
			request := httptest.NewRequest("POST", endpoint, body)
			request.ContentLength = int64(len(shardBytes))
			rec := httptest.NewRecorder()
			NewHandler(WithStorage(probe), WithAuthorizer(authorizer)).ServeHTTP(rec, request)
			if rec.Code != http.StatusForbidden || strings.TrimSpace(rec.Body.String()) != "forbidden" {
				t.Fatalf("response = %d %q", rec.Code, rec.Body.String())
			}
			if !reflect.DeepEqual(authorizer.calls, []auth.Grant{{Permission: auth.Write}}) {
				t.Fatalf("authorizations = %v", authorizer.calls)
			}
			select {
			case <-probe.putStarted:
				t.Fatal("PutShard called after authorization denied")
			default:
			}
			if _, err := target.GetShard(context.Background(), fileHash); err == nil {
				t.Fatal("denied file was stored")
			}
		})
	}
}

type unreadBody struct{ t *testing.T }

func (body *unreadBody) Read([]byte) (int, error) {
	body.t.Error("request body read before authorization")
	return 0, io.EOF
}

func TestUnknownReconstructionRequiresAuthorization(t *testing.T) {
	stor, _, _, _, _, _, _ := authorizerFixture(t)
	clock := time.Now()
	issuer, err := auth.NewIssuer(nil, time.Minute, func() time.Time { return clock })
	if err != nil {
		t.Fatal(err)
	}
	readToken, _, err := issuer.Sign(auth.Grant{Permission: auth.Read})
	if err != nil {
		t.Fatal(err)
	}
	writeToken, _, err := issuer.Sign(auth.Grant{Permission: auth.Write})
	if err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(-2 * time.Minute)
	expired, _, err := issuer.Sign(auth.Grant{Permission: auth.Read})
	if err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(2 * time.Minute)
	for _, endpoint := range []string{"/v1/reconstructions/", "/v2/reconstructions/", "/reconstructions?file_id=", "/reconstructions"} {
		url := endpoint
		if endpoint != "/reconstructions" {
			url += strings.Repeat("ab", 32)
		}
		for _, test := range []struct {
			name   string
			token  string
			status int
		}{
			{"missing", "", http.StatusUnauthorized},
			{"expired", expired, http.StatusUnauthorized},
			{"denied", writeToken, http.StatusForbidden},
			{"valid", readToken, http.StatusNotFound},
		} {
			t.Run(endpoint+"/"+test.name, func(t *testing.T) {
				var target storage.Storage
				want := test.status
				if test.name == "valid" {
					target = stor
					if strings.HasPrefix(endpoint, "/reconstructions") {
						want = http.StatusOK
					}
				}
				request := httptest.NewRequest("GET", url, nil)
				if test.token != "" {
					request.Header.Set("Authorization", "Bearer "+test.token)
				}
				rec := httptest.NewRecorder()
				NewHandler(WithStorage(target), WithAuthorizer(issuer)).ServeHTTP(rec, request)
				if rec.Code != want {
					t.Fatalf("status = %d, want %d", rec.Code, want)
				}
			})
		}
	}
}

func TestAuthorizerDenialMapping(t *testing.T) {
	stor, fileHash, _, _, _, _, shardBytes := authorizerFixture(t)
	for _, test := range []struct {
		err       error
		status    int
		challenge string
	}{
		{auth.ErrUnauthenticated, 401, "Bearer"},
		{errors.New("nope"), 403, ""},
	} {
		// Denying only targeted grants passes the untargeted pre-body shard gate and reaches the per-file check.
		handler := NewHandler(WithStorage(stor), WithAuthorizer(auth.AuthorizerFunc(func(r *http.Request, grant auth.Grant) error {
			if grant.File == (xet.FileHash{}) {
				return nil
			}
			return test.err
		})))
		for _, request := range []*http.Request{
			httptest.NewRequest("GET", "/v1/reconstructions/"+fileHash.String(), nil),
			httptest.NewRequest("POST", "/v1/shards", bytes.NewReader(shardBytes)),
		} {
			t.Run(test.err.Error()+" "+request.URL.Path, func(t *testing.T) {
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, request)
				if rec.Code != test.status || rec.Header().Get("WWW-Authenticate") != test.challenge || !strings.Contains(rec.Body.String(), test.err.Error()) {
					t.Fatalf("response = %d %v %q", rec.Code, rec.Header(), rec.Body.String())
				}
			})
		}
	}
}

func TestNilAuthorizer(t *testing.T) {
	stor, fileHash, _, _, _, _, _ := authorizerFixture(t)
	rec := httptest.NewRecorder()
	NewHandler(WithStorage(stor)).ServeHTTP(rec, httptest.NewRequest("GET", "/v1/reconstructions/"+fileHash.String(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
}

func TestRoutesAuthorizationParsingOrder(t *testing.T) {
	for _, test := range []struct {
		method string
		path   string
		body   string
		status int
		calls  int
	}{
		{"GET", "/v1/reconstructions/nothex", "", 400, 0},
		{"GET", "/v2/reconstructions/nothex", "", 400, 0},
		{"GET", "/reconstructions?file_id=nothex", "", 400, 0},
		{"GET", "/reconstructions?file_id=" + strings.Repeat("ab", 32) + "&file_id=nothex", "", 400, 0},
		{"HEAD", "/v1/xorbs/default/nothex", "", 403, 1},
		{"POST", "/v1/xorbs/default/nothex", "", 403, 1},
		{"GET", "/v1/chunks/default/nothex", "", 403, 1},
		{"POST", "/v1/chunks/default:query", "{", 403, 1},
		{"POST", "/v1/shards", "", 403, 1},
		{"POST", "/shards", "", 403, 1},
		{"POST", "/v2/shards", "", 403, 1},
	} {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			calls := 0
			authorizer := auth.AuthorizerFunc(func(r *http.Request, grant auth.Grant) error {
				calls++
				return auth.ErrForbidden
			})
			rec := httptest.NewRecorder()
			NewHandler(WithAuthorizer(authorizer)).ServeHTTP(rec, httptest.NewRequest(test.method, test.path, strings.NewReader(test.body)))
			if rec.Code != test.status || calls != test.calls {
				t.Fatalf("status = %d, calls = %d", rec.Code, calls)
			}
		})
	}
}

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

// TestXetBridgeServesEmptyDigest: the sha256 of zero bytes names content that
// is never ingested, so the bridge answers it without touching storage.
func TestXetBridgeServesEmptyDigest(t *testing.T) {
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(stor))
	digest := sha256.Sum256(nil)
	path := "/xet-bridge/" + hex.EncodeToString(digest[:])
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
		resp := rec.Result()
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s status = %d, want %d", method, resp.StatusCode, http.StatusOK)
		}
		if len(body) != 0 {
			t.Fatalf("%s body = %q, want empty", method, body)
		}
		if resp.ContentLength != 0 {
			t.Fatalf("%s Content-Length = %d, want 0", method, resp.ContentLength)
		}
	}
}
