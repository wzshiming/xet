package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	iofs "io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
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
)

// buildHealShard stores the xorbs for one file per filesParts entry and
// returns the assembled, not yet stored shard with each file's hash and
// SHA-256 hex digest.
func buildHealShard(t *testing.T, ctx context.Context, st Storage, filesParts ...[][]byte) (*shard.Shard, []xet.FileHash, []string) {
	t.Helper()
	shardObj := shard.NewShard()
	var fileHashes []xet.FileHash
	var shaHexes []string
	for _, parts := range filesParts {
		fileHash, _, _ := addGCFileBlock(t, ctx, st, shardObj, parts)
		fileHashes = append(fileHashes, fileHash)
		var content []byte
		for _, part := range parts {
			content = append(content, part...)
		}
		digest := sha256.Sum256(content)
		shaHexes = append(shaHexes, hex.EncodeToString(digest[:]))
	}
	return shardObj, fileHashes, shaHexes
}

// TestPutShardHealsPartialCommit: a retry of a multi-file shard whose first
// attempt died between index writes must finish the commit instead of
// treating one sibling's entry as proof the whole shard exists.
func TestPutShardHealsPartialCommit(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			shardObj, fileHashes, shaHexes := buildHealShard(t, ctx, st,
				[][]byte{[]byte("partial commit file one")},
				[][]byte{[]byte("partial commit file two")})
			if _, err := st.PutShard(ctx, shardObj); err != nil {
				t.Fatal(err)
			}
			shardHash, _, err := gcs.GetFileIndexEntry(ctx, fileHashes[0])
			if err != nil || shardHash == "" {
				t.Fatalf("file index entry = %q, %v", shardHash, err)
			}

			// Rewind to the mid-commit state: shard object, chunk entries and
			// file one's entries exist, file two's entries do not.
			if _, err := gcs.DeleteFileIndexEntry(ctx, fileHashes[1]); err != nil {
				t.Fatal(err)
			}
			if _, err := gcs.DeleteSHA256IndexEntry(ctx, shaHexes[1]); err != nil {
				t.Fatal(err)
			}

			inserted, err := st.PutShard(ctx, shardObj)
			if err != nil {
				t.Fatalf("PutShard retry: %v", err)
			}
			if inserted {
				t.Fatal("retry reported a new shard object")
			}
			for i, fileHash := range fileHashes {
				if _, err := st.GetShard(ctx, fileHash); err != nil {
					t.Fatalf("GetShard(file %d) after retry: %v", i, err)
				}
				if got, err := gcs.GetSHA256IndexEntry(ctx, shaHexes[i]); err != nil || got != shardHash {
					t.Fatalf("sha256 entry %d = %q, %v; want %q", i, got, err, shardHash)
				}
			}
		})
	}
}

// TestPutShardHealsDanglingEntry: entries left dangling by an out-of-band
// shard object loss must not suppress a re-upload of the same content; the
// shard object is re-stored and entries pointing at a missing shard are
// force-rewritten to it.
func TestPutShardHealsDanglingEntry(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			shardObj, fileHashes, shaHexes := buildHealShard(t, ctx, st,
				[][]byte{[]byte("dangling entry heal")})
			fileHash, shaHex := fileHashes[0], shaHexes[0]
			if _, err := st.PutShard(ctx, shardObj); err != nil {
				t.Fatal(err)
			}
			shardHash, _, err := gcs.GetFileIndexEntry(ctx, fileHash)
			if err != nil || shardHash == "" {
				t.Fatalf("file index entry = %q, %v", shardHash, err)
			}

			// The shard object vanishes out-of-band; entries keep pointing at
			// it. Same content re-uploads to the same hash, so the entries
			// are already correct once the object is back.
			if err := gcs.DeleteShard(ctx, shardHash); err != nil {
				t.Fatal(err)
			}
			inserted, err := st.PutShard(ctx, shardObj)
			if err != nil {
				t.Fatalf("PutShard after object loss: %v", err)
			}
			if !inserted {
				t.Fatal("shard object was not re-stored")
			}
			if _, err := gcs.GetShardByHash(ctx, shardHash); err != nil {
				t.Fatalf("shard object after re-upload: %v", err)
			}
			if _, err := st.GetShard(ctx, fileHash); err != nil {
				t.Fatalf("GetShard after re-upload: %v", err)
			}

			// Entries pointing at a different, missing shard must be
			// force-rewritten to the just-stored shard.
			bogus := strings.Repeat("0", 64)
			if err := gcs.SetFileIndexEntry(ctx, fileHash, bogus); err != nil {
				t.Fatal(err)
			}
			if err := gcs.SetSHA256IndexEntry(ctx, shaHex, bogus); err != nil {
				t.Fatal(err)
			}
			if _, err := st.PutShard(ctx, shardObj); err != nil {
				t.Fatalf("PutShard with bogus entries: %v", err)
			}
			if got, _, err := gcs.GetFileIndexEntry(ctx, fileHash); err != nil || got != shardHash {
				t.Fatalf("file entry = %q, %v; want %q", got, err, shardHash)
			}
			if got, err := gcs.GetSHA256IndexEntry(ctx, shaHex); err != nil || got != shardHash {
				t.Fatalf("sha256 entry = %q, %v; want %q", got, err, shardHash)
			}
			if _, err := st.GetShard(ctx, fileHash); err != nil {
				t.Fatalf("GetShard after force-rewrite: %v", err)
			}
		})
	}
}

// TestPutShardIgnoresPhantomCacheEntry: a warm file-index LRU whose stored
// entry was removed out-of-band (GC on another replica) must not suppress
// the store — the dedup gate reads storage, not the cache.
func TestPutShardIgnoresPhantomCacheEntry(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			shardObj, fileHashes, _ := buildHealShard(t, ctx, st,
				[][]byte{[]byte("phantom cache entry")})
			fileHash := fileHashes[0]
			if _, err := st.PutShard(ctx, shardObj); err != nil {
				t.Fatal(err)
			}
			shardHash, _, err := gcs.GetFileIndexEntry(ctx, fileHash)
			if err != nil || shardHash == "" {
				t.Fatalf("file index entry = %q, %v", shardHash, err)
			}

			// Warm the LRU, then remove the stored entry bypassing eviction.
			if _, err := st.GetShard(ctx, fileHash); err != nil {
				t.Fatal(err)
			}
			switch b := st.(type) {
			case *FileStorage:
				if err := os.Remove(b.objectPath("index/files", fileHash.String())); err != nil {
					t.Fatal(err)
				}
				if _, ok := b.fileIndex.Get(fileHash); !ok {
					t.Fatal("test setup: file-index cache is cold")
				}
			case *S3Storage:
				if err := b.deleteObject(ctx, b.objectKey("index/files", fileHash.String())); err != nil {
					t.Fatal(err)
				}
				if _, ok := b.fileIndex.Get(fileHash); !ok {
					t.Fatal("test setup: file-index cache is cold")
				}
			default:
				t.Fatalf("unhandled backend %T", st)
			}

			if _, err := st.PutShard(ctx, shardObj); err != nil {
				t.Fatalf("PutShard with phantom cache: %v", err)
			}
			if got, _, err := gcs.GetFileIndexEntry(ctx, fileHash); err != nil || got != shardHash {
				t.Fatalf("file index entry after put = %q, %v; want %q", got, err, shardHash)
			}
			if _, err := st.GetShard(ctx, fileHash); err != nil {
				t.Fatalf("GetShard after put: %v", err)
			}
		})
	}
}

// TestPutShardHealsEmptyEntry: an entry stored with empty contents must be
// force-rewritten, not left in place by a write-if-absent no-op.
func TestPutShardHealsEmptyEntry(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			shardObj, fileHashes, shaHexes := buildHealShard(t, ctx, st,
				[][]byte{[]byte("empty entry heal")})
			fileHash, shaHex := fileHashes[0], shaHexes[0]
			if _, err := st.PutShard(ctx, shardObj); err != nil {
				t.Fatal(err)
			}
			shardHash, _, err := gcs.GetFileIndexEntry(ctx, fileHash)
			if err != nil || shardHash == "" {
				t.Fatalf("file index entry = %q, %v", shardHash, err)
			}

			if err := gcs.SetFileIndexEntry(ctx, fileHash, ""); err != nil {
				t.Fatal(err)
			}
			if err := gcs.SetSHA256IndexEntry(ctx, shaHex, ""); err != nil {
				t.Fatal(err)
			}

			if _, err := st.PutShard(ctx, shardObj); err != nil {
				t.Fatalf("PutShard with empty entries: %v", err)
			}
			if got, _, err := gcs.GetFileIndexEntry(ctx, fileHash); err != nil || got != shardHash {
				t.Fatalf("file entry = %q, %v; want %q", got, err, shardHash)
			}
			if got, err := gcs.GetSHA256IndexEntry(ctx, shaHex); err != nil || got != shardHash {
				t.Fatalf("sha256 entry = %q, %v; want %q", got, err, shardHash)
			}
			if _, err := st.GetShard(ctx, fileHash); err != nil {
				t.Fatalf("GetShard after heal: %v", err)
			}
		})
	}
}

// TestPutShardEvictsStaleCacheOnAbsentEntry: healing an absent entry must
// also evict a stale cached mapping, or a warm process keeps resolving the
// file through a shard that no longer exists.
func TestPutShardEvictsStaleCacheOnAbsentEntry(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			shared := [][]byte{[]byte("stale cache shared file")}
			shardA, hashesA, _ := buildHealShard(t, ctx, st,
				shared, [][]byte{[]byte("stale cache sibling a")})
			if _, err := st.PutShard(ctx, shardA); err != nil {
				t.Fatal(err)
			}
			fileHash := hashesA[0]
			hashA, _, err := gcs.GetFileIndexEntry(ctx, fileHash)
			if err != nil || hashA == "" {
				t.Fatalf("file index entry = %q, %v", hashA, err)
			}

			// Warm the LRU with file -> shard A, then lose A and the entry
			// out-of-band, bypassing eviction.
			if _, err := st.GetShard(ctx, fileHash); err != nil {
				t.Fatal(err)
			}
			switch b := st.(type) {
			case *FileStorage:
				if err := os.Remove(b.objectPath("index/files", fileHash.String())); err != nil {
					t.Fatal(err)
				}
			case *S3Storage:
				if err := b.deleteObject(ctx, b.objectKey("index/files", fileHash.String())); err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatalf("unhandled backend %T", st)
			}
			if err := gcs.DeleteShard(ctx, hashA); err != nil {
				t.Fatal(err)
			}

			// A different shard carrying the same file commits; the absent
			// entry is written and the stale mapping must go with it.
			shardB, hashesB, _ := buildHealShard(t, ctx, st,
				shared, [][]byte{[]byte("stale cache sibling b")})
			if hashesB[0] != fileHash {
				t.Fatalf("shared file hash mismatch: %s vs %s", hashesB[0], fileHash)
			}
			if _, err := st.PutShard(ctx, shardB); err != nil {
				t.Fatalf("PutShard shard B: %v", err)
			}
			hashB, _, err := gcs.GetFileIndexEntry(ctx, fileHash)
			if err != nil || hashB == "" || hashB == hashA {
				t.Fatalf("file entry = %q, %v; want new shard", hashB, err)
			}
			if _, err := st.GetShard(ctx, fileHash); err != nil {
				t.Fatalf("GetShard served the stale mapping: %v", err)
			}
		})
	}
}

// TestPutShardDedupHitWritesNothing: a full dedup hit must leave every
// stored file untouched (mtimes pinned), create no file, and leave no temp
// files behind.
func TestPutShardDedupHitWritesNothing(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	fs, err := NewFileStorage(WithBasePath(basePath))
	if err != nil {
		t.Fatal(err)
	}

	shardObj, _, _ := buildHealShard(t, ctx, fs,
		[][]byte{[]byte("dedup hit file one")},
		[][]byte{[]byte("dedup hit file two")})
	if _, err := fs.PutShard(ctx, shardObj); err != nil {
		t.Fatal(err)
	}

	// Pin every stored file to a known past mtime; any rewrite shows up as a
	// fresh timestamp regardless of filesystem time granularity.
	pin := time.Now().Add(-time.Hour).Truncate(time.Second)
	var before []string
	err = filepath.WalkDir(basePath, func(path string, d iofs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		before = append(before, path)
		return os.Chtimes(path, pin, pin)
	})
	if err != nil {
		t.Fatal(err)
	}

	inserted, err := fs.PutShard(ctx, shardObj)
	if err != nil || inserted {
		t.Fatalf("dedup PutShard = %v, %v; want false, nil", inserted, err)
	}

	var after []string
	err = filepath.WalkDir(basePath, func(path string, d iofs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		after = append(after, path)
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.ModTime().Equal(pin) {
			t.Errorf("%s was rewritten (mtime %v)", path, info.ModTime())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(before)
	slices.Sort(after)
	if !slices.Equal(before, after) {
		t.Fatalf("stored files changed:\nbefore %v\nafter  %v", before, after)
	}
}

// recordingHTTPClient forwards requests, recording every non-read call.
type recordingHTTPClient struct {
	mu       sync.Mutex
	mutating []string
}

func (rc *recordingHTTPClient) Do(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		rc.mu.Lock()
		rc.mutating = append(rc.mutating, req.Method+" "+req.URL.Path)
		rc.mu.Unlock()
	}
	return http.DefaultClient.Do(req)
}

func (rc *recordingHTTPClient) take() []string {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	out := rc.mutating
	rc.mutating = nil
	return out
}

// TestS3PutShardDedupHitWritesNothing: a full dedup hit must issue no
// mutating S3 request — only the index GETs and shard HEADs of the gate.
func TestS3PutShardDedupHitWritesNothing(t *testing.T) {
	ctx := context.Background()
	backend := s3mem.New()
	if err := backend.CreateBucket("test-bucket"); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(srv.Close)

	rec := &recordingHTTPClient{}
	client := s3.New(s3.Options{
		BaseEndpoint:               aws.String(srv.URL),
		Region:                     "us-east-1",
		Credentials:                credentials.NewStaticCredentialsProvider("test", "test", ""),
		UsePathStyle:               true,
		HTTPClient:                 rec,
		Retryer:                    aws.NopRetryer{},
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	ss, err := NewS3Storage(ctx, WithS3Client(client), WithS3Bucket("test-bucket"))
	if err != nil {
		t.Fatal(err)
	}

	shardObj, _, _ := buildHealShard(t, ctx, ss,
		[][]byte{[]byte("s3 dedup file one")},
		[][]byte{[]byte("s3 dedup file two")})
	if _, err := ss.PutShard(ctx, shardObj); err != nil {
		t.Fatal(err)
	}
	rec.take()

	inserted, err := ss.PutShard(ctx, shardObj)
	if err != nil || inserted {
		t.Fatalf("dedup PutShard = %v, %v; want false, nil", inserted, err)
	}
	if got := rec.take(); len(got) != 0 {
		t.Fatalf("dedup hit issued mutating requests: %v", got)
	}
}
