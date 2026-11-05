// Package s3 implements storage.Storage over an S3-compatible object store
// with the local backend's layout under an optional key prefix. Puts and
// Deletes probe their key with HEAD first instead of writing conditionally,
// and xorb download URLs are presigned by default.
// Xorbs are read from mapped whole-object local copies in a bounded cache.
package s3

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/xorbmap"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"golang.org/x/sync/errgroup"
)

// defaultPresignExpiry keeps presigned xorb URLs valid long enough for
// clients to work through a large reconstruction term by term.
const defaultPresignExpiry = time.Hour

const defaultXorbCacheSize = 512

// Storage implements storage.Storage backed by an S3-compatible object store.
// It uses the same object layout as the local backend (xorbs/, shards/,
// index/files/, index/chunks/, index/sha256/ with a two-level 2/2/60 hash
// fanout), so a bucket populated by syncing a local storage directory is
// directly usable.
type Storage struct {
	client          *awss3.Client
	presignClient   *awss3.PresignClient
	bucket          string
	prefix          string
	baseURL         string
	presign         bool
	presignExpiry   time.Duration
	presignEndpoint string

	caches *storage.IndexCaches

	cacheDir      string
	xorbCacheSize int
	xorbCache     *xorbmap.Cache

	// xorbLive counts live mappings per hash so an old mapping's release never removes a refilled copy.
	xorbFiles sync.Mutex
	xorbLive  map[xet.XorbHash]int

	// endpoint, region and pathStyle configure the lazily created client
	// when one is not injected directly.
	endpoint  string
	region    string
	pathStyle bool
}

type Option func(*Storage)

// WithS3Client injects a pre-configured S3 client, bypassing the default
// AWS configuration chain. Mainly useful for tests and custom transports.
func WithS3Client(client *awss3.Client) Option {
	return func(ss *Storage) {
		ss.client = client
	}
}

// WithBucket sets the bucket that holds all objects.
func WithBucket(bucket string) Option {
	return func(ss *Storage) {
		ss.bucket = bucket
	}
}

// WithPrefix sets an optional key prefix under which all objects live.
func WithPrefix(prefix string) Option {
	return func(ss *Storage) {
		ss.prefix = strings.Trim(prefix, "/")
	}
}

// WithBaseURL sets the base URL used when generating xorb download URLs,
// mirroring the local backend's WithBaseURL.
func WithBaseURL(baseURL string) Option {
	return func(ss *Storage) {
		ss.baseURL = baseURL
	}
}

// WithEndpoint overrides the S3 endpoint, e.g. for MinIO or other
// S3-compatible stores.
func WithEndpoint(endpoint string) Option {
	return func(ss *Storage) {
		ss.endpoint = endpoint
	}
}

// WithRegion sets the region used for signing requests.
func WithRegion(region string) Option {
	return func(ss *Storage) {
		ss.region = region
	}
}

// WithPathStyle forces path-style addressing (bucket in the URL path
// rather than the host), required by most self-hosted S3 implementations.
func WithPathStyle(pathStyle bool) Option {
	return func(ss *Storage) {
		ss.pathStyle = pathStyle
	}
}

// WithPresign controls whether xorb download URLs are presigned S3 GET
// URLs (default) or relative paths served through the CAS server. Disable
// it when clients cannot reach the S3 endpoint directly.
func WithPresign(presign bool) Option {
	return func(ss *Storage) {
		ss.presign = presign
	}
}

// WithPresignExpiry sets how long presigned xorb URLs stay valid.
// Non-positive values are ignored.
func WithPresignExpiry(expiry time.Duration) Option {
	return func(ss *Storage) {
		if expiry > 0 {
			ss.presignExpiry = expiry
		}
	}
}

// WithPresignEndpoint presigns xorb URLs against a different endpoint than
// the one the server itself uses, for deployments where clients reach the
// object store through a public address while the server uses an internal
// one. Signatures cover the host, so the two endpoints must be the same
// store; defaults to the server's endpoint.
func WithPresignEndpoint(endpoint string) Option {
	return func(ss *Storage) {
		ss.presignEndpoint = endpoint
	}
}

// WithCacheDir sets the directory for local xorb copies (default xet-s3-xorbs under os.TempDir());
// NewStorage empties its xorbs/ subtree, so use one Storage per directory.
func WithCacheDir(dir string) Option {
	return func(ss *Storage) {
		ss.cacheDir = dir
	}
}

// WithXorbCacheSize keeps at most n xorbs downloaded and mapped (default 512; unbounded when n < 1);
// evicted copies stay on disk until their readers close.
func WithXorbCacheSize(n int) Option {
	return func(ss *Storage) {
		ss.xorbCacheSize = n
	}
}

// NewStorage creates an S3-backed storage. Credentials are resolved
// through the standard AWS configuration chain (environment variables,
// shared config, IAM roles) unless a client is injected with WithS3Client.
func NewStorage(ctx context.Context, opts ...Option) (*Storage, error) {
	ss := &Storage{
		presign:       true,
		presignExpiry: defaultPresignExpiry,
		caches:        storage.NewIndexCaches(),
		xorbCacheSize: defaultXorbCacheSize,
		xorbLive:      make(map[xet.XorbHash]int),
	}

	for _, opt := range opts {
		opt(ss)
	}

	if ss.bucket == "" {
		return nil, fmt.Errorf("s3 storage requires a bucket")
	}

	if ss.cacheDir == "" {
		ss.cacheDir = filepath.Join(os.TempDir(), "xet-s3-xorbs")
	}
	// Copies left by an earlier process are unknown to this cache and would never be removed.
	xorbDir := filepath.Join(ss.cacheDir, "xorbs")
	if err := os.RemoveAll(xorbDir); err != nil {
		return nil, fmt.Errorf("clear xorb cache: %w", err)
	}
	if err := os.MkdirAll(xorbDir, 0o755); err != nil {
		return nil, fmt.Errorf("create xorb cache: %w", err)
	}
	ss.xorbCache = xorbmap.New(ss.xorbCacheSize, ss.downloadXorb, ss.removeXorbFile)

	if ss.client == nil {
		var cfgOpts []func(*config.LoadOptions) error
		if ss.region != "" {
			cfgOpts = append(cfgOpts, config.WithRegion(ss.region))
		}
		cfg, err := config.LoadDefaultConfig(ctx, cfgOpts...)
		if err != nil {
			return nil, fmt.Errorf("load aws config: %w", err)
		}
		ss.client = awss3.NewFromConfig(cfg, func(o *awss3.Options) {
			if ss.endpoint != "" {
				o.BaseEndpoint = aws.String(ss.endpoint)
			}
			o.UsePathStyle = ss.pathStyle
		})
	}
	if ss.presignEndpoint != "" {
		// The presign client never sends requests; it only shapes and signs
		// the URL, so a client copy with the public endpoint is enough.
		presignOpts := ss.client.Options()
		presignOpts.BaseEndpoint = aws.String(ss.presignEndpoint)
		ss.presignClient = awss3.NewPresignClient(awss3.New(presignOpts))
	} else {
		ss.presignClient = awss3.NewPresignClient(ss.client)
	}

	return ss, nil
}

// objectKey returns the same two-level fanout layout the local backend uses
// on disk, under the configured prefix.
func (ss *Storage) objectKey(kind, name string) string {
	key := storage.ObjectKey(kind, name)
	if ss.prefix != "" {
		return ss.prefix + "/" + key
	}
	return key
}

// isS3NotFound reports whether err indicates a missing object, covering
// GetObject (NoSuchKey), HeadObject (NotFound) and plain 404 responses.
func isS3NotFound(err error) bool {
	if _, ok := errors.AsType[*types.NoSuchKey](err); ok {
		return true
	}
	if _, ok := errors.AsType[*types.NotFound](err); ok {
		return true
	}
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusNotFound {
		return true
	}
	return false
}

func (ss *Storage) headObject(ctx context.Context, key string) (int64, bool, error) {
	out, err := ss.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(ss.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isS3NotFound(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return aws.ToInt64(out.ContentLength), true, nil
}

func (ss *Storage) getObject(ctx context.Context, key string) ([]byte, error) {
	body, err := ss.getObjectReader(ctx, key)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	return io.ReadAll(body)
}

// getObjectReader returns the object body as a stream; the caller must close it.
func (ss *Storage) getObjectReader(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := ss.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(ss.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

func (ss *Storage) putObject(ctx context.Context, key string, body io.ReadSeeker) error {
	_, err := ss.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(ss.bucket),
		Key:    aws.String(key),
		Body:   body,
	})
	return err
}

// putObjectIfAbsent calls upload unless HEAD finds an object at key,
// reporting whether it did; what names the object in a failed HEAD's error.
// The probe stands in for a conditional write.
func (ss *Storage) putObjectIfAbsent(ctx context.Context, what, key string, upload func() error) (bool, error) {
	if _, exists, err := ss.headObject(ctx, key); err != nil {
		return false, fmt.Errorf("check %s: %w", what, err)
	} else if exists {
		return false, nil
	}
	if err := upload(); err != nil {
		return false, err
	}
	return true, nil
}

// putIndexObject writes shardHash as the index object at key unless one is
// there, reporting whether it wrote; what names the index in a failed
// HEAD's error.
func (ss *Storage) putIndexObject(ctx context.Context, what, key, shardHash string) (bool, error) {
	return ss.putObjectIfAbsent(ctx, what, key, func() error {
		return ss.putObject(ctx, key, strings.NewReader(shardHash))
	})
}

// PutXorb stores an xorb. The stream is spooled to a temporary file, then
// uploaded with a known length so the SDK can sign and retry the request.
func (ss *Storage) PutXorb(ctx context.Context, xorbHash xet.XorbHash, r io.Reader) (bool, error) {
	key := ss.objectKey("xorbs", xorbHash.String())

	// Dedup hits leave the stored object, including LastModified, untouched.
	return ss.putObjectIfAbsent(ctx, "xorb object", key, func() error {
		f, err := os.CreateTemp("", "xet-xorb-*")
		if err != nil {
			return fmt.Errorf("create xorb spool file: %w", err)
		}
		defer func() {
			f.Close()
			os.Remove(f.Name())
		}()

		if _, err := io.Copy(f, r); err != nil {
			return fmt.Errorf("spool xorb: %w", err)
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("rewind xorb spool file: %w", err)
		}

		if err := ss.putObject(ctx, key, f); err != nil {
			return fmt.Errorf("upload xorb: %w", err)
		}
		return nil
	})
}

// GetXorbReadSeekCloser reads the xorb's mapped local copy, held until Close;
// a miss downloads the whole object once, ignoring ctx.
func (ss *Storage) GetXorbReadSeekCloser(ctx context.Context, xorbHash xet.XorbHash) (io.ReadSeekCloser, error) {
	m, err := ss.xorbCache.Acquire(xorbHash)
	if err != nil {
		return nil, fmt.Errorf("get xorb: %w", err)
	}
	return m.Reader(), nil
}

// HasXorb checks whether an xorb exists; a cached copy answers without a request.
func (ss *Storage) HasXorb(ctx context.Context, xorbHash xet.XorbHash) (bool, error) {
	if ss.xorbCache.Has(xorbHash) {
		return true, nil
	}
	_, exists, err := ss.headObject(ctx, ss.objectKey("xorbs", xorbHash.String()))
	if err != nil {
		return false, fmt.Errorf("check xorb object: %w", err)
	}
	return exists, nil
}

// xorbChunkOffsets returns the cumulative packed end-offset of every chunk in
// the xorb, from the in-memory cache, the xorb footer, or a full scan for
// footer-less xorbs.
func (ss *Storage) xorbChunkOffsets(xorbHash xet.XorbHash) ([]uint64, error) {
	return ss.caches.Offsets.GetOrLoad(xorbHash, func() ([]uint64, error) {
		m, err := ss.xorbCache.Acquire(xorbHash)
		if err != nil {
			return nil, fmt.Errorf("get xorb: %w", err)
		}
		defer m.Release()
		offsets, err := storage.ReadXorbChunkOffsets(bytes.NewReader(m.Bytes()))
		if err != nil {
			return nil, fmt.Errorf("read xorb chunk offsets: %w", err)
		}
		return offsets, nil
	})
}

// GetXorbChunkOffsets returns the xorb's chunk offset table; cached after
// the first read.
func (ss *Storage) GetXorbChunkOffsets(ctx context.Context, xorbHash xet.XorbHash) ([]uint64, error) {
	return ss.xorbChunkOffsets(xorbHash)
}

// PutShard stores the shard under the SHA-256 of its encoding unless that object exists.
func (ss *Storage) PutShard(ctx context.Context, s *shard.Shard) (string, bool, error) {
	encoded, shardHash, err := storage.EncodeShard(s)
	if err != nil {
		return "", false, fmt.Errorf("serialize shard: %w", err)
	}
	key := ss.objectKey("shards", shardHash)
	inserted, err := ss.putObjectIfAbsent(ctx, "shard object", key, func() error {
		if err := ss.putObject(ctx, key, bytes.NewReader(encoded)); err != nil {
			return fmt.Errorf("upload shard: %w", err)
		}
		return nil
	})
	if err != nil {
		return "", false, err
	}
	return shardHash, inserted, nil
}

// PutFileIndexEntry writes the index/files entry for fileHash unless one
// exists. Nothing is cached on writes, but each Put evicts the key's cached
// mapping, so warm reads follow the stored entry even after it was deleted
// behind this instance's back.
func (ss *Storage) PutFileIndexEntry(ctx context.Context, fileHash xet.FileHash, shardHash string) (bool, error) {
	inserted, err := ss.putIndexObject(ctx, "file index", ss.objectKey("index/files", fileHash.String()), shardHash)
	if err != nil {
		return false, err
	}
	ss.caches.Files.Remove(fileHash)
	return inserted, nil
}

// PutChunkIndexEntry writes the index/chunks entry for chunkHash unless one exists, evicting its cached mapping.
func (ss *Storage) PutChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash, shardHash string) (bool, error) {
	inserted, err := ss.putIndexObject(ctx, "chunk index", ss.objectKey("index/chunks", chunkHash.String()), shardHash)
	if err != nil {
		return false, err
	}
	ss.caches.Chunks.Remove(chunkHash)
	return inserted, nil
}

// PutSHA256IndexEntry writes the index/sha256 entry for digest unless one exists, evicting its cached mapping.
func (ss *Storage) PutSHA256IndexEntry(ctx context.Context, digest [32]byte, shardHash string) (bool, error) {
	inserted, err := ss.putIndexObject(ctx, "SHA-256 index", ss.objectKey("index/sha256", hex.EncodeToString(digest[:])), shardHash)
	if err != nil {
		return false, err
	}
	ss.caches.SHA256.Remove(digest)
	return inserted, nil
}

func (ss *Storage) getShardByHash(ctx context.Context, shardHash string) (*shard.Shard, error) {
	return ss.caches.Shards.GetOrLoad(shardHash, func() (*shard.Shard, error) {
		return ss.loadShard(ctx, shardHash)
	})
}

// loadShard reads and decodes a stored shard object straight from S3.
func (ss *Storage) loadShard(ctx context.Context, shardHash string) (*shard.Shard, error) {
	out, err := ss.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(ss.bucket),
		Key:    aws.String(ss.objectKey("shards", shardHash)),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	s, err := storage.DecodeStoredShard(out.Body)
	if err != nil {
		return nil, err
	}
	if s.Footer == nil {
		// xet-core prunes cached dedup shards oldest-first by the footer creation
		// time, so pin it to the ingest time instead of the first-serve time.
		creationTime := time.Now()
		if out.LastModified != nil {
			creationTime = *out.LastModified
		}
		s.SetFooter(creationTime)
	}
	return s, nil
}

// LoadShard reads a stored shard object, bypassing the shard cache both
// ways so bulk scans cannot evict hot entries. The returned error wraps
// fs.ErrNotExist when the shard is absent.
func (ss *Storage) LoadShard(ctx context.Context, shardHash string) (*shard.Shard, error) {
	s, err := ss.loadShard(ctx, shardHash)
	if err != nil && isS3NotFound(err) {
		return nil, fmt.Errorf("shard %s: %w", shardHash, iofs.ErrNotExist)
	}
	return s, err
}

// GetShardByHash loads a stored shard by the hash of its serialized bytes.
// The returned error wraps fs.ErrNotExist when the shard is absent.
func (ss *Storage) GetShardByHash(ctx context.Context, shardHash string) (*shard.Shard, error) {
	s, err := ss.getShardByHash(ctx, shardHash)
	if err != nil && isS3NotFound(err) {
		return nil, fmt.Errorf("shard %s: %w", shardHash, iofs.ErrNotExist)
	}
	return s, err
}

// indexWalkConcurrency bounds parallel index-object reads during walks.
const indexWalkConcurrency = 4

// walkIndex calls fn with the name and recorded shard hash of every committed
// entry under kind; what names the index in errors.
func (ss *Storage) walkIndex(ctx context.Context, kind, what string, fn func(name, shardHash string) error) error {
	base := kind + "/"
	if ss.prefix != "" {
		base = ss.prefix + "/" + base
	}
	paginator := awss3.NewListObjectsV2Paginator(ss.client, &awss3.ListObjectsV2Input{
		Bucket: aws.String(ss.bucket),
		Prefix: aws.String(base),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list %s: %w", what, err)
		}
		var keys, names []string
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			name := strings.ReplaceAll(strings.TrimPrefix(key, base), "/", "")
			if len(name) != 64 {
				continue
			}
			if _, err := hex.DecodeString(name); err != nil {
				continue
			}
			keys = append(keys, key)
			names = append(names, name)
		}
		// Read the page's index objects in parallel; fn stays sequential.
		bodies := make([][]byte, len(keys))
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(indexWalkConcurrency)
		for i, key := range keys {
			g.Go(func() error {
				data, err := ss.getObject(gctx, key)
				if err != nil {
					// Deleted between list and read: leave the slot nil.
					if isS3NotFound(err) {
						return nil
					}
					return fmt.Errorf("read %s %s: %w", what, key, err)
				}
				bodies[i] = data
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
		for i, name := range names {
			if bodies[i] == nil {
				continue
			}
			if err := fn(name, strings.TrimSpace(string(bodies[i]))); err != nil {
				return err
			}
		}
	}
	return nil
}

// WalkFileIndex calls fn for every committed index/files entry.
func (ss *Storage) WalkFileIndex(ctx context.Context, fn func(fileHash, shardHash string) error) error {
	return ss.walkIndex(ctx, "index/files", "file index", fn)
}

// WalkChunkIndex calls fn for every committed index/chunks entry.
func (ss *Storage) WalkChunkIndex(ctx context.Context, fn func(chunkHash, shardHash string) error) error {
	return ss.walkIndex(ctx, "index/chunks", "chunk index", fn)
}

// walkHashedObjects calls fn for every hash-named object stored under kind,
// using the listing's own size and modification time.
func (ss *Storage) walkHashedObjects(ctx context.Context, kind string, fn func(hash string, size int64, modTime time.Time) error) error {
	base := kind + "/"
	if ss.prefix != "" {
		base = ss.prefix + "/" + base
	}
	paginator := awss3.NewListObjectsV2Paginator(ss.client, &awss3.ListObjectsV2Input{
		Bucket: aws.String(ss.bucket),
		Prefix: aws.String(base),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list %s: %w", kind, err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			hash := strings.ReplaceAll(strings.TrimPrefix(key, base), "/", "")
			if len(hash) != 64 {
				continue
			}
			if _, err := hex.DecodeString(hash); err != nil {
				continue
			}
			var modTime time.Time
			if obj.LastModified != nil {
				modTime = *obj.LastModified
			}
			if err := fn(hash, aws.ToInt64(obj.Size), modTime); err != nil {
				return err
			}
		}
	}
	return nil
}

// WalkShards calls fn for every stored shard object.
func (ss *Storage) WalkShards(ctx context.Context, fn func(shardHash string, size int64, modTime time.Time) error) error {
	return ss.walkHashedObjects(ctx, "shards", fn)
}

// WalkXorbs calls fn for every stored xorb object.
func (ss *Storage) WalkXorbs(ctx context.Context, fn func(xorbHash string, size int64, modTime time.Time) error) error {
	return ss.walkHashedObjects(ctx, "xorbs", fn)
}

func (ss *Storage) Usage(ctx context.Context) (storage.Usage, error) {
	return storage.ComputeUsage(ctx, ss.walkHashedObjects)
}

// WalkSHA256Index calls fn for every committed index/sha256 entry.
func (ss *Storage) WalkSHA256Index(ctx context.Context, fn func(sha256Hex, shardHash string) error) error {
	return ss.walkIndex(ctx, "index/sha256", "SHA-256 index", fn)
}

// deleteObject removes one object; missing keys are not an error.
func (ss *Storage) deleteObject(ctx context.Context, key string) error {
	_, err := ss.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(ss.bucket),
		Key:    aws.String(key),
	})
	if err != nil && isS3NotFound(err) {
		return nil
	}
	return err
}

// deleteObjectIfExists deletes key when HEAD finds an object there, reporting
// whether it did; what names the object in errors.
func (ss *Storage) deleteObjectIfExists(ctx context.Context, what, key string) (bool, error) {
	_, exists, err := ss.headObject(ctx, key)
	if err != nil {
		return false, fmt.Errorf("check %s: %w", what, err)
	}
	if !exists {
		return false, nil
	}
	if err := ss.deleteObject(ctx, key); err != nil {
		return false, fmt.Errorf("delete %s: %w", what, err)
	}
	return true, nil
}

// DeleteFileIndexEntry removes the index/files entry for fileHash, reporting
// whether it existed.
func (ss *Storage) DeleteFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (bool, error) {
	existed, err := ss.deleteObjectIfExists(ctx, "file index", ss.objectKey("index/files", fileHash.String()))
	if err != nil {
		return false, err
	}
	// Evicting after the delete narrows but does not close the re-cache window.
	ss.caches.Files.Remove(fileHash)
	return existed, nil
}

// GetFileIndexEntry returns the shard hash recorded for fileHash, or ""
// when the entry is absent, bypassing the cache so sweeps see stored state.
func (ss *Storage) GetFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (string, error) {
	data, err := ss.getObject(ctx, ss.objectKey("index/files", fileHash.String()))
	if err != nil {
		if isS3NotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("read file index: %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}

// DeleteShard removes a stored shard object, reporting whether it existed.
func (ss *Storage) DeleteShard(ctx context.Context, shardHash string) (bool, error) {
	existed, err := ss.deleteObjectIfExists(ctx, "shard", ss.objectKey("shards", shardHash))
	ss.caches.Shards.Remove(shardHash)
	return existed, err
}

// DeleteXorb removes a stored xorb object, reporting whether it existed;
// readers already open keep their copy until Close.
func (ss *Storage) DeleteXorb(ctx context.Context, xorbHash xet.XorbHash) (bool, error) {
	existed, err := ss.deleteObjectIfExists(ctx, "xorb", ss.objectKey("xorbs", xorbHash.String()))
	ss.xorbCache.Remove(xorbHash)
	ss.caches.Offsets.Remove(xorbHash)
	return existed, err
}

// GetChunkIndexEntry returns the shard hash recorded for chunkHash, or ""
// when the entry is absent, bypassing the cache so sweeps see stored state.
func (ss *Storage) GetChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash) (string, error) {
	data, err := ss.getObject(ctx, ss.objectKey("index/chunks", chunkHash.String()))
	if err != nil {
		if isS3NotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("read chunk index: %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}

// DeleteChunkIndexEntry removes the index/chunks entry for chunkHash,
// reporting whether it existed.
func (ss *Storage) DeleteChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash) (bool, error) {
	existed, err := ss.deleteObjectIfExists(ctx, "chunk index", ss.objectKey("index/chunks", chunkHash.String()))
	ss.caches.Chunks.Remove(chunkHash)
	return existed, err
}

// GetSHA256IndexEntry returns the shard hash recorded for the SHA-256
// digest, or "" when the entry is absent, bypassing the cache.
func (ss *Storage) GetSHA256IndexEntry(ctx context.Context, digest [32]byte) (string, error) {
	data, err := ss.getObject(ctx, ss.objectKey("index/sha256", hex.EncodeToString(digest[:])))
	if err != nil {
		if isS3NotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("read SHA-256 index: %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}

// DeleteSHA256IndexEntry removes the index/sha256 entry, reporting whether
// it existed.
func (ss *Storage) DeleteSHA256IndexEntry(ctx context.Context, digest [32]byte) (bool, error) {
	existed, err := ss.deleteObjectIfExists(ctx, "SHA-256 index", ss.objectKey("index/sha256", hex.EncodeToString(digest[:])))
	if err != nil {
		return false, err
	}
	// Evicting after the delete narrows but does not close the re-cache window.
	ss.caches.SHA256.Remove(digest)
	return existed, nil
}

// GetShard retrieves a shard by file hash. The returned error wraps
// fs.ErrNotExist when the file index entry or the shard object is absent.
func (ss *Storage) GetShard(ctx context.Context, fileHash xet.FileHash) (*shard.Shard, error) {
	s, err := ss.getShardByFileHash(ctx, fileHash)
	if err != nil && isS3NotFound(err) {
		return nil, fmt.Errorf("file %s: %w", fileHash.String(), iofs.ErrNotExist)
	}
	return s, err
}

func (ss *Storage) getShardByFileHash(ctx context.Context, fileHash xet.FileHash) (*shard.Shard, error) {
	shardHash, err := ss.caches.Files.GetOrLoad(fileHash, func() (string, error) {
		data, err := ss.getObject(ctx, ss.objectKey("index/files", fileHash.String()))
		if err != nil {
			return "", fmt.Errorf("read file index: %w", err)
		}
		return strings.TrimSpace(string(data)), nil
	})
	if err != nil {
		return nil, err
	}
	return ss.getShardByHash(ctx, shardHash)
}

// GetShardByChunkHash retrieves a shard by chunk hash (for deduplication)
func (ss *Storage) GetShardByChunkHash(ctx context.Context, chunkHash xet.ChunkHash) (*shard.Shard, error) {
	shardHash, err := ss.caches.Chunks.GetOrLoad(chunkHash, func() (string, error) {
		data, err := ss.getObject(ctx, ss.objectKey("index/chunks", chunkHash.String()))
		if err != nil {
			if isS3NotFound(err) {
				return "", fmt.Errorf("chunk not found")
			}
			return "", fmt.Errorf("read chunk index: %w", err)
		}
		return strings.TrimSpace(string(data)), nil
	})
	if err != nil {
		return nil, err
	}
	return ss.getShardByHash(ctx, shardHash)
}

// GetFileHashBySHA256 resolves a SHA-256 digest to the xet file hash recorded
// at ingest, loading the owning shard and matching its file metadata.
func (ss *Storage) GetFileHashBySHA256(ctx context.Context, digest [32]byte) (xet.FileHash, error) {
	sh, err := ss.getShardBySHA256(ctx, digest)
	if err != nil {
		return xet.FileHash{}, err
	}
	file := sh.FindFileBySHA256(digest)
	if file == nil {
		return xet.FileHash{}, fmt.Errorf("SHA-256 is not present in shard")
	}
	return file.FileHash, nil
}

// getShardBySHA256 resolves a SHA-256 digest through index/sha256/<digest>,
// whose contents are the hash of the owning shard, reading the index through
// the bounded cache.
func (ss *Storage) getShardBySHA256(ctx context.Context, digest [32]byte) (*shard.Shard, error) {
	shardHash, err := ss.caches.SHA256.GetOrLoad(digest, func() (string, error) {
		data, err := ss.getObject(ctx, ss.objectKey("index/sha256", hex.EncodeToString(digest[:])))
		if err != nil {
			if isS3NotFound(err) {
				return "", fmt.Errorf("sha256 %s: %w", hex.EncodeToString(digest[:]), iofs.ErrNotExist)
			}
			return "", fmt.Errorf("read SHA-256 index: %w", err)
		}
		return strings.TrimSpace(string(data)), nil
	})
	if err != nil {
		return nil, err
	}
	return ss.getShardByHash(ctx, shardHash)
}

// GetShardBySHA256 retrieves the shard holding the file with the SHA-256 digest.
func (ss *Storage) GetShardBySHA256(ctx context.Context, digest [32]byte) (*shard.Shard, error) {
	return ss.getShardBySHA256(ctx, digest)
}

// GetXorbURL generates a URL for accessing xorb data. By default it is a
// presigned S3 GET URL so clients fetch xorb ranges straight from the object
// store; with presigning disabled the URL routes through the CAS server's
// xorb endpoint like the local backend.
func (ss *Storage) GetXorbURL(ctx context.Context, xorbHash xet.XorbHash) (string, error) {
	if ss.presign {
		req, err := ss.presignClient.PresignGetObject(ctx, &awss3.GetObjectInput{
			Bucket: aws.String(ss.bucket),
			Key:    aws.String(ss.objectKey("xorbs", xorbHash.String())),
		}, awss3.WithPresignExpires(ss.presignExpiry))
		if err != nil {
			return "", fmt.Errorf("presign xorb URL: %w", err)
		}
		return req.URL, nil
	}
	if ss.baseURL == "" {
		// If no base URL is configured, return a relative path
		return fmt.Sprintf("/v1/xorbs/default/%s", xorbHash.String()), nil
	}
	return fmt.Sprintf("%s/v1/xorbs/default/%s", ss.baseURL, xorbHash.String()), nil
}

var _ storage.Storage = (*Storage)(nil)
