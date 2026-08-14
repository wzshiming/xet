package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/golang/groupcache/lru"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// defaultPresignExpiry keeps presigned xorb URLs valid long enough for
// clients to work through a large reconstruction term by term.
const defaultPresignExpiry = time.Hour

// S3Storage implements Storage backed by an S3-compatible object store. It
// uses the same object layout as FileStorage (xorbs/, shards/, files/,
// chunks/, sha256/ with a two-character fanout), so a bucket populated by
// syncing a FileStorage directory is directly usable.
type S3Storage struct {
	client          *s3.Client
	presignClient   *s3.PresignClient
	bucket          string
	prefix          string
	baseURL         string
	presign         bool
	presignExpiry   time.Duration
	presignEndpoint string

	fileIndex    *lru.Cache // bounded file hash -> shard hash
	shardIndex   *lru.Cache // bounded shard hash -> shard cache
	chunkIndex   *lru.Cache // bounded chunk hash -> shard hash
	sha256Index  *lru.Cache // bounded SHA-256 -> file hash
	offsetsIndex *lru.Cache // bounded xorb hash -> []uint64 packed chunk end-offsets

	fileMut    sync.Mutex // guards fileIndex
	shardMut   sync.Mutex // guards shardIndex
	chunkMut   sync.Mutex // guards chunkIndex
	sha256Mut  sync.Mutex // guards sha256Index
	offsetsMut sync.Mutex // guards offsetsIndex

	// endpoint, region and pathStyle configure the lazily created client
	// when one is not injected directly.
	endpoint  string
	region    string
	pathStyle bool
}

type S3Option func(*S3Storage)

// WithS3Client injects a pre-configured S3 client, bypassing the default
// AWS configuration chain. Mainly useful for tests and custom transports.
func WithS3Client(client *s3.Client) S3Option {
	return func(ss *S3Storage) {
		ss.client = client
	}
}

// WithS3Bucket sets the bucket that holds all objects.
func WithS3Bucket(bucket string) S3Option {
	return func(ss *S3Storage) {
		ss.bucket = bucket
	}
}

// WithS3Prefix sets an optional key prefix under which all objects live.
func WithS3Prefix(prefix string) S3Option {
	return func(ss *S3Storage) {
		ss.prefix = strings.Trim(prefix, "/")
	}
}

// WithS3BaseURL sets the base URL used when generating xorb download URLs,
// mirroring WithBaseURL on FileStorage.
func WithS3BaseURL(baseURL string) S3Option {
	return func(ss *S3Storage) {
		ss.baseURL = baseURL
	}
}

// WithS3Endpoint overrides the S3 endpoint, e.g. for MinIO or other
// S3-compatible stores.
func WithS3Endpoint(endpoint string) S3Option {
	return func(ss *S3Storage) {
		ss.endpoint = endpoint
	}
}

// WithS3Region sets the region used for signing requests.
func WithS3Region(region string) S3Option {
	return func(ss *S3Storage) {
		ss.region = region
	}
}

// WithS3PathStyle forces path-style addressing (bucket in the URL path
// rather than the host), required by most self-hosted S3 implementations.
func WithS3PathStyle(pathStyle bool) S3Option {
	return func(ss *S3Storage) {
		ss.pathStyle = pathStyle
	}
}

// WithS3Presign controls whether xorb download URLs are presigned S3 GET
// URLs (default) or relative paths served through the CAS server. Disable
// it when clients cannot reach the S3 endpoint directly.
func WithS3Presign(presign bool) S3Option {
	return func(ss *S3Storage) {
		ss.presign = presign
	}
}

// WithS3PresignExpiry sets how long presigned xorb URLs stay valid.
// Non-positive values are ignored.
func WithS3PresignExpiry(expiry time.Duration) S3Option {
	return func(ss *S3Storage) {
		if expiry > 0 {
			ss.presignExpiry = expiry
		}
	}
}

// WithS3PresignEndpoint presigns xorb URLs against a different endpoint than
// the one the server itself uses, for deployments where clients reach the
// object store through a public address while the server uses an internal
// one. Signatures cover the host, so the two endpoints must be the same
// store; defaults to the server's endpoint.
func WithS3PresignEndpoint(endpoint string) S3Option {
	return func(ss *S3Storage) {
		ss.presignEndpoint = endpoint
	}
}

// NewS3Storage creates an S3-backed storage. Credentials are resolved
// through the standard AWS configuration chain (environment variables,
// shared config, IAM roles) unless a client is injected with WithS3Client.
func NewS3Storage(ctx context.Context, opts ...S3Option) (*S3Storage, error) {
	ss := &S3Storage{
		presign:       true,
		presignExpiry: defaultPresignExpiry,
		fileIndex:     lru.New(defaultFileCacheSize),
		shardIndex:    lru.New(defaultShardCacheSize),
		chunkIndex:    lru.New(defaultChunkCacheSize),
		sha256Index:   lru.New(defaultSHA256CacheSize),
		offsetsIndex:  lru.New(defaultOffsetsCacheSize),
	}

	for _, opt := range opts {
		opt(ss)
	}

	if ss.bucket == "" {
		return nil, fmt.Errorf("s3 storage requires a bucket")
	}

	if ss.client == nil {
		var cfgOpts []func(*config.LoadOptions) error
		if ss.region != "" {
			cfgOpts = append(cfgOpts, config.WithRegion(ss.region))
		}
		cfg, err := config.LoadDefaultConfig(ctx, cfgOpts...)
		if err != nil {
			return nil, fmt.Errorf("load aws config: %w", err)
		}
		ss.client = s3.NewFromConfig(cfg, func(o *s3.Options) {
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
		ss.presignClient = s3.NewPresignClient(s3.New(presignOpts))
	} else {
		ss.presignClient = s3.NewPresignClient(ss.client)
	}

	return ss, nil
}

// objectKey returns the same git-style fanout layout FileStorage uses on
// disk: <prefix>/<kind>/<name[:2]>/<name[2:]>.
func (ss *S3Storage) objectKey(kind, name string) string {
	var key string
	if len(name) <= 2 {
		key = kind + "/" + name
	} else {
		key = kind + "/" + name[:2] + "/" + name[2:]
	}
	if ss.prefix != "" {
		return ss.prefix + "/" + key
	}
	return key
}

// isS3NotFound reports whether err indicates a missing object, covering
// GetObject (NoSuchKey), HeadObject (NotFound) and plain 404 responses.
func isS3NotFound(err error) bool {
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusNotFound {
		return true
	}
	return false
}

func (ss *S3Storage) headObject(ctx context.Context, key string) (int64, bool, error) {
	out, err := ss.client.HeadObject(ctx, &s3.HeadObjectInput{
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

func (ss *S3Storage) getObject(ctx context.Context, key string) ([]byte, error) {
	out, err := ss.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(ss.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

// getObjectRange returns the [start, end] byte range (inclusive) of an object.
func (ss *S3Storage) getObjectRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	out, err := ss.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(ss.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

func (ss *S3Storage) putObject(ctx context.Context, key string, body io.ReadSeeker) error {
	_, err := ss.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(ss.bucket),
		Key:    aws.String(key),
		Body:   body,
	})
	return err
}

// putIndexObject writes a small index object. Unlike writeIndexFile it
// overwrites unconditionally: any shard containing the chunk/file is a valid
// mapping target, and skipping the existence probe halves the request count.
func (ss *S3Storage) putIndexObject(ctx context.Context, key string, value []byte) error {
	return ss.putObject(ctx, key, bytes.NewReader(value))
}

// PutXorb stores an xorb. The stream is validated while being spooled to a
// temporary file, then uploaded with a known length so the SDK can sign and
// retry the request.
func (ss *S3Storage) PutXorb(ctx context.Context, _ string, xorbHash xet.XorbHash, r io.Reader) (bool, error) {
	key := ss.objectKey("xorbs", xorbHash.String())

	if _, exists, err := ss.headObject(ctx, key); err != nil {
		return false, fmt.Errorf("check xorb object: %w", err)
	} else if exists {
		return false, nil // Already exists
	}

	f, err := os.CreateTemp("", "xet-xorb-*")
	if err != nil {
		return false, fmt.Errorf("create xorb spool file: %w", err)
	}
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	if err := xorb.Validate(io.TeeReader(r, f), xorbHash); err != nil {
		return false, fmt.Errorf("validate xorb: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return false, fmt.Errorf("rewind xorb spool file: %w", err)
	}

	if err := ss.putObject(ctx, key, f); err != nil {
		return false, fmt.Errorf("upload xorb: %w", err)
	}
	return true, nil
}

// GetXorbReadSeekCloser returns a ReadSeekCloser over the xorb object; reads
// after a seek are served with S3 range requests.
func (ss *S3Storage) GetXorbReadSeekCloser(ctx context.Context, _ string, xorbHash xet.XorbHash) (io.ReadSeekCloser, error) {
	key := ss.objectKey("xorbs", xorbHash.String())
	size, exists, err := ss.headObject(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("check xorb object: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("xorb not found")
	}
	return &s3ReadSeekCloser{ctx: ctx, storage: ss, key: key, size: size}, nil
}

// HasXorb checks whether an xorb exists.
func (ss *S3Storage) HasXorb(ctx context.Context, _ string, xorbHash xet.XorbHash) (bool, error) {
	_, exists, err := ss.headObject(ctx, ss.objectKey("xorbs", xorbHash.String()))
	if err != nil {
		return false, fmt.Errorf("check xorb object: %w", err)
	}
	return exists, nil
}

// xorbChunkOffsets returns the cumulative packed end-offset of every chunk in
// the xorb, from the in-memory cache, the xorb footer, or a full scan for
// footer-less xorbs.
func (ss *S3Storage) xorbChunkOffsets(ctx context.Context, xorbHash xet.XorbHash) ([]uint64, error) {
	ss.offsetsMut.Lock()
	if v, ok := ss.offsetsIndex.Get(xorbHash); ok {
		ss.offsetsMut.Unlock()
		return v.([]uint64), nil
	}
	ss.offsetsMut.Unlock()

	f, err := ss.GetXorbReadSeekCloser(ctx, "", xorbHash)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	offsets, err := xorb.ReadChunkOffsets(f)
	if errors.Is(err, xorb.ErrNoFooter) {
		offsets, err = xorb.ScanChunkOffsets(f)
	}
	if err != nil {
		return nil, fmt.Errorf("read xorb chunk offsets: %w", err)
	}

	ss.offsetsMut.Lock()
	ss.offsetsIndex.Add(xorbHash, offsets)
	ss.offsetsMut.Unlock()
	return offsets, nil
}

// GetXorbDataRange returns the [start, end] byte range (inclusive) within
// the stored xorb binary for the given chunk range [chunkStart, chunkEnd).
func (ss *S3Storage) GetXorbDataRange(ctx context.Context, _ string, xorbHash xet.XorbHash, chunkStart, chunkEnd uint32) (startByte, endByte int64, err error) {
	offsets, err := ss.xorbChunkOffsets(ctx, xorbHash)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get chunk data range: %w", err)
	}
	return xorb.ChunkDataRangeFromOffsets(offsets, chunkStart, chunkEnd)
}

// hasFile checks whether a file hash already has a shard mapping.
func (ss *S3Storage) hasFile(ctx context.Context, fileHash xet.FileHash) (bool, error) {
	ss.fileMut.Lock()
	_, exists := ss.fileIndex.Get(fileHash)
	ss.fileMut.Unlock()
	if exists {
		return true, nil
	}

	_, exists, err := ss.headObject(ctx, ss.objectKey("files", fileHash.String()))
	if err != nil {
		return false, fmt.Errorf("check file index: %w", err)
	}
	return exists, nil
}

func (ss *S3Storage) computeFileSHA256(ctx context.Context, fileBlock *shard.FileBlock) ([32]byte, error) {
	if len(fileBlock.Entries) == 0 {
		return [32]byte{}, nil
	}

	h := sha256.New()
	buf := make([]byte, xet.MaxChunkSize)
	for _, entry := range fileBlock.Entries {
		if err := ctx.Err(); err != nil {
			return [32]byte{}, err
		}

		start, end, err := ss.GetXorbDataRange(ctx, "", entry.CASHash, entry.ChunkIndexStart, entry.ChunkIndexEnd)
		if err != nil {
			return [32]byte{}, fmt.Errorf("locate xorb chunks: %w", err)
		}
		rc, err := ss.getObjectRange(ctx, ss.objectKey("xorbs", entry.CASHash.String()), start, end)
		if err != nil {
			return [32]byte{}, fmt.Errorf("read xorb chunks: %w", err)
		}
		decoder := xorb.NewDecoder(rc, false)
		written, err := io.CopyBuffer(h, decoder, buf)
		rc.Close()
		if err != nil {
			return [32]byte{}, fmt.Errorf("decode xorb chunks: %w", err)
		}
		if written != int64(entry.UnpackedSegBytes) {
			return [32]byte{}, fmt.Errorf("reconstructed term has %d bytes, expected %d", written, entry.UnpackedSegBytes)
		}
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

// PutShard stores a shard and its file/chunk/sha256 index objects.
func (ss *S3Storage) PutShard(ctx context.Context, s *shard.Shard) (bool, error) {
	if len(s.Files) == 0 {
		return false, fmt.Errorf("shard has no file blocks")
	}

	// Check if any file in the shard already exists
	for _, fileBlock := range s.Files {
		exists, err := ss.hasFile(ctx, fileBlock.FileHash)
		if err != nil {
			return false, fmt.Errorf("check shard: %w", err)
		}
		if exists {
			return false, nil // Already exists
		}
	}

	for i := range s.Files {
		computed, err := ss.computeFileSHA256(ctx, &s.Files[i])
		if err != nil {
			return false, fmt.Errorf("compute SHA-256 for file %s: %w", s.Files[i].FileHash.String(), err)
		}
		if s.Files[i].MetadataExt != nil && s.Files[i].MetadataExt.SHA256Hash != shard.NewSHA256Hash(computed) {
			return false, fmt.Errorf("SHA-256 mismatch for file %s", s.Files[i].FileHash.String())
		}

		s.Files[i].MetadataExt = &shard.FileMetadataExt{SHA256Hash: shard.NewSHA256Hash(computed)}
		s.Files[i].Flags |= shard.FileWithMetadataExt
	}

	// Serialize the shard first, then hash those exact persisted bytes. The
	// shard object is addressed by this hash rather than by any contained file.
	r, err := s.Encode(true)
	if err != nil {
		return false, fmt.Errorf("serialize shard: %w", err)
	}
	var encoded bytes.Buffer
	if _, err := io.Copy(&encoded, r); err != nil {
		return false, fmt.Errorf("serialize shard: %w", err)
	}
	shardHash, err := computeShardHashFromReader(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		return false, fmt.Errorf("hash shard: %w", err)
	}

	shardKey := ss.objectKey("shards", shardHash)
	wasInserted := true
	if _, exists, err := ss.headObject(ctx, shardKey); err != nil {
		return false, fmt.Errorf("check shard object: %w", err)
	} else if exists {
		wasInserted = false
	} else if err := ss.putObject(ctx, shardKey, bytes.NewReader(encoded.Bytes())); err != nil {
		return false, fmt.Errorf("upload shard: %w", err)
	}

	// The files/ index is written last: hasFile treats it as the commit
	// marker, so a partial failure leaves a retryable shard instead of one
	// that reports "already exists" with missing chunk/sha256 indexes.
	shardHashData := []byte(shardHash)
	for _, casBlock := range s.CASInfos {
		for _, chunk := range casBlock.Chunks {
			if err := ss.putIndexObject(ctx, ss.objectKey("chunks", chunk.ChunkHash.String()), shardHashData); err != nil {
				return wasInserted, fmt.Errorf("write chunk index: %w", err)
			}
		}
	}
	for _, file := range s.Files {
		if err := ss.putIndexObject(ctx, ss.objectKey("sha256", file.MetadataExt.SHA256Hash.String()), []byte(file.FileHash.String())); err != nil {
			return wasInserted, fmt.Errorf("write SHA-256 index for file %s: %w", file.FileHash.String(), err)
		}
	}
	for _, file := range s.Files {
		if err := ss.putIndexObject(ctx, ss.objectKey("files", file.FileHash.String()), shardHashData); err != nil {
			return wasInserted, fmt.Errorf("write file index for file %s: %w", file.FileHash.String(), err)
		}
	}

	// Populate caches only after everything is persisted; caching earlier
	// would make a retry skip the objects that failed to write.
	ss.shardMut.Lock()
	ss.shardIndex.Add(shardHash, s)
	ss.shardMut.Unlock()
	for _, file := range s.Files {
		ss.fileMut.Lock()
		ss.fileIndex.Add(file.FileHash, shardHash)
		ss.fileMut.Unlock()
	}

	return wasInserted, nil
}

func (ss *S3Storage) getShardByHash(ctx context.Context, shardHash string) (*shard.Shard, error) {
	ss.shardMut.Lock()
	value, exists := ss.shardIndex.Get(shardHash)
	ss.shardMut.Unlock()
	if exists {
		return value.(*shard.Shard), nil
	}

	data, err := ss.getObject(ctx, ss.objectKey("shards", shardHash))
	if err != nil {
		return nil, err
	}
	s := shard.NewShard()
	if err := s.Decode(bytes.NewReader(data), true); err != nil {
		return nil, err
	}

	ss.shardMut.Lock()
	ss.shardIndex.Add(shardHash, s)
	ss.shardMut.Unlock()
	for _, fileBlock := range s.Files {
		ss.fileMut.Lock()
		ss.fileIndex.Add(fileBlock.FileHash, shardHash)
		ss.fileMut.Unlock()
	}
	return s, nil
}

// GetShard retrieves a shard by file hash
func (ss *S3Storage) GetShard(ctx context.Context, fileHash xet.FileHash) (*shard.Shard, error) {
	ss.fileMut.Lock()
	value, exists := ss.fileIndex.Get(fileHash)
	ss.fileMut.Unlock()
	if exists {
		return ss.getShardByHash(ctx, value.(string))
	}

	data, err := ss.getObject(ctx, ss.objectKey("files", fileHash.String()))
	if err != nil {
		return nil, fmt.Errorf("read file index: %w", err)
	}
	shardHash := strings.TrimSpace(string(data))
	ss.fileMut.Lock()
	ss.fileIndex.Add(fileHash, shardHash)
	ss.fileMut.Unlock()
	return ss.getShardByHash(ctx, shardHash)
}

// GetShardByChunkHash retrieves a shard by chunk hash (for deduplication)
func (ss *S3Storage) GetShardByChunkHash(ctx context.Context, _ string, chunkHash xet.ChunkHash) (*shard.Shard, error) {
	ss.chunkMut.Lock()
	value, exists := ss.chunkIndex.Get(chunkHash)
	ss.chunkMut.Unlock()
	if exists {
		return ss.getShardByHash(ctx, value.(string))
	}

	data, err := ss.getObject(ctx, ss.objectKey("chunks", chunkHash.String()))
	if err != nil {
		if isS3NotFound(err) {
			return nil, fmt.Errorf("chunk not found")
		}
		return nil, fmt.Errorf("read chunk index: %w", err)
	}
	shardHash := strings.TrimSpace(string(data))
	ss.chunkMut.Lock()
	ss.chunkIndex.Add(chunkHash, shardHash)
	ss.chunkMut.Unlock()
	return ss.getShardByHash(ctx, shardHash)
}

// GetFileHashBySHA256 resolves a SHA-256 digest to the xet file hash recorded
// at ingest.
func (ss *S3Storage) GetFileHashBySHA256(ctx context.Context, _ string, digest [32]byte) (xet.FileHash, error) {
	ss.sha256Mut.Lock()
	value, exists := ss.sha256Index.Get(digest)
	ss.sha256Mut.Unlock()
	if exists {
		return value.(xet.FileHash), nil
	}

	data, err := ss.getObject(ctx, ss.objectKey("sha256", hex.EncodeToString(digest[:])))
	if err != nil {
		if isS3NotFound(err) {
			return xet.FileHash{}, fmt.Errorf("SHA-256 not found")
		}
		return xet.FileHash{}, fmt.Errorf("read SHA-256 index: %w", err)
	}
	fileHash, err := xet.ParseFileHash(strings.TrimSpace(string(data)))
	if err != nil {
		return xet.FileHash{}, fmt.Errorf("invalid SHA-256 index: %w", err)
	}
	ss.sha256Mut.Lock()
	ss.sha256Index.Add(digest, fileHash)
	ss.sha256Mut.Unlock()
	return fileHash, nil
}

func (ss *S3Storage) GetReconstructedFile(ctx context.Context, namespace string, sha256 [32]byte) (io.ReadSeekCloser, error) {
	fileHash, err := ss.GetFileHashBySHA256(ctx, namespace, sha256)
	if err != nil {
		return nil, fmt.Errorf("get file hash by sha256: %w", err)
	}
	sh, err := ss.GetShard(ctx, fileHash)
	if err != nil {
		return nil, fmt.Errorf("get shard by sha256: %w", err)
	}
	return newReconstructedFile(ctx, ss, namespace, sh, sha256)
}

// GetXorbURL generates a URL for accessing xorb data. By default it is a
// presigned S3 GET URL so clients fetch xorb ranges straight from the object
// store; with presigning disabled the URL routes through the CAS server's
// xorb endpoint like FileStorage.
func (ss *S3Storage) GetXorbURL(namespace string, xorbHash xet.XorbHash) (string, error) {
	if ss.presign {
		req, err := ss.presignClient.PresignGetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(ss.bucket),
			Key:    aws.String(ss.objectKey("xorbs", xorbHash.String())),
		}, s3.WithPresignExpires(ss.presignExpiry))
		if err != nil {
			return "", fmt.Errorf("presign xorb URL: %w", err)
		}
		return req.URL, nil
	}
	if ss.baseURL == "" {
		// If no base URL is configured, return a relative path
		return fmt.Sprintf("/v1/xorbs/%s/%s", namespace, xorbHash.String()), nil
	}
	return fmt.Sprintf("%s/v1/xorbs/%s/%s", ss.baseURL, namespace, xorbHash.String()), nil
}

// s3ReadSeekCloser adapts an S3 object to io.ReadSeekCloser. Seeks are lazy:
// a range request is only issued when a read needs data at a new position.
type s3ReadSeekCloser struct {
	ctx     context.Context
	storage *S3Storage
	key     string
	size    int64

	pos     int64
	body    io.ReadCloser
	bodyPos int64
	closed  bool
}

func (r *s3ReadSeekCloser) Read(p []byte) (int, error) {
	if r.closed {
		return 0, fmt.Errorf("read s3 object: closed")
	}
	if r.pos >= r.size {
		return 0, io.EOF
	}
	if r.body != nil && r.bodyPos != r.pos {
		r.body.Close()
		r.body = nil
	}
	if r.body == nil {
		body, err := r.storage.getObjectRange(r.ctx, r.key, r.pos, r.size-1)
		if err != nil {
			return 0, fmt.Errorf("read s3 object range: %w", err)
		}
		r.body = body
		r.bodyPos = r.pos
	}
	n, err := r.body.Read(p)
	r.pos += int64(n)
	r.bodyPos = r.pos
	if err == io.EOF && r.pos < r.size {
		// The range stream ended early; drop it so the next read reopens.
		r.body.Close()
		r.body = nil
		if n > 0 {
			return n, nil
		}
		return 0, io.ErrUnexpectedEOF
	}
	return n, err
}

func (r *s3ReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	if r.closed {
		return 0, fmt.Errorf("seek s3 object: closed")
	}
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = r.pos + offset
	case io.SeekEnd:
		next = r.size + offset
	default:
		return 0, fmt.Errorf("invalid seek whence %d", whence)
	}
	if next < 0 {
		return 0, fmt.Errorf("negative seek position %d", next)
	}
	r.pos = next
	return next, nil
}

func (r *s3ReadSeekCloser) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.body != nil {
		err := r.body.Close()
		r.body = nil
		return err
	}
	return nil
}

var _ Storage = (*S3Storage)(nil)
