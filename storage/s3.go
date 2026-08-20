package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
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
	"github.com/wzshiming/httpseek"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
	"golang.org/x/sync/errgroup"
)

// defaultPresignExpiry keeps presigned xorb URLs valid long enough for
// clients to work through a large reconstruction term by term.
const defaultPresignExpiry = time.Hour

// S3Storage implements Storage backed by an S3-compatible object store. It
// uses the same object layout as FileStorage (xorbs/, shards/, index/files/,
// index/chunks/, index/sha256/ with a two-character fanout), so a bucket
// populated by syncing a FileStorage directory is directly usable.
type S3Storage struct {
	client          *s3.Client
	presignClient   *s3.PresignClient
	bucket          string
	prefix          string
	baseURL         string
	presign         bool
	presignExpiry   time.Duration
	presignEndpoint string

	fileIndex      *lru.Cache // bounded file hash -> shard hash
	shardIndex     *lru.Cache // bounded shard hash -> shard cache
	chunkIndex     *lru.Cache // bounded chunk hash -> shard hash
	sha256Index    *lru.Cache // bounded SHA-256 -> shard hash
	offsetsIndex   *lru.Cache // bounded xorb hash -> []uint64 packed chunk end-offsets
	validatedIndex *lru.Cache // bounded xorb hash -> content-validated marker

	fileMut      sync.Mutex // guards fileIndex
	shardMut     sync.Mutex // guards shardIndex
	chunkMut     sync.Mutex // guards chunkIndex
	sha256Mut    sync.Mutex // guards sha256Index
	offsetsMut   sync.Mutex // guards offsetsIndex
	validatedMut sync.Mutex // guards validatedIndex

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

// WithS3Presign controls whether xorb transfers go straight to the object
// store (default): downloads use presigned S3 GET URLs and uploads from
// clients that advertise direct-upload support are redirected to presigned
// S3 PUT URLs, verified at shard upload time. When disabled, transfers
// stream through the CAS server; use this when clients cannot reach the S3
// endpoint directly.
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
		presign:        true,
		presignExpiry:  defaultPresignExpiry,
		fileIndex:      lru.New(defaultFileCacheSize),
		shardIndex:     lru.New(defaultShardCacheSize),
		chunkIndex:     lru.New(defaultChunkCacheSize),
		sha256Index:    lru.New(defaultSHA256CacheSize),
		offsetsIndex:   lru.New(defaultOffsetsCacheSize),
		validatedIndex: lru.New(defaultValidatedCacheSize),
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
	// The presign client never sends requests; it only shapes and signs
	// URLs, so a client copy is enough. Checksum calculation must be
	// when_required: the default folds x-amz-checksum-* headers into
	// presigned PUT signatures, which plain HTTP clients and most
	// S3-compatible stores do not handle.
	presignOpts := ss.client.Options()
	if ss.presignEndpoint != "" {
		// Presign against the public endpoint clients actually reach.
		presignOpts.BaseEndpoint = aws.String(ss.presignEndpoint)
	}
	presignOpts.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	ss.presignClient = s3.NewPresignClient(s3.New(presignOpts))

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
	body, err := ss.getObjectReader(ctx, key)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	return io.ReadAll(body)
}

// getObjectReader returns the object body as a stream; the caller must close it.
func (ss *S3Storage) getObjectReader(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := ss.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(ss.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
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
	ss.markXorbValidated(xorbHash)
	return true, nil
}

// xorbValidated reports whether this process already validated the xorb's
// content, via PutXorb or an earlier ValidateXorb.
func (ss *S3Storage) xorbValidated(xorbHash xet.XorbHash) bool {
	ss.validatedMut.Lock()
	defer ss.validatedMut.Unlock()
	_, ok := ss.validatedIndex.Get(xorbHash)
	return ok
}

func (ss *S3Storage) markXorbValidated(xorbHash xet.XorbHash) {
	ss.validatedMut.Lock()
	defer ss.validatedMut.Unlock()
	ss.validatedIndex.Add(xorbHash, struct{}{})
}

// errTrackingReader records the first error the underlying reader returns so
// a failed validation can be told apart from a failed transfer.
type errTrackingReader struct {
	r   io.Reader
	err error
}

func (t *errTrackingReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if err != nil && err != io.EOF && t.err == nil {
		t.err = err
	}
	return n, err
}

// ValidateXorb streams the stored xorb back from S3 through full content
// validation, covering xorbs uploaded directly with presigned PUT URLs.
// With presigning disabled there is no direct-upload ingress (every object
// passed PutXorb validation), so only existence is checked. Hashes already
// validated by this process are skipped, so shard retries do not re-read
// the bytes. Invalid content is deleted: it has never validated, so nothing
// references it, and leaving it would poison the key (existence checks make
// clients skip re-uploading the valid bytes).
func (ss *S3Storage) ValidateXorb(ctx context.Context, _ string, xorbHash xet.XorbHash) error {
	if ss.xorbValidated(xorbHash) {
		return nil
	}

	key := ss.objectKey("xorbs", xorbHash.String())
	if !ss.presign {
		_, exists, err := ss.headObject(ctx, key)
		if err != nil {
			return fmt.Errorf("check xorb object: %w", err)
		}
		if !exists {
			return ErrXorbNotFound
		}
		return nil
	}

	out, err := ss.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(ss.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isS3NotFound(err) {
			return ErrXorbNotFound
		}
		return fmt.Errorf("read xorb object: %w", err)
	}
	defer out.Body.Close()

	body := &errTrackingReader{r: out.Body}
	if err := xorb.Validate(body, xorbHash); err != nil {
		// A failed read is a transport problem, not proof of bad content;
		// report it as retryable instead of rejecting the shard for good.
		if body.err != nil {
			return fmt.Errorf("read xorb object: %w", err)
		}
		ss.deleteObjectIfMatch(ctx, key, aws.ToString(out.ETag))
		return fmt.Errorf("%w: %v", ErrXorbInvalid, err)
	}

	ss.markXorbValidated(xorbHash)
	return nil
}

// deleteObjectIfMatch best-effort deletes an object; the ETag condition keeps
// a concurrent valid re-upload from being deleted along with the bad bytes.
// Stores without conditional-delete support get an unconditional retry:
// leaving the object would poison the key for good, while the delete race is
// only the narrow window after another deletion of the same bad bytes.
func (ss *S3Storage) deleteObjectIfMatch(ctx context.Context, key, etag string) {
	in := &s3.DeleteObjectInput{
		Bucket: aws.String(ss.bucket),
		Key:    aws.String(key),
	}
	if etag != "" {
		in.IfMatch = aws.String(etag)
	}
	_, err := ss.client.DeleteObject(ctx, in)
	if err == nil || in.IfMatch == nil || isS3PreconditionFailed(err) {
		return
	}
	in.IfMatch = nil
	_, _ = ss.client.DeleteObject(ctx, in)
}

// isS3PreconditionFailed reports a failed If-Match/If-None-Match condition:
// the object changed, as opposed to the store rejecting the conditional
// request itself.
func isS3PreconditionFailed(err error) bool {
	var respErr *awshttp.ResponseError
	return errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusPreconditionFailed
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
	return httpseek.NewOpenSeeker(&s3Opener{ctx: ctx, storage: ss, key: key, size: size}), nil
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

// GetXorbChunkOffsets returns the xorb's chunk offset table; cached after
// the first read.
func (ss *S3Storage) GetXorbChunkOffsets(ctx context.Context, xorbHash xet.XorbHash) ([]uint64, error) {
	return ss.xorbChunkOffsets(ctx, xorbHash)
}

// hasFile checks whether a file hash already has a shard mapping.
func (ss *S3Storage) hasFile(ctx context.Context, fileHash xet.FileHash) (bool, error) {
	ss.fileMut.Lock()
	_, exists := ss.fileIndex.Get(fileHash)
	ss.fileMut.Unlock()
	if exists {
		return true, nil
	}

	_, exists, err := ss.headObject(ctx, ss.objectKey("index/files", fileHash.String()))
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

	// Serialize without the footer (it embeds a creation timestamp), so the
	// stored bytes — and the sha256 hash addressing them — are deterministic
	// for identical shard content.
	r, err := s.Encode(false)
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

	// The index/files/ index is written last: hasFile treats it as the commit
	// marker, so a partial failure leaves a retryable shard instead of one
	// that reports "already exists" with missing chunk/sha256 indexes. Nothing
	// is cached here; the read path populates the caches from the stored
	// objects, so a warm process serves exactly what a restarted one would.
	shardHashData := []byte(shardHash)
	for _, casBlock := range s.CASInfos {
		for _, chunk := range casBlock.Chunks {
			if err := ss.putIndexObject(ctx, ss.objectKey("index/chunks", chunk.ChunkHash.String()), shardHashData); err != nil {
				return wasInserted, fmt.Errorf("write chunk index: %w", err)
			}
		}
	}
	for _, file := range s.Files {
		if err := ss.putIndexObject(ctx, ss.objectKey("index/sha256", file.MetadataExt.SHA256Hash.String()), shardHashData); err != nil {
			return wasInserted, fmt.Errorf("write SHA-256 index for file %s: %w", file.FileHash.String(), err)
		}
	}
	for _, file := range s.Files {
		if err := ss.putIndexObject(ctx, ss.objectKey("index/files", file.FileHash.String()), shardHashData); err != nil {
			return wasInserted, fmt.Errorf("write file index for file %s: %w", file.FileHash.String(), err)
		}
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

	out, err := ss.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(ss.bucket),
		Key:    aws.String(ss.objectKey("shards", shardHash)),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	s, err := decodeStoredShard(out.Body)
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

// GetShardByHash loads a stored shard by the hash of its serialized bytes.
// The returned error wraps fs.ErrNotExist when the shard is absent.
func (ss *S3Storage) GetShardByHash(ctx context.Context, shardHash string) (*shard.Shard, error) {
	s, err := ss.getShardByHash(ctx, shardHash)
	if err != nil && isS3NotFound(err) {
		return nil, fmt.Errorf("shard %s: %w", shardHash, iofs.ErrNotExist)
	}
	return s, err
}

// fileIndexWalkConcurrency bounds parallel index-object reads during walks.
const fileIndexWalkConcurrency = 4

// WalkFileIndex calls fn for every committed index/files entry.
func (ss *S3Storage) WalkFileIndex(ctx context.Context, fn func(fileHash, shardHash string) error) error {
	base := "index/files/"
	if ss.prefix != "" {
		base = ss.prefix + "/" + base
	}
	paginator := s3.NewListObjectsV2Paginator(ss.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(ss.bucket),
		Prefix: aws.String(base),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list file index: %w", err)
		}
		var keys, hashes []string
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			fileHash := strings.ReplaceAll(strings.TrimPrefix(key, base), "/", "")
			if len(fileHash) != 64 {
				continue
			}
			if _, err := hex.DecodeString(fileHash); err != nil {
				continue
			}
			keys = append(keys, key)
			hashes = append(hashes, fileHash)
		}
		// Read the page's index objects in parallel; fn stays sequential.
		bodies := make([][]byte, len(keys))
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(fileIndexWalkConcurrency)
		for i, key := range keys {
			g.Go(func() error {
				data, err := ss.getObject(gctx, key)
				if err != nil {
					// Deleted between list and read: leave the slot nil.
					if isS3NotFound(err) {
						return nil
					}
					return fmt.Errorf("read file index %s: %w", key, err)
				}
				bodies[i] = data
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
		for i, fileHash := range hashes {
			if bodies[i] == nil {
				continue
			}
			if err := fn(fileHash, strings.TrimSpace(string(bodies[i]))); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetShard retrieves a shard by file hash
func (ss *S3Storage) GetShard(ctx context.Context, fileHash xet.FileHash) (*shard.Shard, error) {
	ss.fileMut.Lock()
	value, exists := ss.fileIndex.Get(fileHash)
	ss.fileMut.Unlock()
	if exists {
		return ss.getShardByHash(ctx, value.(string))
	}

	data, err := ss.getObject(ctx, ss.objectKey("index/files", fileHash.String()))
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

	data, err := ss.getObject(ctx, ss.objectKey("index/chunks", chunkHash.String()))
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
// at ingest, loading the owning shard and matching its file metadata.
func (ss *S3Storage) GetFileHashBySHA256(ctx context.Context, _ string, digest [32]byte) (xet.FileHash, error) {
	sh, err := ss.getShardBySHA256(ctx, digest)
	if err != nil {
		return xet.FileHash{}, err
	}
	file := findFileBySHA256(sh, digest)
	if file == nil {
		return xet.FileHash{}, fmt.Errorf("SHA-256 is not present in shard")
	}
	return file.FileHash, nil
}

// getShardBySHA256 resolves a SHA-256 digest through index/sha256/<digest>,
// whose contents are the hash of the owning shard, reading the index through
// the bounded cache.
func (ss *S3Storage) getShardBySHA256(ctx context.Context, digest [32]byte) (*shard.Shard, error) {
	ss.sha256Mut.Lock()
	value, exists := ss.sha256Index.Get(digest)
	ss.sha256Mut.Unlock()
	if exists {
		return ss.getShardByHash(ctx, value.(string))
	}

	data, err := ss.getObject(ctx, ss.objectKey("index/sha256", hex.EncodeToString(digest[:])))
	if err != nil {
		if isS3NotFound(err) {
			return nil, fmt.Errorf("SHA-256 not found")
		}
		return nil, fmt.Errorf("read SHA-256 index: %w", err)
	}
	shardHash := strings.TrimSpace(string(data))
	ss.sha256Mut.Lock()
	ss.sha256Index.Add(digest, shardHash)
	ss.sha256Mut.Unlock()
	return ss.getShardByHash(ctx, shardHash)
}

func (ss *S3Storage) GetReconstructedFile(ctx context.Context, namespace string, sha256 [32]byte) (io.ReadSeekCloser, error) {
	sh, err := ss.getShardBySHA256(ctx, sha256)
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

// XorbUploadURL returns a presigned PUT URL so clients upload xorb bytes
// straight to the object store, or "" when presigning is disabled. Bytes
// accepted this way are unverified until ValidateXorb runs at shard time.
// Content-Length and If-None-Match: * are folded into the signature, so the
// URL creates an object of exactly size bytes and cannot overwrite an
// existing one (e.g. a replayed URL after the xorb was committed).
func (ss *S3Storage) XorbUploadURL(ctx context.Context, _ string, xorbHash xet.XorbHash, size int64) (string, error) {
	if !ss.presign {
		return "", nil
	}
	req, err := ss.presignClient.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(ss.bucket),
		Key:           aws.String(ss.objectKey("xorbs", xorbHash.String())),
		ContentLength: aws.Int64(size),
		IfNoneMatch:   aws.String("*"),
	}, s3.WithPresignExpires(ss.presignExpiry))
	if err != nil {
		return "", fmt.Errorf("presign xorb upload URL: %w", err)
	}
	return req.URL, nil
}

// s3Opener adapts an S3 object to httpseek.Opener: each open is served with
// an S3 range GET, and the ETag reported on every open lets the OpenSeeker
// detect content changes across reopens.
type s3Opener struct {
	ctx     context.Context
	storage *S3Storage
	key     string
	size    int64 // total object size from the open-time HEAD probe
}

var _ httpseek.Opener = (*s3Opener)(nil)

func (o *s3Opener) OpenRange(_ string, start, end int64) (httpseek.OpenResult, error) {
	if start < 0 {
		// Suffix of length end, resolved against the known size.
		start = max(o.size-end, 0)
		end = o.size - 1
	}
	res := httpseek.OpenResult{Start: start, End: end, Size: o.size}

	in := &s3.GetObjectInput{
		Bucket: aws.String(o.storage.bucket),
		Key:    aws.String(o.key),
	}
	switch {
	case end >= 0:
		in.Range = aws.String(fmt.Sprintf("bytes=%d-%d", start, end))
	case start > 0:
		in.Range = aws.String(fmt.Sprintf("bytes=%d-", start))
	}
	out, err := o.storage.client.GetObject(o.ctx, in)
	if err != nil {
		var respErr *awshttp.ResponseError
		if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusRequestedRangeNotSatisfiable {
			return res, httpseek.ErrRangeNotSatisfiable
		}
		return res, err
	}
	res.Body = out.Body
	res.Validator = aws.ToString(out.ETag)
	return res, nil
}

// Size reports the size learned by the HEAD probe at construction time.
func (o *s3Opener) Size(string) (httpseek.SizeResult, error) {
	return httpseek.SizeResult{Size: o.size}, nil
}

var _ Storage = (*S3Storage)(nil)
var _ XorbUploadRedirector = (*S3Storage)(nil)
var _ XorbValidator = (*S3Storage)(nil)
