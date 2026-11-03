package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/crc64nvme"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/storagetest"
)

// httpClientFunc stands in for the S3 client's HTTP client.
type httpClientFunc func(*http.Request) (*http.Response, error)

func (f httpClientFunc) Do(req *http.Request) (*http.Response, error) { return f(req) }

// newTestS3StorageVia is newTestS3Storage with every S3 API request sent through do.
func newTestS3StorageVia(t *testing.T, do httpClientFunc) *Storage {
	t.Helper()
	base := newTestS3Storage(t)
	opts := base.client.Options()
	opts.HTTPClient = do
	ss, err := NewStorage(context.Background(), WithCacheDir(t.TempDir()), WithS3Client(awss3.New(opts)), WithBucket(base.bucket))
	if err != nil {
		t.Fatal(err)
	}
	return ss
}

// putGranted sends body as xet-core does: the grant's method and URL with exactly
// its headers plus Content-Length, and no credentials.
func putGranted(t *testing.T, grant *storage.XorbUploadGrant, body []byte) int {
	t.Helper()
	req, err := http.NewRequest(grant.Method, grant.URL, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range grant.Headers {
		req.Header.Set(name, value)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("direct upload: %v", err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// stageObject stores body as the staged upload of a fresh grant for xorbHash,
// the object either upload path leaves for the commit.
func stageObject(t *testing.T, ss *Storage, xorbHash xet.XorbHash, body []byte) string {
	t.Helper()
	ctx := context.Background()
	grant, err := ss.GrantXorbUpload(ctx, xorbHash, int64(len(body)), crc64nvme.Checksum(body))
	if err != nil {
		t.Fatalf("GrantXorbUpload: %v", err)
	}
	if err := ss.putObject(ctx, ss.objectKey("staging", grant.ID), bytes.NewReader(body)); err != nil {
		t.Fatalf("store the staged upload: %v", err)
	}
	return grant.ID
}

// stagedIDs returns the grant ids WalkStagedXorbs lists.
func stagedIDs(t *testing.T, ss *Storage) []string {
	t.Helper()
	var ids []string
	if err := ss.WalkStagedXorbs(context.Background(), func(id string, _ int64, _ time.Time) error {
		ids = append(ids, id)
		return nil
	}); err != nil {
		t.Fatalf("WalkStagedXorbs: %v", err)
	}
	return ids
}

// s3Op is the object operation of one S3 request; source is set for a copy.
type s3Op struct {
	method, key, source string
}

// s3OpOf reads the operation of a path-style request to test-bucket.
func s3OpOf(req *http.Request) s3Op {
	op := s3Op{method: req.Method, key: strings.TrimPrefix(req.URL.Path, "/test-bucket/")}
	if source := req.Header.Get("X-Amz-Copy-Source"); source != "" {
		if unescaped, err := url.PathUnescape(source); err == nil {
			source = unescaped
		}
		op.method, op.source = "COPY", strings.TrimPrefix(strings.TrimPrefix(source, "/"), "test-bucket/")
	}
	return op
}

func (op s3Op) String() string {
	if op.method == "COPY" {
		return "COPY " + op.source + " -> " + op.key
	}
	return op.method + " " + op.key
}

// s3Trace logs the operations sent through forward since its last reset.
type s3Trace struct {
	mu  sync.Mutex
	ops []string
}

func (tr *s3Trace) forward(req *http.Request) (*http.Response, error) {
	tr.mu.Lock()
	tr.ops = append(tr.ops, s3OpOf(req).String())
	tr.mu.Unlock()
	return http.DefaultClient.Do(req)
}

func (tr *s3Trace) reset() {
	tr.mu.Lock()
	tr.ops = nil
	tr.mu.Unlock()
}

func (tr *s3Trace) sent() []string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return slices.Clone(tr.ops)
}

// stagingID returns the grant id naming a staging key.
func stagingID(key string) string {
	return strings.ReplaceAll(strings.TrimPrefix(key, "staging/"), "/", "")
}

// privateKey returns the key the first copy in ops wrote, failing unless it
// copied stagedKey to a fresh staging key.
func privateKey(t *testing.T, ss *Storage, ops []string, stagedKey string) string {
	t.Helper()
	for _, op := range ops {
		if !strings.HasPrefix(op, "COPY ") {
			continue
		}
		key, ok := strings.CutPrefix(op, "COPY "+stagedKey+" -> ")
		if id := stagingID(key); !ok || key == stagedKey || !storage.ValidGrantID(id) || key != ss.objectKey("staging", id) {
			t.Fatalf("first copy of the commit = %s, want %s copied to a fresh staging key", op, stagedKey)
		}
		return key
	}
	t.Fatalf("commit sent %v, want %s copied to a fresh staging key", ops, stagedKey)
	return ""
}

// A presigned grant is a PutObject of the staging key at the presign endpoint
// that signs the checksum as a header, not as a query parameter.
func TestS3PresignedGrantURL(t *testing.T) {
	ctx := context.Background()
	const expiry = 90 * time.Second
	const signedHeaders = "content-length;host;if-none-match;x-amz-checksum-crc64nvme"
	data, xorbHash := storagetest.EncodeXorb(t, true, []byte("presigned upload"))
	internal := newTestS3Storage(t, WithPrefix("some/prefix"), WithPresignExpiry(expiry))
	public := newTestS3Storage(t, WithPresignExpiry(expiry), WithPresignEndpoint("http://public.example:9000"))

	for _, c := range []struct {
		ss       *Storage
		endpoint string
	}{
		{internal, aws.ToString(internal.client.Options().BaseEndpoint)},
		{public, "http://public.example:9000"},
	} {
		grant, err := c.ss.GrantXorbUpload(ctx, xorbHash, int64(len(data)), crc64nvme.Checksum(data))
		if err != nil {
			t.Fatalf("GrantXorbUpload: %v", err)
		}
		u, err := url.Parse(grant.URL)
		if err != nil {
			t.Fatalf("grant URL %q: %v", grant.URL, err)
		}
		key := c.ss.objectKey("staging", grant.ID)
		if got := u.Scheme + "://" + u.Host; got != c.endpoint || !strings.HasSuffix(u.Path, "/"+key) {
			t.Fatalf("grant URL = %s, want %s/.../%s", grant.URL, c.endpoint, key)
		}
		q := u.Query()
		if got := q.Get("X-Amz-Expires"); got != "90" {
			t.Fatalf("X-Amz-Expires = %q, want the presign expiry, 90", got)
		}
		if got := q.Get("X-Amz-SignedHeaders"); got != signedHeaders {
			t.Fatalf("X-Amz-SignedHeaders = %q, want %q", got, signedHeaders)
		}
		for _, name := range []string{"X-Amz-Signature", "X-Amz-Credential", "X-Amz-Date", "X-Amz-Algorithm"} {
			if q.Get(name) == "" {
				t.Fatalf("grant URL %s lacks %s", grant.URL, name)
			}
		}
		for name := range q {
			if strings.Contains(strings.ToLower(name), "checksum") {
				t.Fatalf("grant URL query carries %s, want the checksum signed as a header", name)
			}
		}
	}
}

// S3 accepts presigned URLs valid for at least a whole second, so a shorter
// expiry presigns upload grants for one.
func TestS3PresignExpiryClamped(t *testing.T) {
	ss := newTestS3Storage(t, WithPresignExpiry(500*time.Millisecond))
	grant, err := ss.GrantXorbUpload(context.Background(), xet.XorbHash{1}, 8, 0)
	if err != nil {
		t.Fatalf("GrantXorbUpload: %v", err)
	}
	u, err := url.Parse(grant.URL)
	if err != nil {
		t.Fatalf("grant URL %q: %v", grant.URL, err)
	}
	if got := u.Query().Get("X-Amz-Expires"); got != "1" {
		t.Fatalf("X-Amz-Expires = %q with a 500ms presign expiry, want 1", got)
	}
}

// Download URLs get the same one-second floor as upload grants.
func TestS3DownloadPresignExpiryClamped(t *testing.T) {
	ss := newTestS3Storage(t, WithPresignExpiry(500*time.Millisecond))
	raw, err := ss.GetXorbURL(context.Background(), xet.XorbHash{1})
	if err != nil {
		t.Fatalf("GetXorbURL: %v", err)
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("download URL %q: %v", raw, err)
	}
	if got := u.Query().Get("X-Amz-Expires"); got != "1" {
		t.Fatalf("download URL X-Amz-Expires = %q with a 500ms presign expiry, want 1", got)
	}
}

// A presigned grant's headers are exactly those its signature covers besides the
// host, and every grant gets its own id.
func TestS3PresignedGrantHeaders(t *testing.T) {
	ctx := context.Background()
	ss := newTestS3Storage(t)
	data, xorbHash := storagetest.EncodeXorb(t, true, []byte("presigned upload"))
	checksum := crc64nvme.Checksum(data)
	want := map[string]string{
		"content-length":           strconv.Itoa(len(data)),
		"if-none-match":            "*",
		"x-amz-checksum-crc64nvme": crc64nvme.Base64(checksum),
	}

	seen := map[string]bool{}
	for range 2 {
		grant, err := ss.GrantXorbUpload(ctx, xorbHash, int64(len(data)), checksum)
		if err != nil {
			t.Fatalf("GrantXorbUpload: %v", err)
		}
		if grant.Method != http.MethodPut || !maps.Equal(grant.Headers, want) {
			t.Fatalf("grant = %s with headers %v, want PUT with %v", grant.Method, grant.Headers, want)
		}
		if !storage.ValidGrantID(grant.ID) || seen[grant.ID] {
			t.Fatalf("grant id = %q, want 32 lowercase hex characters distinct per grant", grant.ID)
		}
		seen[grant.ID] = true
	}
}

// With presign off a grant still routes the upload through the CAS server.
func TestS3RoutedGrant(t *testing.T) {
	ss := newTestS3Storage(t, WithPresign(false), WithBaseURL("http://cas.example"))
	grant, err := ss.GrantXorbUpload(context.Background(), xet.XorbHash{1}, 8, 0)
	if err != nil {
		t.Fatalf("GrantXorbUpload: %v", err)
	}
	if want := "http://cas.example" + storage.XorbUploadPath(grant.ID); grant.Method != http.MethodPut || grant.URL != want {
		t.Fatalf("grant = %s %s, want PUT %s", grant.Method, grant.URL, want)
	}
	if grant.Headers == nil || len(grant.Headers) != 0 {
		t.Fatalf("grant headers = %#v, want a non-nil empty map", grant.Headers)
	}
}

// With presign on the CAS server keeps no grants, so its upload route refuses
// even an issued id.
func TestS3PresignedGrantRefusesRoutedUpload(t *testing.T) {
	ctx := context.Background()
	ss := newTestS3Storage(t)
	data, xorbHash := storagetest.EncodeXorb(t, true, []byte("meant for the store"))
	grant, err := ss.GrantXorbUpload(ctx, xorbHash, int64(len(data)), crc64nvme.Checksum(data))
	if err != nil {
		t.Fatalf("GrantXorbUpload: %v", err)
	}

	if err := ss.PutStagedXorb(ctx, grant.ID, bytes.NewReader(data)); !errors.Is(err, storage.ErrUploadGrantNotFound) {
		t.Fatalf("PutStagedXorb(issued id) with presign on = %v, want ErrUploadGrantNotFound", err)
	}
	if ids := stagedIDs(t, ss); len(ids) != 0 {
		t.Fatalf("staged = %v, want nothing", ids)
	}
}

// A grant presigns with the caller's context, so a cancelled one fails the
// grant at the credentials lookup.
func TestS3PresignedGrantFailsOnCancelledContext(t *testing.T) {
	type ctxKey struct{}
	var seen context.Context
	client := awss3.New(awss3.Options{
		Region: "us-east-1",
		Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			seen = ctx
			if err := ctx.Err(); err != nil {
				return aws.Credentials{}, err
			}
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
	})
	ss, err := NewStorage(context.Background(), WithCacheDir(t.TempDir()), WithS3Client(client), WithBucket("test-bucket"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), ctxKey{}, "grant"))
	cancel()

	if grant, err := ss.GrantXorbUpload(ctx, xet.XorbHash{1}, 8, 0); grant != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("GrantXorbUpload(cancelled ctx) = %+v, %v; want no grant and an error wrapping context.Canceled", grant, err)
	}
	if seen == nil || seen.Value(ctxKey{}) != "grant" {
		t.Fatalf("presign resolved credentials with %v, want the caller's context", seen)
	}
}

// An upload sent straight to the store with the grant's headers lands once, and
// its commit publishes it and consumes the staged object.
func TestS3DirectUploadCommits(t *testing.T) {
	ctx := context.Background()
	ss := newTestS3Storage(t)
	data, xorbHash := storagetest.EncodeXorb(t, true, []byte("uploaded straight to the store"))
	grant, err := ss.GrantXorbUpload(ctx, xorbHash, int64(len(data)), crc64nvme.Checksum(data))
	if err != nil {
		t.Fatalf("GrantXorbUpload: %v", err)
	}

	for attempt, want := range []int{http.StatusOK, http.StatusPreconditionFailed} {
		if got := putGranted(t, grant, data); got != want {
			t.Fatalf("direct upload attempt %d = %d, want %d", attempt+1, got, want)
		}
	}
	if published, err := ss.CommitStagedXorb(ctx, grant.ID, xorbHash); err != nil || !published {
		t.Fatalf("CommitStagedXorb = %v, %v; want true, nil", published, err)
	}
	if ids := stagedIDs(t, ss); len(ids) != 0 {
		t.Fatalf("staged after the commit = %v, want nothing", ids)
	}
	if _, err := ss.CommitStagedXorb(ctx, grant.ID, xorbHash); !errors.Is(err, storage.ErrStagedXorbNotFound) {
		t.Fatalf("second CommitStagedXorb = %v, want ErrStagedXorbNotFound", err)
	}
}

// A staged upload swept between the commit's read and its copy is not found,
// and nothing is published.
func TestS3CommitStagedXorbSweptBeforeCopy(t *testing.T) {
	ctx := context.Background()
	var ss *Storage
	var xorbKey string
	ss = newTestS3StorageVia(t, func(req *http.Request) (*http.Response, error) {
		// The copy publishing the xorb takes the object the commit read.
		if op := s3OpOf(req); op.method == "COPY" && op.key == xorbKey {
			if err := ss.DeleteStagedXorb(req.Context(), stagingID(op.source)); err != nil {
				return nil, err
			}
		}
		return http.DefaultClient.Do(req)
	})
	data, xorbHash := storagetest.EncodeXorb(t, true, []byte("swept before its copy"))
	xorbKey = ss.objectKey("xorbs", xorbHash.String())
	grantID := stageObject(t, ss, xorbHash, data)

	if published, err := ss.CommitStagedXorb(ctx, grantID, xorbHash); published || !errors.Is(err, storage.ErrStagedXorbNotFound) {
		t.Fatalf("CommitStagedXorb(swept before the copy) = %v, %v; want false, ErrStagedXorbNotFound", published, err)
	}
	if ok, err := ss.HasXorb(ctx, xorbHash); err != nil || ok {
		t.Fatalf("HasXorb = %v, %v; want absent", ok, err)
	}
}

// A commit validates and publishes a private copy of the staged upload, never
// the staged key a presigned URL may still write, and leaves neither behind.
func TestS3CommitValidatesPrivateIntermediate(t *testing.T) {
	ctx := context.Background()
	var trace s3Trace
	ss := newTestS3StorageVia(t, trace.forward)
	data, xorbHash := storagetest.EncodeXorb(t, true, []byte("validated as a private copy"))
	grantID := stageObject(t, ss, xorbHash, data)
	stagedKey, xorbKey := ss.objectKey("staging", grantID), ss.objectKey("xorbs", xorbHash.String())

	trace.reset()
	if published, err := ss.CommitStagedXorb(ctx, grantID, xorbHash); err != nil || !published {
		t.Fatalf("CommitStagedXorb = %v, %v; want true, nil", published, err)
	}
	ops := trace.sent()
	private := privateKey(t, ss, ops, stagedKey)
	copyAt := slices.Index(ops, "COPY "+stagedKey+" -> "+private)
	readAt := slices.Index(ops, "GET "+private)
	publishAt := slices.Index(ops, "COPY "+private+" -> "+xorbKey)
	if slices.Contains(ops, "GET "+stagedKey) || !(copyAt < readAt && readAt < publishAt) {
		t.Fatalf("commit sent %v, want %s copied to %s, only the copy read, then copied to %s", ops, stagedKey, private, xorbKey)
	}
	if ids := stagedIDs(t, ss); len(ids) != 0 {
		t.Fatalf("staged after the commit = %v, want neither the upload nor its private copy", ids)
	}
	if stored, err := ss.getObject(ctx, xorbKey); err != nil || !bytes.Equal(stored, data) {
		t.Fatalf("published xorb = %d bytes, %v; want the %d staged bytes", len(stored), err, len(data))
	}
}

// A presigned URL may still write the staged key while the commit validates;
// the commit publishes the bytes it validated, not the ones written since.
func TestS3CommitIgnoresRePutDuringValidation(t *testing.T) {
	ctx := context.Background()
	data, xorbHash := storagetest.EncodeXorb(t, true, []byte("validated before a re-PUT"))
	forged := bytes.Repeat([]byte{0xa5}, len(data))
	var ss *Storage
	var stagedKey string
	var rePut atomic.Bool
	ss = newTestS3StorageVia(t, func(req *http.Request) (*http.Response, error) {
		resp, err := http.DefaultClient.Do(req)
		// The validating read already has its response when the staged key changes.
		if op := s3OpOf(req); err == nil && op.method == http.MethodGet && strings.HasPrefix(op.key, "staging/") && rePut.CompareAndSwap(false, true) {
			if err := ss.putObject(req.Context(), stagedKey, bytes.NewReader(forged)); err != nil {
				t.Errorf("re-PUT the staged key: %v", err)
			}
		}
		return resp, err
	})
	grantID := stageObject(t, ss, xorbHash, data)
	stagedKey = ss.objectKey("staging", grantID)

	if published, err := ss.CommitStagedXorb(ctx, grantID, xorbHash); err != nil || !published {
		t.Fatalf("CommitStagedXorb with the staged key re-PUT mid-validation = %v, %v; want true, nil", published, err)
	}
	if !rePut.Load() {
		t.Fatal("the commit read no staged object, want the staged key re-PUT during its validating read")
	}
	stored, err := ss.getObject(ctx, ss.objectKey("xorbs", xorbHash.String()))
	if err != nil {
		t.Fatalf("read the published xorb: %v", err)
	}
	if bytes.Equal(stored, forged) {
		t.Fatal("published xorb = the bytes re-PUT into the staged key after the validating read, want the validated ones")
	}
	if !bytes.Equal(stored, data) {
		t.Fatalf("published xorb = %d bytes, want the %d validated ones", len(stored), len(data))
	}
}

// A staged upload deleted before the commit fails the copy that takes it,
// before anything reads it: not found, with nothing published or left behind.
func TestS3CommitStagedXorbDeletedBeforePrivateCopy(t *testing.T) {
	ctx := context.Background()
	var trace s3Trace
	ss := newTestS3StorageVia(t, trace.forward)
	data, xorbHash := storagetest.EncodeXorb(t, true, []byte("deleted before its commit"))
	grantID := stageObject(t, ss, xorbHash, data)
	stagedKey := ss.objectKey("staging", grantID)
	if err := ss.DeleteStagedXorb(ctx, grantID); err != nil {
		t.Fatalf("DeleteStagedXorb: %v", err)
	}

	trace.reset()
	if published, err := ss.CommitStagedXorb(ctx, grantID, xorbHash); published || !errors.Is(err, storage.ErrStagedXorbNotFound) {
		t.Fatalf("CommitStagedXorb(deleted upload) = %v, %v; want false, ErrStagedXorbNotFound", published, err)
	}
	ops := trace.sent()
	privateKey(t, ss, ops, stagedKey)
	if slices.Contains(ops, "GET "+stagedKey) {
		t.Fatalf("commit sent %v, want the staged key copied, never read", ops)
	}
	if ok, err := ss.HasXorb(ctx, xorbHash); err != nil || ok {
		t.Fatalf("HasXorb = %v, %v; want absent", ok, err)
	}
	if ids := stagedIDs(t, ss); len(ids) != 0 {
		t.Fatalf("staged after the commit = %v, want nothing", ids)
	}
}

// A private copy swept before the commit reads it is not found, so the client
// asks for a new grant; the staged upload stays for the staging sweep.
func TestS3CommitPrivateCopySweptBeforeRead(t *testing.T) {
	ctx := context.Background()
	var ss *Storage
	var stagedKey string
	ss = newTestS3StorageVia(t, func(req *http.Request) (*http.Response, error) {
		// The sweep takes the private copy just before the commit reads it.
		if op := s3OpOf(req); op.method == http.MethodGet && strings.HasPrefix(op.key, "staging/") && op.key != stagedKey {
			if err := ss.DeleteStagedXorb(req.Context(), stagingID(op.key)); err != nil {
				return nil, err
			}
		}
		return http.DefaultClient.Do(req)
	})
	data, xorbHash := storagetest.EncodeXorb(t, true, []byte("its private copy swept"))
	grantID := stageObject(t, ss, xorbHash, data)
	stagedKey = ss.objectKey("staging", grantID)

	if published, err := ss.CommitStagedXorb(ctx, grantID, xorbHash); published || !errors.Is(err, storage.ErrStagedXorbNotFound) {
		t.Fatalf("CommitStagedXorb(private copy swept before its read) = %v, %v; want false, ErrStagedXorbNotFound", published, err)
	}
	if ids := stagedIDs(t, ss); !slices.Equal(ids, []string{grantID}) {
		t.Fatalf("staged after the commit = %v, want [%s] kept", ids, grantID)
	}
	if ok, err := ss.HasXorb(ctx, xorbHash); err != nil || ok {
		t.Fatalf("HasXorb = %v, %v; want absent", ok, err)
	}
}

// A byte past the staged xorb fails the commit as a mismatch, which deletes
// both the staged upload and its private copy.
func TestS3CommitTrailingBytesDropsPrivateCopy(t *testing.T) {
	ctx := context.Background()
	var trace s3Trace
	ss := newTestS3StorageVia(t, trace.forward)
	data, xorbHash := storagetest.EncodeXorb(t, true, []byte("followed by a trailing byte"))
	grantID := stageObject(t, ss, xorbHash, append(slices.Clone(data), 0))

	trace.reset()
	if published, err := ss.CommitStagedXorb(ctx, grantID, xorbHash); published || !errors.Is(err, storage.ErrStagedXorbMismatch) {
		t.Fatalf("CommitStagedXorb(xorb + 1 trailing byte) = %v, %v; want false, ErrStagedXorbMismatch", published, err)
	}
	private := privateKey(t, ss, trace.sent(), ss.objectKey("staging", grantID))
	if ids := stagedIDs(t, ss); len(ids) != 0 {
		t.Fatalf("staged after the refused commit = %v, want neither %s nor its private copy %s", ids, grantID, stagingID(private))
	}
	if ok, err := ss.HasXorb(ctx, xorbHash); err != nil || ok {
		t.Fatalf("HasXorb = %v, %v; want absent", ok, err)
	}
}

// Committing over a stored xorb publishes nothing and succeeds even when
// deleting the staged upload fails; the staging sweep reclaims it.
func TestS3CommitExistingXorbIgnoresStagedDeleteFailure(t *testing.T) {
	ctx := context.Background()
	var stagedDelete string
	var denied atomic.Bool
	ss := newTestS3StorageVia(t, func(req *http.Request) (*http.Response, error) {
		if stagedDelete != "" && s3OpOf(req).String() == stagedDelete {
			denied.Store(true)
			// Unlike a transport error, a denial is not retried.
			body := `<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`
			return &http.Response{
				Status:        "403 Forbidden",
				StatusCode:    http.StatusForbidden,
				Proto:         "HTTP/1.1",
				ProtoMajor:    1,
				ProtoMinor:    1,
				Header:        http.Header{"Content-Type": {"application/xml"}},
				Body:          io.NopCloser(strings.NewReader(body)),
				ContentLength: int64(len(body)),
				Request:       req,
			}, nil
		}
		return http.DefaultClient.Do(req)
	})
	data, xorbHash := storagetest.EncodeXorb(t, true, []byte("stored before its commit"))
	if _, err := ss.PutXorb(ctx, xorbHash, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	grantID := stageObject(t, ss, xorbHash, data)
	stagedDelete = "DELETE " + ss.objectKey("staging", grantID)

	if published, err := ss.CommitStagedXorb(ctx, grantID, xorbHash); published || err != nil {
		t.Fatalf("CommitStagedXorb(stored xorb, staged delete denied) = %v, %v; want false, nil", published, err)
	}
	if !denied.Load() {
		t.Fatal("the commit never deleted the staged upload, want a best-effort delete")
	}
}

// Read without a Content-Length, a staged upload is still validated up to the
// end of its stream.
func TestS3CommitStagedXorbWithoutContentLength(t *testing.T) {
	ctx := context.Background()
	ss := newTestS3StorageVia(t, func(req *http.Request) (*http.Response, error) {
		resp, err := http.DefaultClient.Do(req)
		if err == nil && req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/staging/") {
			resp.Header.Del("Content-Length")
			resp.ContentLength = -1
		}
		return resp, err
	})
	data, xorbHash := storagetest.EncodeXorb(t, true, []byte("read without a length"))

	trailing := stageObject(t, ss, xorbHash, append(slices.Clone(data), 0))
	if published, err := ss.CommitStagedXorb(ctx, trailing, xorbHash); published || !errors.Is(err, storage.ErrStagedXorbMismatch) {
		t.Fatalf("CommitStagedXorb(xorb + 1 trailing byte, no Content-Length) = %v, %v; want false, ErrStagedXorbMismatch", published, err)
	}
	if ids := stagedIDs(t, ss); len(ids) != 0 {
		t.Fatalf("staged after the refused commit = %v, want nothing", ids)
	}
	valid := stageObject(t, ss, xorbHash, data)
	if published, err := ss.CommitStagedXorb(ctx, valid, xorbHash); err != nil || !published {
		t.Fatalf("CommitStagedXorb(valid xorb, no Content-Length) = %v, %v; want true, nil", published, err)
	}
}

var errStagedReadCut = errors.New("staged read cut off")

// cutBody serves n bytes of a response body, then fails.
type cutBody struct {
	io.ReadCloser
	n int
}

func (c *cutBody) Read(p []byte) (int, error) {
	if c.n <= 0 {
		return 0, errStagedReadCut
	}
	n, err := c.ReadCloser.Read(p[:min(len(p), c.n)])
	c.n -= n
	return n, err
}

// A read failing mid-commit is the transport's fault, not the upload's: the
// commit reports it and keeps the staged upload for a retry.
func TestS3CommitStagedXorbKeepsUploadOnReadError(t *testing.T) {
	ctx := context.Background()
	cut := false
	ss := newTestS3StorageVia(t, func(req *http.Request) (*http.Response, error) {
		resp, err := http.DefaultClient.Do(req)
		if err == nil && cut && req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/staging/") {
			resp.Body = &cutBody{ReadCloser: resp.Body, n: 16}
		}
		return resp, err
	})
	data, xorbHash := storagetest.EncodeXorb(t, true, []byte("committed once the read recovers"))
	grantID := stageObject(t, ss, xorbHash, data)

	cut = true
	published, err := ss.CommitStagedXorb(ctx, grantID, xorbHash)
	if published || !errors.Is(err, errStagedReadCut) || errors.Is(err, storage.ErrStagedXorbMismatch) {
		t.Fatalf("CommitStagedXorb with its read cut off = %v, %v; want false and the read error, not ErrStagedXorbMismatch", published, err)
	}
	if ids := stagedIDs(t, ss); !slices.Equal(ids, []string{grantID}) {
		t.Fatalf("staged after the failed read = %v, want [%s] kept", ids, grantID)
	}
	cut = false
	if published, err := ss.CommitStagedXorb(ctx, grantID, xorbHash); err != nil || !published {
		t.Fatalf("CommitStagedXorb once the read recovers = %v, %v; want true, nil", published, err)
	}
}

// cancelOnResponse cancels once the first OK response match accepts is read whole.
type cancelOnResponse struct {
	cancel context.CancelFunc
	match  func(s3Op) bool
	fired  atomic.Bool
}

func (c *cancelOnResponse) forward(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK || !c.match(s3OpOf(req)) || !c.fired.CompareAndSwap(false, true) {
		return resp, err
	}
	// The body reads under the request's context, about to be cancelled.
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	c.cancel()
	return resp, nil
}

// A client gone once its xorb is published must not orphan the staging
// objects: the commit deletes the upload and its private copy regardless.
func TestS3CommitCleanupOutlivesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var xorbKey string
	canceller := &cancelOnResponse{cancel: cancel, match: func(op s3Op) bool { return op.method == "COPY" && op.key == xorbKey }}
	ss := newTestS3StorageVia(t, canceller.forward)
	data, xorbHash := storagetest.EncodeXorb(t, true, []byte("published as its client left"))
	xorbKey = ss.objectKey("xorbs", xorbHash.String())
	grantID := stageObject(t, ss, xorbHash, data)

	if published, err := ss.CommitStagedXorb(ctx, grantID, xorbHash); err != nil || !published {
		t.Fatalf("CommitStagedXorb cancelled after its publishing copy = %v, %v; want true, nil", published, err)
	}
	if !canceller.fired.Load() {
		t.Fatal("the commit never published by copy, want its context cancelled right after")
	}
	if ids := stagedIDs(t, ss); len(ids) != 0 {
		t.Fatalf("staged after the commit = %v, want the upload and its private copy deleted despite the cancelled context", ids)
	}
}

// A client gone while its upload is judged must not orphan the staging objects
// either: the refused commit deletes the upload and its private copy.
func TestS3CommitRefusalCleanupOutlivesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	canceller := &cancelOnResponse{cancel: cancel, match: func(op s3Op) bool {
		return op.method == http.MethodGet && strings.HasPrefix(op.key, "staging/")
	}}
	ss := newTestS3StorageVia(t, canceller.forward)
	data, xorbHash := storagetest.EncodeXorb(t, true, []byte("refused as its client left"))
	grantID := stageObject(t, ss, xorbHash, append(slices.Clone(data), 0))

	if published, err := ss.CommitStagedXorb(ctx, grantID, xorbHash); published || !errors.Is(err, storage.ErrStagedXorbMismatch) {
		t.Fatalf("CommitStagedXorb(xorb + 1 trailing byte) cancelled after its read = %v, %v; want false, ErrStagedXorbMismatch", published, err)
	}
	if !canceller.fired.Load() {
		t.Fatal("the commit never read its private copy, want its context cancelled right after")
	}
	if ids := stagedIDs(t, ss); len(ids) != 0 {
		t.Fatalf("staged after the refused commit = %v, want the upload and its private copy deleted despite the cancelled context", ids)
	}
}
