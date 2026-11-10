package storagetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/crc64nvme"
	"github.com/wzshiming/xet/storage"
)

// grantFor issues an upload grant declaring data's length and CRC-64/NVME.
func grantFor(t *testing.T, ctx context.Context, st storage.Storage, xorbHash xet.XorbHash, data []byte) *storage.XorbUploadGrant {
	t.Helper()
	grant, err := st.GrantXorbUpload(ctx, xorbHash, int64(len(data)), crc64nvme.Checksum(data))
	if err != nil {
		t.Fatalf("GrantXorbUpload: %v", err)
	}
	return grant
}

// stageXorb uploads data under a fresh grant for xorbHash.
func stageXorb(t *testing.T, ctx context.Context, b Backend, st storage.Storage, xorbHash xet.XorbHash, data []byte) *storage.XorbUploadGrant {
	t.Helper()
	grant := grantFor(t, ctx, st, xorbHash, data)
	if err := b.upload(t, ctx, st, grant, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatalf("upload under the grant: %v", err)
	}
	return grant
}

// assertMismatchRefused uploads length bytes of body under grant and fails unless
// the upload, or else the commit of what it staged, refuses them as mismatched.
func assertMismatchRefused(t *testing.T, ctx context.Context, b Backend, st storage.Storage, grant *storage.XorbUploadGrant, xorbHash xet.XorbHash, length int64, body io.Reader, what string) {
	t.Helper()
	if err := b.upload(t, ctx, st, grant, length, body); err != nil {
		if !errors.Is(err, storage.ErrStagedXorbMismatch) {
			t.Fatalf("upload(%s) = %v, want ErrStagedXorbMismatch or a refusal at commit", what, err)
		}
		return
	}
	// A store that takes the upload unchecked leaves the refusal to the commit.
	if published, err := st.CommitStagedXorb(ctx, grant.ID, xorbHash); published || !errors.Is(err, storage.ErrStagedXorbMismatch) {
		t.Fatalf("CommitStagedXorb(%s, staged unchecked) = %v, %v; want false, ErrStagedXorbMismatch", what, published, err)
	}
}

// assertStaged fails unless WalkStagedXorbs lists exactly want (grant id to size).
func assertStaged(t *testing.T, ctx context.Context, st storage.Storage, want map[string]int64) {
	t.Helper()
	got := map[string]int64{}
	if err := st.WalkStagedXorbs(ctx, func(grantID string, size int64, _ time.Time) error {
		got[grantID] = size
		return nil
	}); err != nil {
		t.Fatalf("WalkStagedXorbs: %v", err)
	}
	if !maps.Equal(got, want) {
		t.Fatalf("staged uploads = %v, want %v", got, want)
	}
}

var errReadPastLimit = errors.New("read past the granted length+1 bytes")

// limitedBody serves r but fails any read once limit bytes were served.
type limitedBody struct {
	r      io.Reader
	limit  int
	served int
}

func (l *limitedBody) Read(p []byte) (int, error) {
	if l.served >= l.limit {
		return 0, errReadPastLimit
	}
	n, err := l.r.Read(p)
	l.served += n
	return n, err
}

// testGrantRoundTrip: the commit publishes exactly the granted body and consumes the grant.
func testGrantRoundTrip(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	data, xorbHash := EncodeXorb(t, true, []byte("granted chunk one"), []byte("granted chunk two"))

	grant := grantFor(t, ctx, st, xorbHash, data)
	if !storage.ValidGrantID(grant.ID) {
		t.Fatalf("grant id = %q, want 32 lowercase hex characters", grant.ID)
	}
	if grant.Method != http.MethodPut {
		t.Fatalf("grant method = %q, want PUT", grant.Method)
	}
	if b.UploadGranted == nil {
		if !strings.HasSuffix(grant.URL, storage.XorbUploadPath(grant.ID)) {
			t.Fatalf("grant URL = %s, want ...%s", grant.URL, storage.XorbUploadPath(grant.ID))
		}
		if grant.Headers == nil || len(grant.Headers) != 0 {
			t.Fatalf("grant headers = %#v, want a non-nil empty map", grant.Headers)
		}
	} else {
		if u, err := url.Parse(grant.URL); err != nil || u.Scheme == "" || u.Host == "" {
			t.Fatalf("grant URL = %q, want an absolute URL to upload to directly", grant.URL)
		}
		if len(grant.Headers) == 0 {
			t.Fatalf("grant headers = %#v, want the headers a direct upload carries", grant.Headers)
		}
	}
	if err := b.upload(t, ctx, st, grant, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatalf("upload under the grant: %v", err)
	}
	assertStaged(t, ctx, st, map[string]int64{grant.ID: int64(len(data))})

	if published, err := st.CommitStagedXorb(ctx, grant.ID, xorbHash); err != nil || !published {
		t.Fatalf("CommitStagedXorb = %v, %v; want true, nil", published, err)
	}
	rc, err := st.GetXorbReadSeekCloser(ctx, xorbHash)
	if err != nil {
		t.Fatalf("GetXorbReadSeekCloser: %v", err)
	}
	stored, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || !bytes.Equal(stored, data) {
		t.Fatalf("published xorb = %d bytes, %v; want the %d staged bytes", len(stored), err, len(data))
	}
	assertStaged(t, ctx, st, nil)
	if _, err := st.CommitStagedXorb(ctx, grant.ID, xorbHash); !errors.Is(err, storage.ErrStagedXorbNotFound) {
		t.Fatalf("second CommitStagedXorb = %v, want ErrStagedXorbNotFound", err)
	}
	if err := st.PutStagedXorb(ctx, grant.ID, bytes.NewReader(data)); !errors.Is(err, storage.ErrUploadGrantNotFound) {
		t.Fatalf("PutStagedXorb after the commit = %v, want ErrUploadGrantNotFound (grant consumed)", err)
	}
}

// testGrantCommitExistingXorb: committing over a stored xorb publishes nothing yet consumes the grant.
func testGrantCommitExistingXorb(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	data, xorbHash := EncodeXorb(t, true, []byte("already stored chunk"))
	if _, err := st.PutXorb(ctx, xorbHash, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	grant := stageXorb(t, ctx, b, st, xorbHash, data)
	if published, err := st.CommitStagedXorb(ctx, grant.ID, xorbHash); err != nil || published {
		t.Fatalf("CommitStagedXorb(stored xorb) = %v, %v; want false, nil", published, err)
	}
	assertStaged(t, ctx, st, nil)
	if err := st.PutStagedXorb(ctx, grant.ID, bytes.NewReader(data)); !errors.Is(err, storage.ErrUploadGrantNotFound) {
		t.Fatalf("PutStagedXorb after the commit = %v, want ErrUploadGrantNotFound (grant consumed)", err)
	}
}

// testGrantPutLengthMismatch: off-by-one bodies are refused at upload or commit, leaving nothing behind; a store reading them stops at length+1.
func testGrantPutLengthMismatch(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	data, xorbHash := EncodeXorb(t, true, []byte("length-checked chunk"))
	n := int64(len(data))
	grant := grantFor(t, ctx, st, xorbHash, data)

	for _, body := range [][]byte{data[:n-1], append(slices.Clone(data), 0)} {
		// Granting the body's own checksum leaves the length as the only mismatch.
		own, err := st.GrantXorbUpload(ctx, xorbHash, n, crc64nvme.Checksum(body))
		if err != nil {
			t.Fatalf("GrantXorbUpload: %v", err)
		}
		for _, g := range []*storage.XorbUploadGrant{grant, own} {
			var r io.Reader = bytes.NewReader(body)
			// Only a store reading the body must stop at length+1; a direct upload's HTTP client probes for EOF.
			if b.UploadGranted == nil {
				r = &limitedBody{r: r, limit: len(data) + 1}
			}
			assertMismatchRefused(t, ctx, b, st, g, xorbHash, int64(len(body)), r, fmt.Sprintf("%d bytes under a %d-byte grant", len(body), n))
		}
	}
	assertStaged(t, ctx, st, nil)
	if ok, err := st.HasXorb(ctx, xorbHash); err != nil || ok {
		t.Fatalf("HasXorb after the refusals = %v, %v; want absent", ok, err)
	}
	fresh := grantFor(t, ctx, st, xorbHash, data)
	if err := b.upload(t, ctx, st, fresh, n, bytes.NewReader(data)); err != nil {
		t.Fatalf("upload(right body) under a new grant after the refusals = %v, want nil", err)
	}
}

// testGrantPutChecksumMismatch: one flipped byte is refused at upload or commit, leaving nothing behind.
func testGrantPutChecksumMismatch(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	data, xorbHash := EncodeXorb(t, true, []byte("checksummed chunk"))
	grant := grantFor(t, ctx, st, xorbHash, data)

	flipped := slices.Clone(data)
	flipped[len(flipped)/2] ^= 0x01
	assertMismatchRefused(t, ctx, b, st, grant, xorbHash, int64(len(flipped)), bytes.NewReader(flipped), "one flipped byte")
	assertStaged(t, ctx, st, nil)
	if ok, err := st.HasXorb(ctx, xorbHash); err != nil || ok {
		t.Fatalf("HasXorb after the refusal = %v, %v; want absent", ok, err)
	}
	fresh := grantFor(t, ctx, st, xorbHash, data)
	if err := b.upload(t, ctx, st, fresh, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatalf("upload(right body) under a new grant after the refusal = %v, want nil", err)
	}
}

// testGrantPutRepeat: a second upload under a staged grant is refused; the first still commits.
func testGrantPutRepeat(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	data, xorbHash := EncodeXorb(t, true, []byte("uploaded twice"))
	grant := stageXorb(t, ctx, b, st, xorbHash, data)

	if err := b.upload(t, ctx, st, grant, int64(len(data)), bytes.NewReader(data)); !errors.Is(err, storage.ErrStagedXorbExists) {
		t.Fatalf("second upload = %v, want ErrStagedXorbExists", err)
	}
	if published, err := st.CommitStagedXorb(ctx, grant.ID, xorbHash); err != nil || !published {
		t.Fatalf("CommitStagedXorb = %v, %v; want true, nil", published, err)
	}
}

// testGrantPutUnknownID: ids never granted — unknown, traversing, hash-length — stage nothing.
func testGrantPutUnknownID(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	data, _ := EncodeXorb(t, true, []byte("ungranted chunk"))

	for _, id := range []string{strings.Repeat("0", 32), "../x", strings.Repeat("ab", 32)} {
		if err := st.PutStagedXorb(ctx, id, bytes.NewReader(data)); !errors.Is(err, storage.ErrUploadGrantNotFound) {
			t.Fatalf("PutStagedXorb(%q) = %v, want ErrUploadGrantNotFound", id, err)
		}
	}
	assertStaged(t, ctx, st, nil)
}

// testGrantExpired: late uploads are refused as expired, repeatedly, and stage nothing.
func testGrantExpired(t *testing.T, b Backend) {
	if b.UploadGranted != nil {
		t.Skip("a direct upload expires by the store's signature check")
	}
	ctx := context.Background()
	st := b.NewWithUploadGrantTTL(t, time.Nanosecond)
	data, xorbHash := EncodeXorb(t, true, []byte("late chunk"))
	grant := grantFor(t, ctx, st, xorbHash, data)
	time.Sleep(10 * time.Millisecond)

	for attempt := 1; attempt <= 2; attempt++ {
		if err := b.upload(t, ctx, st, grant, int64(len(data)), bytes.NewReader(data)); !errors.Is(err, storage.ErrUploadGrantExpired) {
			t.Fatalf("upload attempt %d after the TTL = %v, want ErrUploadGrantExpired", attempt, err)
		}
	}
	assertStaged(t, ctx, st, nil)
}

// testGrantCommitInvalidXorb: commit, not upload, validates the bytes against the committed hash.
func testGrantCommitInvalidXorb(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	dataA, _ := EncodeXorb(t, true, []byte("xorb A's chunk"))
	_, hashB := EncodeXorb(t, true, []byte("xorb B's chunk"))

	grant := grantFor(t, ctx, st, hashB, dataA)
	if err := b.upload(t, ctx, st, grant, int64(len(dataA)), bytes.NewReader(dataA)); err != nil {
		t.Fatalf("upload(A's bytes under B's grant) = %v, want nil", err)
	}
	_, err := st.CommitStagedXorb(ctx, grant.ID, hashB)
	if !errors.Is(err, storage.ErrStagedXorbMismatch) || !strings.Contains(err.Error(), "xorb hash mismatch") {
		t.Fatalf("CommitStagedXorb(A's bytes as B) = %v, want ErrStagedXorbMismatch carrying the xorb hash mismatch", err)
	}
	if ok, err := st.HasXorb(ctx, hashB); err != nil || ok {
		t.Fatalf("HasXorb(B) = %v, %v; want absent", ok, err)
	}
	assertStaged(t, ctx, st, nil)
	if err := st.PutStagedXorb(ctx, grant.ID, bytes.NewReader(dataA)); !errors.Is(err, storage.ErrUploadGrantNotFound) {
		t.Fatalf("PutStagedXorb after the refused commit = %v, want ErrUploadGrantNotFound (grant dropped)", err)
	}
}

// testGrantCommitTrailingBytes: a byte past a valid xorb's end fails the commit, which publishes nothing and drops the upload.
func testGrantCommitTrailingBytes(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	data, xorbHash := EncodeXorb(t, true, []byte("chunk before a trailing byte"))
	// The grant declares the whole body, so only the commit can catch the extra byte.
	body := append(slices.Clone(data), 0)
	grant := stageXorb(t, ctx, b, st, xorbHash, body)

	if published, err := st.CommitStagedXorb(ctx, grant.ID, xorbHash); published || !errors.Is(err, storage.ErrStagedXorbMismatch) {
		t.Fatalf("CommitStagedXorb(xorb + 1 trailing byte) = %v, %v; want false, ErrStagedXorbMismatch", published, err)
	}
	if ok, err := st.HasXorb(ctx, xorbHash); err != nil || ok {
		t.Fatalf("HasXorb = %v, %v; want absent", ok, err)
	}
	var stored []string
	if err := st.WalkXorbs(ctx, func(hash string, _ int64, _ time.Time) error {
		stored = append(stored, hash)
		return nil
	}); err != nil || len(stored) != 0 {
		t.Fatalf("WalkXorbs = %v, %v; want no xorbs", stored, err)
	}
	assertStaged(t, ctx, st, nil)
	if err := st.PutStagedXorb(ctx, grant.ID, bytes.NewReader(body)); !errors.Is(err, storage.ErrUploadGrantNotFound) {
		t.Fatalf("PutStagedXorb after the refused commit = %v, want ErrUploadGrantNotFound (grant dropped)", err)
	}
}

// testGrantCommitRefreshesModTime: the published xorb is stamped at its commit, not at its upload.
func testGrantCommitRefreshesModTime(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	data, xorbHash := EncodeXorb(t, true, []byte("committed a second after its upload"))
	grant := stageXorb(t, ctx, b, st, xorbHash, data)
	var uploaded time.Time
	if err := st.WalkStagedXorbs(ctx, func(_ string, _ int64, modTime time.Time) error {
		uploaded = modTime
		return nil
	}); err != nil || uploaded.IsZero() {
		t.Fatalf("WalkStagedXorbs = %v with the upload stamped %v, want its time", err, uploaded)
	}
	// S3 may stamp whole seconds, so the commit runs in a later one.
	next := uploaded.Truncate(time.Second).Add(time.Second)
	for time.Now().Before(next) {
		time.Sleep(time.Until(next))
	}
	committing := time.Now()

	if published, err := st.CommitStagedXorb(ctx, grant.ID, xorbHash); err != nil || !published {
		t.Fatalf("CommitStagedXorb = %v, %v; want true, nil", published, err)
	}
	var stamped time.Time
	if err := st.WalkXorbs(ctx, func(_ string, _ int64, modTime time.Time) error {
		stamped = modTime
		return nil
	}); err != nil {
		t.Fatalf("WalkXorbs: %v", err)
	}
	if floor := committing.Truncate(time.Second); stamped.Before(uploaded) || stamped.Before(floor) {
		t.Fatalf("published xorb stamped %v, want the commit's time (not before %v) rather than the upload's %v", stamped, floor, uploaded)
	}
}

// testGrantCommitAfterExpiry: a grant expiring between the upload and the commit leaves the upload committable.
func testGrantCommitAfterExpiry(t *testing.T, b Backend) {
	ctx := context.Background()
	const ttl = 500 * time.Millisecond
	st := b.NewWithUploadGrantTTL(t, ttl)
	data, xorbHash := EncodeXorb(t, true, []byte("committed past its grant's TTL"))

	grant := grantFor(t, ctx, st, xorbHash, data)
	err := b.upload(t, ctx, st, grant, int64(len(data)), bytes.NewReader(data))
	if errors.Is(err, storage.ErrUploadGrantExpired) {
		// The machine stalled past the TTL before the upload: retry once.
		grant = grantFor(t, ctx, st, xorbHash, data)
		err = b.upload(t, ctx, st, grant, int64(len(data)), bytes.NewReader(data))
	}
	if err != nil {
		t.Fatalf("upload within the TTL: %v", err)
	}
	time.Sleep(ttl + 200*time.Millisecond)

	if published, err := st.CommitStagedXorb(ctx, grant.ID, xorbHash); err != nil || !published {
		t.Fatalf("CommitStagedXorb past the grant's TTL = %v, %v; want true, nil", published, err)
	}
	if ok, err := st.HasXorb(ctx, xorbHash); err != nil || !ok {
		t.Fatalf("HasXorb = %v, %v; want stored", ok, err)
	}
}

// testGrantCommitUnknownID: ids without a staged upload, or malformed even when naming its path, commit nothing.
func testGrantCommitUnknownID(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	data, xorbHash := EncodeXorb(t, true, []byte("committed under its own id only"))
	staged := stageXorb(t, ctx, b, st, xorbHash, data)
	pending := grantFor(t, ctx, st, xorbHash, data)

	for _, id := range []string{strings.Repeat("0", 32), pending.ID, "", "../x", strings.Repeat("ab", 32), staged.ID + "/"} {
		if published, err := st.CommitStagedXorb(ctx, id, xorbHash); !errors.Is(err, storage.ErrStagedXorbNotFound) {
			t.Fatalf("CommitStagedXorb(%q) = %v, %v; want ErrStagedXorbNotFound", id, published, err)
		}
	}
	assertStaged(t, ctx, st, map[string]int64{staged.ID: int64(len(data))})
}

// testSweepReclaimsStaleStagedXorb: an uncommitted upload is spared in grace, dry-reported, then reclaimed.
func testSweepReclaimsStaleStagedXorb(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	data, xorbHash := EncodeXorb(t, true, []byte("abandoned upload"))
	grant := stageXorb(t, ctx, b, st, xorbHash, data)
	size := int64(len(data))

	res, err := storage.Sweep(ctx, st, storage.SweepOptions{Grace: time.Hour})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.SweptStagedXorbs != 0 || res.SkippedInGrace != 1 {
		t.Fatalf("in-grace sweep = %d staged swept, %d skipped; want 0, 1", res.SweptStagedXorbs, res.SkippedInGrace)
	}

	res, err = storage.Sweep(ctx, st, storage.SweepOptions{Grace: NoGrace, DryRun: true})
	if err != nil {
		t.Fatalf("dry Sweep: %v", err)
	}
	if res.SweptStagedXorbs != 1 || res.ReclaimedBytes != size {
		t.Fatalf("dry sweep = %d staged swept, %d bytes; want 1, %d", res.SweptStagedXorbs, res.ReclaimedBytes, size)
	}
	assertStaged(t, ctx, st, map[string]int64{grant.ID: size})

	res, err = storage.Sweep(ctx, st, storage.SweepOptions{Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.SweptStagedXorbs != 1 || res.ReclaimedBytes != size {
		t.Fatalf("sweep = %d staged swept, %d bytes; want 1, %d", res.SweptStagedXorbs, res.ReclaimedBytes, size)
	}
	assertStaged(t, ctx, st, nil)
	if _, err := st.CommitStagedXorb(ctx, grant.ID, xorbHash); !errors.Is(err, storage.ErrStagedXorbNotFound) {
		t.Fatalf("CommitStagedXorb after the sweep = %v, want ErrStagedXorbNotFound", err)
	}
}

// testSweepStepCountsStagedDeletes: staged deletes count toward MaxDeletes, and spending it ends the pass before xorbs are judged.
func testSweepStepCountsStagedDeletes(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	dead, deadHash := EncodeXorb(t, true, []byte("unreferenced xorb"))
	if _, err := st.PutXorb(ctx, deadHash, bytes.NewReader(dead)); err != nil {
		t.Fatal(err)
	}
	staged, stagedHash := EncodeXorb(t, true, []byte("abandoned upload"))
	stageXorb(t, ctx, b, st, stagedHash, staged)

	g := storage.NewGC(st)
	for step, wantDone := range []bool{false, true} {
		res, err := g.SweepStep(ctx, storage.SweepOptions{Grace: NoGrace, MaxDeletes: 1})
		if err != nil {
			t.Fatalf("SweepStep %d: %v", step+1, err)
		}
		if swept := res.SweptStagedXorbs + len(res.SweptXorbs); res.Done != wantDone || swept != 1 {
			t.Fatalf("step %d = done %v after %d deletes, want done %v after 1", step+1, res.Done, swept, wantDone)
		}
		if step == 0 {
			if res.SweptStagedXorbs != 1 || res.RemainingXorbs != 0 {
				t.Fatalf("step 1 = %d staged swept, %d xorbs remaining; want 1, 0 (the staged delete spent the bound before xorbs were judged)", res.SweptStagedXorbs, res.RemainingXorbs)
			}
			if ok, err := st.HasXorb(ctx, deadHash); err != nil || !ok {
				t.Fatalf("HasXorb(dead) after step 1 = %v, %v; want still stored", ok, err)
			}
		}
	}
	assertStaged(t, ctx, st, nil)
	if ok, err := st.HasXorb(ctx, deadHash); err != nil || ok {
		t.Fatalf("HasXorb(dead) = %v, %v; want swept", ok, err)
	}
}

// testGrantUsageUnaffected: only the commit, not the staged upload, shows in Usage.
func testGrantUsageUnaffected(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	before, err := st.Usage(ctx)
	if err != nil {
		t.Fatalf("Usage: %v", err)
	}
	data, xorbHash := EncodeXorb(t, true, []byte("usage-neutral upload"))
	grant := stageXorb(t, ctx, b, st, xorbHash, data)

	if got, err := st.Usage(ctx); err != nil || got != before {
		t.Fatalf("Usage with a staged upload = %+v, %v; want %+v", got, err, before)
	}
	if _, err := st.CommitStagedXorb(ctx, grant.ID, xorbHash); err != nil {
		t.Fatalf("CommitStagedXorb: %v", err)
	}
	want := before
	want.Xorbs.Count++
	want.Xorbs.Bytes += int64(len(data))
	if got, err := st.Usage(ctx); err != nil || got != want {
		t.Fatalf("Usage after the commit = %+v, %v; want %+v", got, err, want)
	}
}

// testGrantInvisibleToWalkXorbs: a staged upload is no xorb or shard, and deleting it is idempotent.
func testGrantInvisibleToWalkXorbs(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	data, xorbHash := EncodeXorb(t, true, []byte("staged, not stored"))
	grant := stageXorb(t, ctx, b, st, xorbHash, data)

	for name, walk := range map[string]func(context.Context, func(string, int64, time.Time) error) error{
		"WalkXorbs":  st.WalkXorbs,
		"WalkShards": st.WalkShards,
	} {
		if err := walk(ctx, func(hash string, _ int64, _ time.Time) error {
			return errors.New("walked " + hash)
		}); err != nil {
			t.Fatalf("%s with only a staged upload = %v, want no objects", name, err)
		}
	}
	for attempt := 1; attempt <= 2; attempt++ {
		if err := st.DeleteStagedXorb(ctx, grant.ID); err != nil {
			t.Fatalf("DeleteStagedXorb attempt %d = %v, want nil", attempt, err)
		}
		assertStaged(t, ctx, st, nil)
	}
}

var errPutReturned = errors.New("upload returned")

// testGrantPutInFlightHidden: an upload still streaming is unlisted, so a no-grace sweep cannot reclaim it.
func testGrantPutInFlightHidden(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	data, xorbHash := EncodeXorb(t, true, []byte("still streaming"))
	grant := grantFor(t, ctx, st, xorbHash, data)

	pr, pw := io.Pipe()
	defer pw.Close()
	done := make(chan error, 1)
	go func() {
		err := b.upload(t, ctx, st, grant, int64(len(data)), pr)
		pr.CloseWithError(errPutReturned)
		done <- err
	}()
	half := len(data) / 2
	if _, err := pw.Write(data[:half]); err != nil {
		t.Fatalf("upload stopped reading mid-body: %v", <-done)
	}

	assertStaged(t, ctx, st, nil)
	res, err := storage.Sweep(ctx, st, storage.SweepOptions{Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.SweptStagedXorbs != 0 {
		t.Fatalf("sweep mid-upload reclaimed %d staged uploads, want 0", res.SweptStagedXorbs)
	}

	if _, err := pw.Write(data[half:]); err != nil {
		t.Fatalf("upload stopped reading mid-body: %v", <-done)
	}
	pw.Close()
	if err := <-done; err != nil {
		t.Fatalf("upload after the sweep = %v, want nil", err)
	}
	assertStaged(t, ctx, st, map[string]int64{grant.ID: int64(len(data))})
}
