package chaos_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
)

// ulSize keeps every scenario file within one xorb.
const ulSize = 1 << 20

// Scenario file seeds; a seed only has to be fresh within its Test's fixture, so gap subtests add their index to a base.
const (
	ulSeedXorb5xx        uint32 = 16
	ulSeedXorbCut        uint32 = 17
	ulSeedHeadStall      uint32 = 18
	ulSeedRetryableFrame uint32 = 19
	ulSeedStreamStall    uint32 = 20
	ulSeedStreamCut      uint32 = 21
	ulSeedFallback       uint32 = 22
	ulSeedCancel         uint32 = 23
	ulSeedTCPSlow        uint32 = 24
	ulSeedTCPCut         uint32 = 25
	ulSeedGapIdle        uint32 = 26
	ulSeedGapBatch       uint32 = 28
	ulSeedGapStream      uint32 = 30
)

// ulAPI is the shard API an upload commits through.
type ulAPI int

const (
	ulV1 ulAPI = iota + 1 // UploadFile: POST /v1/shards
	ulV2                  // UploadFileV2: POST /v2/shards, answered with an NDJSON stream
)

// ulFirstFrame is the frame /v2/shards flushes before reading the shard (server/shard_v2.go:152); relaying its length cuts a stream before the result.
const ulFirstFrame = `{"type":"validating","verified":0,"total":0}` + "\n"

// ulRetryableNDJSON is a /v2/shards answer whose terminal frame asks for the upload again.
const ulRetryableNDJSON = ulFirstFrame + `{"type":"error","message":"chaos","retryable":true}` + "\n"

// TestULXorbPost5xxRetried pins a 503 on the xorb POST being retried with the whole body and progress committed once.
func TestULXorbPost5xxRetried(t *testing.T) {
	fx := newFixture(t)
	ctx, _ := testCtx(t, testTimeout)
	data := pseudoRandom(ulSize, ulSeedXorb5xx)
	dir, c, prog := ulClient(t, fx)
	fx.fp.arm(ulOnce(ulXorbPost, fault{kind: injectStatus, status: http.StatusServiceUnavailable}))
	before := snapshotWorkers()

	res := ulRun(t, ctx, c, ulV1, data)
	ulRequireOK(t, fx, dir, before, data, res)
	recs := ulSettle(t, fx)
	ulRequireTrace(t, recs, "query 200", "head 404", "post 503", "post 200", "shard1 200")
	posts := filter(recs, ulXorbPost)
	if posts[0].ReqBytes != posts[1].ReqBytes {
		t.Fatalf("the 503'd xorb POST carried %d body bytes and its retry %d; want the whole xorb both times", posts[0].ReqBytes, posts[1].ReqBytes)
	}
	ulRequireProgressOnce(t, prog, posts[1])
}

// TestULXorbPostCutMidBodyRetried pins a xorb POST cut 256 KiB into its body being resent whole and progress committed once.
func TestULXorbPostCutMidBodyRetried(t *testing.T) {
	const cut = 256 << 10
	fx := newFixture(t)
	ctx, _ := testCtx(t, testTimeout)
	data := pseudoRandom(ulSize, ulSeedXorbCut)
	dir, c, prog := ulClient(t, fx)
	fx.fp.arm(ulOnce(ulXorbPost, fault{kind: cutRequestBody, prefix: cut}))
	before := snapshotWorkers()

	res := ulRun(t, ctx, c, ulV1, data)
	ulRequireOK(t, fx, dir, before, data, res)
	recs := ulSettle(t, fx)
	ulRequireTrace(t, recs, "query 200", "head 404", "post 0", "post 200", "shard1 200")
	posts := filter(recs, ulXorbPost)
	if posts[0].ReqBytes != cut {
		t.Fatalf("the cut xorb POST relayed %d body bytes; want %d", posts[0].ReqBytes, cut)
	}
	ulRequireProgressOnce(t, prog, posts[1])
}

// TestULXorbHeadStallRetried pins a xorb HEAD stalled before its headers being abandoned by the idle guard and retried.
func TestULXorbHeadStallRetried(t *testing.T) {
	fx := newFixture(t)
	ctx, _ := testCtx(t, testTimeout)
	data := pseudoRandom(ulSize, ulSeedHeadStall)
	dir, c, _ := ulClient(t, fx, shortIdle)
	fx.fp.arm(ulOnce(ulXorbHead, fault{kind: stallHeaders}))
	before := snapshotWorkers()

	res := ulAwait(t, ulStart(ctx, c, ulV1, data), 3*time.Second)
	ulRequireOK(t, fx, dir, before, data, res)
	ulRequireTrace(t, ulSettle(t, fx), "query 200", "head 0", "head 404", "post 200", "shard1 200")
}

// TestULShardV2RetryableFrameRetried pins a retryable error frame from /v2/shards repeating the shard upload.
func TestULShardV2RetryableFrameRetried(t *testing.T) {
	fx := newFixture(t)
	ctx, _ := testCtx(t, testTimeout)
	data := pseudoRandom(ulSize, ulSeedRetryableFrame)
	dir, c, _ := ulClient(t, fx)
	fx.fp.arm(ulOnce(ulShard(ulV2), fault{kind: injectBody, status: http.StatusOK, contentType: "application/x-ndjson", body: []byte(ulRetryableNDJSON)}))
	before := snapshotWorkers()

	res := ulRun(t, ctx, c, ulV2, data)
	ulRequireOK(t, fx, dir, before, data, res)
	recs := ulSettle(t, fx)
	ulRequireTrace(t, recs, "query 200", "head 404", "post 200", "shard2 200", "shard2 200")
	if shards := filter(recs, ulShard(ulV2)); shards[0].Fault != injectBody || shards[1].Fault != passThrough {
		t.Fatalf("shard uploads %s; want the injected retryable stream, then the backend's", ulDescribe(shards))
	}
}

// TestULShardV2StreamStallCancel pins a /v2/shards stream stalled after its first frame holding the upload until cancel ends it promptly and cleanly.
func TestULShardV2StreamStallCancel(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := testCtx(t, testTimeout)
	data := pseudoRandom(ulSize, ulSeedStreamStall)
	dir, c, _ := ulClient(t, fx)
	prefix := int64(len(ulFirstFrame))
	fx.fp.arm(ulAlways(ulShard(ulV2), fault{kind: stallBody, prefix: prefix}))
	before := snapshotWorkers()

	ch := ulStart(ctx, c, ulV2, data)
	ulAwaitRecord(t, fx, ch, func(r record) bool {
		return ulShard(ulV2)(r) && r.Status == http.StatusOK && r.Bytes >= prefix
	})
	select {
	case res := <-ch:
		t.Fatalf("upload ended before cancel on a stalled stream: %v; want only ctx to end it", res.err)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	if res := ulAwait(t, ch, time.Second); res.err == nil {
		t.Fatal("upload succeeded on a stream stalled before its result")
	}
	ulSettle(t, fx)
	ulRequireNoStaging(t, dir)
	fx.noLeakedWorkers(t, before)
}

// TestULShardV2StreamCutFailsFast pins a /v2/shards stream reset after its first frame failing the upload at once, not at a deadline.
func TestULShardV2StreamCutFailsFast(t *testing.T) {
	fx := newFixture(t)
	ctx, _ := testCtx(t, testTimeout)
	data := pseudoRandom(ulSize, ulSeedStreamCut)
	dir, c, _ := ulClient(t, fx)
	fx.fp.arm(ulAlways(ulShard(ulV2), fault{kind: resetBody, prefix: int64(len(ulFirstFrame))}))
	before := snapshotWorkers()

	res := ulAwait(t, ulStart(ctx, c, ulV2, data), 2*time.Second)
	if res.err == nil || errors.Is(res.err, context.DeadlineExceeded) {
		t.Fatalf("upload over reset /v2/shards streams: %v; want a prompt error, not a deadline", res.err)
	}
	if recs := ulSettle(t, fx); len(filter(recs, ulShard(ulV2))) == 0 {
		t.Fatalf("no /v2/shards request; want the failure to come from the stream:%s", ulDescribe(recs))
	}
	ulRequireNoStaging(t, dir)
	fx.noLeakedWorkers(t, before)
}

// TestULDedupBatchFallbackGET pins a 404 or 405 batch query falling back to a shard GET per probe: a miss uploads the xorb, a hit skips it.
func TestULDedupBatchFallbackGET(t *testing.T) {
	fx := newFixture(t)
	ctx, _ := testCtx(t, testTimeout)
	data := pseudoRandom(ulSize, ulSeedFallback)
	var hashes []xet.FileHash
	for _, phase := range []struct {
		status int
		trace  []string
	}{
		{http.StatusNotFound, []string{"query 404", "chunk 404", "head 404", "post 200", "shard1 200"}},
		// A fresh cache dir: the first client's cached chunk locations would skip the query.
		{http.StatusMethodNotAllowed, []string{"query 405", "chunk 200", "shard1 200"}},
	} {
		dir, c, _ := ulClient(t, fx)
		fx.fp.arm(ulAlways(ulBatchQuery, fault{kind: injectStatus, status: phase.status}))
		before := snapshotWorkers()

		res := ulRun(t, ctx, c, ulV1, data)
		ulRequireOK(t, fx, dir, before, data, res)
		ulRequireTrace(t, ulSettle(t, fx), phase.trace...)
		hashes = append(hashes, res.hash)
	}
	if hashes[0] != hashes[1] {
		t.Fatalf("the upload behind a 405 returned %s; want %s as behind the 404", hashes[1], hashes[0])
	}
}

// TestULDedupShardGet5xxKeepsBatchHit pins a batch hit surviving a 503 on its shard GET: only the chunks it misses go into a new xorb.
func TestULDedupShardGet5xxKeepsBatchHit(t *testing.T) {
	fx := newFixture(t)
	ctx, _ := testCtx(t, testTimeout)
	dir, c, _ := ulClient(t, fx)
	fx.fp.arm(ulAlways(ulChunkGet, fault{kind: injectStatus, status: http.StatusServiceUnavailable}))
	before := snapshotWorkers()

	res := ulRun(t, ctx, c, ulV1, fx.file1.data)
	ulRequireOK(t, fx, dir, before, fx.file1.data, res)
	if res.hash != fx.file1.hash {
		t.Fatalf("upload of file1 returned %s; want %s", res.hash, fx.file1.hash)
	}
	recs := ulSettle(t, fx)
	ulRequireTrace(t, recs, "query 200", "chunk 503", "head 404", "post 200", "shard1 200")
	// Xorb hashes are chunk Merkle roots: a dropped hit would repack file1's own xorb, which HEAD finds.
	head, post := ulXorbHash(filter(recs, ulXorbHead)[0]), ulXorbHash(filter(recs, ulXorbPost)[0])
	if own := fx.file1.layout.Terms[0].Hash; head == own || post != head {
		t.Fatalf("HEAD of xorb %s, POST of %s; want both of one xorb other than file1's %s", head, post, own)
	}
}

// TestULCancelMidUploadCleansStaging pins cancel during a stalled xorb POST leaving no staged xorb and nothing stored, and the same client then completing the upload.
func TestULCancelMidUploadCleansStaging(t *testing.T) {
	fx := newFixture(t)
	data := pseudoRandom(ulSize, ulSeedCancel)
	dir, c, _ := ulClient(t, fx)
	fx.fp.arm(ulAlways(ulXorbPost, fault{kind: stallHeaders}))
	before := snapshotWorkers()

	ctx, cancel := testCtx(t, testTimeout)
	ch := ulStart(ctx, c, ulV1, data)
	ulAwaitRecord(t, fx, ch, ulXorbPost)
	cancel()
	if res := ulAwait(t, ch, 2*time.Second); !errors.Is(res.err, context.Canceled) {
		t.Fatalf("upload canceled during its xorb POST: %v; want context.Canceled", res.err)
	}
	ulRequireTrace(t, ulSettle(t, fx), "query 200", "head 404", "post 0")
	ulRequireNoStaging(t, dir)
	fx.noLeakedWorkers(t, before)
	if fx.storedErr(data) == nil {
		t.Fatal("the server stores the file whose upload was canceled")
	}

	fx.fp.arm(nil)
	ctx, _ = testCtx(t, testTimeout)
	before = snapshotWorkers()
	res := ulRun(t, ctx, c, ulV1, data)
	ulRequireOK(t, fx, dir, before, data, res)
	ulRequireTrace(t, ulSettle(t, fx), "query 200", "head 404", "post 200", "shard1 200")
}

// TestULTCPImpairedUploadPath pins uploads over a slow, jittery link and over a connection reset inside the xorb body.
func TestULTCPImpairedUploadPath(t *testing.T) {
	fx := newFixture(t)
	t.Run("delayAndRate", func(t *testing.T) {
		ctx, _ := testCtx(t, testTimeout)
		fx.fp.arm(nil)
		fx.tcp.delay(toServer, 20*time.Millisecond, 5*time.Millisecond)
		fx.tcp.rate(toServer, 8<<20, 64<<10)
		t.Cleanup(fx.tcp.clear)
		data := pseudoRandom(ulSize, ulSeedTCPSlow)
		dir, c, prog := ulClient(t, fx)
		cuts, sent := fx.tcp.cuts(), fx.tcp.bytes(toServer)
		before := snapshotWorkers()

		res := ulAwait(t, ulStart(ctx, c, ulV1, data), 5*time.Second)
		ulRequireOK(t, fx, dir, before, data, res)
		recs := ulSettle(t, fx)
		ulRequireTrace(t, recs, "query 200", "head 404", "post 200", "shard1 200")
		post := filter(recs, ulXorbPost)[0]
		ulRequireProgressOnce(t, prog, post)
		if got := fx.tcp.bytes(toServer) - sent; got < post.ReqBytes {
			t.Fatalf("the link carried %d bytes to the server; want at least the %d-byte xorb", got, post.ReqBytes)
		}
		if fired := fx.tcp.cuts() - cuts; fired != 0 {
			t.Fatalf("%d cuts fired on a link with none armed", fired)
		}
	})
	t.Run("cutMidBody", func(t *testing.T) {
		ctx, _ := testCtx(t, testTimeout)
		fx.fp.arm(nil)
		fx.tcp.cutOnce(toServer, 512<<10, cutRst)
		t.Cleanup(fx.tcp.clear)
		data := pseudoRandom(ulSize, ulSeedTCPCut)
		dir, c, prog := ulClient(t, fx)
		cuts := fx.tcp.cuts()
		before := snapshotWorkers()

		res := ulRun(t, ctx, c, ulV1, data)
		ulRequireOK(t, fx, dir, before, data, res)
		// Not settled: the cut's RST can miss the fault proxy (seen on macOS), holding that POST's handler open until cleanup.
		posts := filter(fx.fp.records(), ulXorbPost)
		if len(posts) != 2 || posts[1].Status != http.StatusOK {
			t.Fatalf("xorb POSTs %s; want the cut one, then a 200 retry", ulDescribe(posts))
		}
		ulRequireProgressOnce(t, prog, posts[1])
		if fired := fx.tcp.cuts() - cuts; fired != 1 {
			t.Fatalf("%d cuts fired; want the one armed", fired)
		}
	})
}

// TestULGapPostStallIdle wants a stalled POST abandoned at the idle timeout and retried, as GETs and HEADs are.
func TestULGapPostStallIdle(t *testing.T) {
	fx := newFixture(t)
	for i, tc := range []struct {
		name  string
		api   ulAPI
		match func(record) bool
		f     fault
	}{
		{"xorbPost", ulV1, ulXorbPost, fault{kind: stallHeaders}},
		{"shardV2Stream", ulV2, ulShard(ulV2), fault{kind: stallBody, prefix: int64(len(ulFirstFrame))}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := pseudoRandom(ulSize, ulSeedGapIdle+uint32(i))
			dir, c, _ := ulClient(t, fx, shortIdle)
			fx.fp.arm(ulOnce(tc.match, tc.f))
			knownGap(t, "POST paths bypass the idle guard, so a stalled POST waits on ctx alone: client/idle.go:31-33, client/client.go:236")
			ctx, _ := testCtx(t, 3*time.Second)
			before := snapshotWorkers()

			res := ulRun(t, ctx, c, tc.api, data)
			ulRequireOK(t, fx, dir, before, data, res)
			if recs := ulSettle(t, fx); len(filter(recs, tc.match)) != 2 || res.elapsed >= 2*time.Second {
				t.Fatalf("upload took %v; want the stalled request and one retry within 2s:%s", res.elapsed, ulDescribe(recs))
			}
		})
	}
}

// TestULGapDedupBatchFailure wants an upload to survive a failed batch dedup query, by retrying it or uploading without dedup.
func TestULGapDedupBatchFailure(t *testing.T) {
	fx := newFixture(t)
	for i, tc := range []struct {
		name string
		opts []client.Options
		arm  func(t *testing.T)
	}{
		{"status503", nil, func(*testing.T) {
			fx.fp.arm(ulOnce(ulBatchQuery, fault{kind: injectStatus, status: http.StatusServiceUnavailable}))
		}},
		{"stall", []client.Options{shortIdle}, func(*testing.T) {
			fx.fp.arm(ulOnce(ulBatchQuery, fault{kind: stallHeaders}))
		}},
		{"refused", nil, func(t *testing.T) {
			fx.fp.arm(nil)
			fx.tcp.refuse(1)
			t.Cleanup(fx.tcp.clear)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := pseudoRandom(ulSize, ulSeedGapBatch+uint32(i))
			dir, c, _ := ulClient(t, fx, tc.opts...)
			tc.arm(t)
			knownGap(t, "the batch dedup POST has no retry or idle guard and any failure but 404/405 aborts the upload: client/shard.go:288-301, upload/upload.go:203-206")
			ctx, _ := testCtx(t, 3*time.Second)
			before := snapshotWorkers()

			res := ulRun(t, ctx, c, ulV1, data)
			ulRequireOK(t, fx, dir, before, data, res)
			if recs := ulSettle(t, fx); len(filter(recs, ulXorbPost)) != 1 {
				t.Fatalf("want one xorb POST:%s", ulDescribe(recs))
			}
		})
	}
}

// TestULGapShardV2StreamCutRetried wants a /v2/shards stream cut before its result frame retried, as the POST is idempotent.
func TestULGapShardV2StreamCutRetried(t *testing.T) {
	fx := newFixture(t)
	for i, tc := range []struct {
		name string
		kind faultKind
	}{{"reset", resetBody}, {"eof", shortChunked}} {
		t.Run(tc.name, func(t *testing.T) {
			data := pseudoRandom(ulSize, ulSeedGapStream+uint32(i))
			dir, c, _ := ulClient(t, fx)
			fx.fp.arm(ulOnce(ulShard(ulV2), fault{kind: tc.kind, prefix: int64(len(ulFirstFrame))}))
			knownGap(t, "a /v2/shards stream ending before its result frame is terminal, though the POST is idempotent: client/shard.go:84-86, client/shard.go:191-194")
			ctx, _ := testCtx(t, 3*time.Second)
			before := snapshotWorkers()

			res := ulRun(t, ctx, c, ulV2, data)
			ulRequireOK(t, fx, dir, before, data, res)
			if recs := ulSettle(t, fx); len(filter(recs, ulShard(ulV2))) != 2 {
				t.Fatalf("want the cut stream and one retry:%s", ulDescribe(recs))
			}
		})
	}
}

// ulClient builds a client on a fresh cache dir that records its progress; opts override the fixture defaults.
func ulClient(t *testing.T, fx *fixture, opts ...client.Options) (string, *client.Client, *ulProgress) {
	t.Helper()
	dir, prog := t.TempDir(), &ulProgress{events: map[string][]ulEvent{}}
	c := fx.newClient(t, dir, append([]client.Options{client.WithProgressFunc(prog.add)}, opts...)...)
	return dir, c, prog
}

// ulEvent is one progress callback.
type ulEvent struct{ current, total int64 }

// ulProgress records the progress callbacks per name.
type ulProgress struct {
	mu     sync.Mutex
	events map[string][]ulEvent
}

func (p *ulProgress) add(name string, current, total int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events[name] = append(p.events[name], ulEvent{current, total})
}

func (p *ulProgress) of(name string) []ulEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.events[name])
}

// ulResult is how one upload ended.
type ulResult struct {
	hash    xet.FileHash
	err     error
	elapsed time.Duration
}

// ulStart uploads data through api in the background.
func ulStart(ctx context.Context, c *client.Client, api ulAPI, data []byte) <-chan ulResult {
	ch := make(chan ulResult, 1)
	upload := c.UploadFile
	if api == ulV2 {
		upload = c.UploadFileV2
	}
	go func() {
		start := time.Now()
		hash, err := upload(ctx, bytes.NewReader(data))
		ch <- ulResult{hash, err, time.Since(start)}
	}()
	return ch
}

// ulRun uploads data through api and waits for the result.
func ulRun(t *testing.T, ctx context.Context, c *client.Client, api ulAPI, data []byte) ulResult {
	t.Helper()
	return ulAwait(t, ulStart(ctx, c, api, data), waitTimeout)
}

// ulAwait returns the upload's result, failing t if it is still running after within.
func ulAwait(t *testing.T, ch <-chan ulResult, within time.Duration) ulResult {
	t.Helper()
	select {
	case res := <-ch:
		return res
	case <-time.After(within):
		t.Fatalf("upload still running after %v", within)
		return ulResult{}
	}
}

// ulAwaitRecord waits until the fault proxy records a request match picks, failing t if the upload on ch ends first.
func ulAwaitRecord(t *testing.T, fx *fixture, ch <-chan ulResult, match func(record) bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for !slices.ContainsFunc(fx.fp.records(), match) {
		if time.Now().After(deadline) {
			t.Fatalf("no awaited request after %v; requests:%s", waitTimeout, ulDescribe(fx.fp.records()))
		}
		select {
		case res := <-ch:
			t.Fatalf("upload ended before the awaited request: %v; requests:%s", res.err, ulDescribe(fx.fp.records()))
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// ulOnce faults the first request match picks in the phase.
func ulOnce(match func(record) bool, f fault) rule {
	return func(r record) fault {
		if match(r) && r.Seq == 0 {
			return f
		}
		return fault{}
	}
}

// ulAlways faults every request match picks.
func ulAlways(match func(record) bool, f fault) rule {
	return func(r record) fault {
		if match(r) {
			return f
		}
		return fault{}
	}
}

func ulXorbPost(r record) bool {
	return r.Method == http.MethodPost && strings.HasPrefix(r.Path, "/v1/xorbs/")
}

func ulXorbHead(r record) bool {
	return r.Method == http.MethodHead && strings.HasPrefix(r.Path, "/v1/xorbs/")
}

func ulBatchQuery(r record) bool {
	return r.Method == http.MethodPost && r.Path == "/v1/chunks/default:query"
}

// ulChunkGet picks the GET of the dedup shard behind one chunk.
func ulChunkGet(r record) bool {
	return r.Method == http.MethodGet && strings.HasPrefix(r.Path, "/v1/chunks/")
}

func ulShard(api ulAPI) func(record) bool {
	path := "/v1/shards"
	if api == ulV2 {
		path = "/v2/shards"
	}
	return func(r record) bool { return r.Method == http.MethodPost && r.Path == path }
}

// ulXorbHash is the hash a /v1/xorbs/ request names.
func ulXorbHash(r record) string {
	return r.Path[strings.LastIndexByte(r.Path, '/')+1:]
}

// ulLabel names r by upload endpoint and status, as in "post 200".
func ulLabel(r record) string {
	name := r.Method + " " + r.Path
	switch {
	case ulBatchQuery(r):
		name = "query"
	case ulChunkGet(r):
		name = "chunk"
	case ulXorbHead(r):
		name = "head"
	case ulXorbPost(r):
		name = "post"
	case ulShard(ulV1)(r):
		name = "shard1"
	case ulShard(ulV2)(r):
		name = "shard2"
	}
	return fmt.Sprintf("%s %d", name, r.Status)
}

// ulDescribe lists recs one per line for failure messages.
func ulDescribe(recs []record) string {
	var b strings.Builder
	for _, r := range recs {
		fmt.Fprintf(&b, "\n\t%s %s seq=%d fault=%s status=%d reqBytes=%d bytes=%d open=%t", r.Method, r.Path, r.Seq, r.Fault, r.Status, r.ReqBytes, r.Bytes, r.End.IsZero())
	}
	return b.String()
}

// ulSettle waits for every proxied request to finish, naming the open ones on timeout, and returns the phase's records.
func ulSettle(t *testing.T, fx *fixture) []record {
	t.Helper()
	if err := fx.fp.waitErr(func(_ []record, inflight int) bool { return inflight == 0 }); err != nil {
		t.Fatalf("%v:%s", err, ulDescribe(fx.fp.records()))
	}
	return fx.fp.records()
}

// ulRequireTrace fails t unless recs carry exactly the labels want, in arrival order.
func ulRequireTrace(t *testing.T, recs []record, want ...string) {
	t.Helper()
	got := make([]string, len(recs))
	for i, r := range recs {
		got[i] = ulLabel(r)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("requests %q; want %q:%s", got, want, ulDescribe(recs))
	}
}

// ulRequireProgressOnce fails t unless post's xorb reported (0, n) then (n, n), n being the body post carried.
func ulRequireProgressOnce(t *testing.T, prog *ulProgress, post record) {
	t.Helper()
	hash, n := ulXorbHash(post), post.ReqBytes
	if got, want := prog.of(hash), []ulEvent{{0, n}, {n, n}}; !slices.Equal(got, want) {
		t.Fatalf("progress of xorb %s = %v; want %v", hash, got, want)
	}
}

func ulRequireNoStaging(t *testing.T, dir string) {
	t.Helper()
	if left := stagingFiles(t, dir); len(left) != 0 {
		t.Fatalf("staged xorbs left behind: %q", left)
	}
}

// ulRequireOK fails t unless the upload succeeded, the server stores data under its hash, and the client left no staging files or workers.
func ulRequireOK(t *testing.T, fx *fixture, dir string, before workerSet, data []byte, res ulResult) {
	t.Helper()
	if res.err != nil {
		t.Fatalf("upload failed after %v: %v; requests:%s", res.elapsed.Round(time.Millisecond), res.err, ulDescribe(fx.fp.records()))
	}
	fx.requireStored(t, data)
	if !fx.serverHas(t, res.hash) {
		t.Fatalf("the server has no shard for the returned hash %s", res.hash)
	}
	ulRequireNoStaging(t, dir)
	fx.noLeakedWorkers(t, before)
}
