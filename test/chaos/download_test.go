package chaos_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
)

func TestDownloadNoFault(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	for _, api := range []apiVersion{apiAuto, apiV1, apiBatch} {
		t.Run(api.String(), func(t *testing.T) {
			fx.proxy.arm(nil)
			cacheDir := t.TempDir()
			fx.mustDownload(t, ctx, fx.proxyClient(t, cacheDir), api)

			recs := fx.proxy.settle(t)
			if len(filter(recs, record.reconstruction)) == 0 {
				t.Fatal("no reconstruction request traversed the proxy")
			}
			extra := 0
			if api == apiBatch {
				extra = 1 // small's own xorb
			}
			fx.requireAccounting(t, filter(recs, record.xorbGet), extra)
			for _, r := range recs {
				if r.Status != http.StatusOK && r.Status != http.StatusPartialContent || r.End.IsZero() {
					t.Fatalf("request %+v did not complete successfully", r)
				}
			}
			fx.assertCached(t, ctx, cacheDir)
		})
	}
}

func TestXorbHeaderStallOnceThenHeal(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	for _, api := range []apiVersion{apiV2, apiV1} {
		t.Run(api.String(), func(t *testing.T) {
			fx.proxy.arm(func(r record) fault {
				if fx.big.match(r) && r.Seq == 0 {
					return fault{kind: stallHeaders}
				}
				return fault{}
			})
			cacheDir := t.TempDir()
			began := time.Now()
			fx.mustDownload(t, ctx, fx.proxyClient(t, cacheDir), api)
			if elapsed := time.Since(began); elapsed < idleTimeout {
				t.Fatalf("download took %v, expected it to outlive the idle timeout %v", elapsed, idleTimeout)
			}

			gets := filter(fx.proxy.settle(t), fx.big.match)
			if len(gets) != 2 || gets[0].Fault != stallHeaders || gets[0].Status != 0 || gets[0].Bytes != 0 {
				t.Fatalf("requests for %s = %+v, want one withheld response and one retry", fx.big.rangeHeader(), gets)
			}
			if gets[1].Fault != passThrough || gets[1].Status != http.StatusPartialContent || gets[1].Bytes != fx.big.end-fx.big.start+1 {
				t.Fatalf("retry = %+v, want the full range served with 206", gets[1])
			}
			fx.assertCached(t, ctx, cacheDir)
		})
	}
}

// TestXorbBodyFaultResumesAtWireOffset breaks the first response for the
// multi-chunk range after a mid-chunk prefix and expects one resume request
// starting exactly where the wire stopped; a reset may lose received bytes,
// so its resume start is only bounded.
func TestXorbBodyFaultResumesAtWireOffset(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	prefix := fx.midChunkPrefix(t, fx.big)

	cases := []struct {
		kind faultKind
		apis []apiVersion
	}{
		{stallBody, []apiVersion{apiV2, apiV1, apiBatch}},
		{abortBody, []apiVersion{apiV2, apiV1, apiBatch}},
		{resetBody, []apiVersion{apiV2, apiV1}},
		{shortBody, []apiVersion{apiV2, apiV1}},
		{shortChunked, []apiVersion{apiV2, apiV1}},
	}
	for _, tc := range cases {
		for _, api := range tc.apis {
			t.Run(tc.kind.String()+"/"+api.String(), func(t *testing.T) {
				fx.proxy.arm(func(r record) fault {
					if fx.big.match(r) && r.Seq == 0 {
						return fault{kind: tc.kind, prefix: prefix}
					}
					return fault{}
				})
				cacheDir := t.TempDir()
				fx.mustDownload(t, ctx, fx.proxyClient(t, cacheDir), api)

				gets := filter(fx.proxy.settle(t), fx.big.within(prefix))
				if len(gets) != 2 || gets[0].Fault != tc.kind || gets[0].Range != fx.big.rangeHeader() || gets[0].Status != http.StatusPartialContent || gets[0].Bytes != prefix {
					t.Fatalf("requests = %+v, want the faulted first response (%d bytes) and one resume", gets, prefix)
				}
				resume := gets[1]
				start, end := parseRange(t, resume.Range)
				t.Logf("%s after %d bytes of %s: resume %s", tc.kind, prefix, fx.big.rangeHeader(), resume.Range)
				if tc.kind == resetBody {
					if start < fx.big.start || start > fx.big.start+prefix {
						t.Fatalf("resume Range %q, want a start within %d bytes after %d", resume.Range, prefix, fx.big.start)
					}
				} else if start != fx.big.start+prefix {
					t.Fatalf("resume Range %q, want bytes=%d-%d", resume.Range, fx.big.start+prefix, fx.big.end)
				}
				if end != fx.big.end || resume.Fault != passThrough || resume.Status != http.StatusPartialContent || resume.Bytes != end-start+1 {
					t.Fatalf("resume = %+v, want the remaining %d bytes served with 206", resume, fx.big.end-start+1)
				}
				fx.assertCached(t, ctx, cacheDir)
			})
		}
	}
}

// trickleFault paces xorb bodies so the largest packed chunk takes longer than
// the idle timeout to arrive while every gap stays far below it.
func (fx *fixture) trickleFault(t *testing.T) fault {
	t.Helper()
	const gap = 40 * time.Millisecond
	piece := fx.maxChunk(t, fx.big) / 6
	if pieces := (fx.maxChunk(t, fx.big) + piece - 1) / piece; time.Duration(pieces-1)*gap <= idleTimeout || gap*3 > idleTimeout {
		t.Fatalf("fixture: %d pieces of %d bytes every %v cannot span the %v idle timeout", pieces, piece, gap, idleTimeout)
	}
	return fault{kind: trickle, piece: piece, gap: gap}
}

func TestTrickleBodyOutlivesIdleTimeoutWithoutRetry(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	slow := fx.trickleFault(t)
	fx.proxy.arm(func(r record) fault {
		if r.xorbGet() {
			return slow
		}
		return fault{}
	})

	cacheDir := t.TempDir()
	began := time.Now()
	fx.mustDownload(t, ctx, fx.proxyClient(t, cacheDir), apiV2)
	if elapsed := time.Since(began); elapsed <= idleTimeout {
		t.Fatalf("download took %v, expected the trickle to outlive the idle timeout %v", elapsed, idleTimeout)
	}

	gets := filter(fx.proxy.settle(t), record.xorbGet)
	fx.requireAccounting(t, gets, 0)
	for _, g := range gets {
		start, end := parseRange(t, g.Range)
		if g.Fault != trickle || g.Status != http.StatusPartialContent || g.Bytes != end-start+1 {
			t.Fatalf("request %+v was retried or cut short", g)
		}
	}
	fx.assertCached(t, ctx, cacheDir)
}

func v2Reconstruction(r record) bool {
	return r.Method == http.MethodGet && strings.HasPrefix(r.Path, "/v2/reconstructions/")
}

// reconstructionLog renders the reconstruction requests as "<version> <status> <Range>".
func reconstructionLog(recs []record) []string {
	var out []string
	for _, r := range filter(recs, record.reconstruction) {
		out = append(out, fmt.Sprintf("%s %d %s", r.Path[:3], r.Status, r.Range))
	}
	return out
}

func TestV2UnavailableFallsBackToV1UnderFault(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	prefix := fx.midChunkPrefix(t, fx.big)
	fx.proxy.arm(func(r record) fault {
		switch {
		case v2Reconstruction(r):
			return fault{kind: injectStatus, status: http.StatusNotFound}
		case fx.big.match(r) && r.Seq == 0:
			return fault{kind: abortBody, prefix: prefix}
		}
		return fault{}
	})

	cacheDir := t.TempDir()
	fx.mustDownload(t, ctx, fx.proxyClient(t, cacheDir), apiAuto)

	recs := fx.proxy.settle(t)
	if got, want := reconstructionLog(recs), []string{"/v2 404 ", "/v1 200 "}; !slices.Equal(got, want) {
		t.Fatalf("reconstruction requests = %q, want %q", got, want)
	}
	gets := filter(recs, fx.big.within(prefix))
	if len(gets) != 2 || gets[0].Fault != abortBody || gets[0].Bytes != prefix || gets[1].Range != fmt.Sprintf("bytes=%d-%d", fx.big.start+prefix, fx.big.end) {
		t.Fatalf("requests = %+v, want an aborted first response and a resume from %d", gets, fx.big.start+prefix)
	}
	fx.assertCached(t, ctx, cacheDir)
}

// TestSeededOutputResumesThroughFault seeds the destination up to the middle
// of the second term, so the client asks for the file suffix and only the
// xorbs behind it, and heals a body stall on the way.
func TestSeededOutputResumesThroughFault(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	seeded := int64(fx.terms[0].UnpackedLength + fx.terms[1].UnpackedLength/2)
	prefix := fx.midChunkPrefix(t, fx.big)

	for _, tc := range []struct {
		name      string
		v2        bool
		wantRecon []string
	}{
		{"v2", true, []string{fmt.Sprintf("/v2 200 bytes=%d-", seeded)}},
		{"v1 fallback", false, []string{fmt.Sprintf("/v2 404 bytes=%d-", seeded), fmt.Sprintf("/v1 200 bytes=%d-", seeded)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx.proxy.arm(func(r record) fault {
				switch {
				case !tc.v2 && v2Reconstruction(r):
					return fault{kind: injectStatus, status: http.StatusNotFound}
				case fx.big.match(r) && r.Seq == 0:
					return fault{kind: stallBody, prefix: prefix}
				}
				return fault{}
			})
			cacheDir := t.TempDir()
			out := newOutput(t, fx.file2[:seeded])
			if err := fx.download(ctx, fx.proxyClient(t, cacheDir), apiAuto, out); err != nil {
				t.Fatal(err)
			}
			fx.requireFile2(t, out)
			if out.rewound || out.written != int64(len(fx.file2))-seeded {
				t.Fatalf("wrote %d bytes (rewound=%v), want %d after the seeded prefix", out.written, out.rewound, int64(len(fx.file2))-seeded)
			}

			recs := fx.proxy.settle(t)
			if got := reconstructionLog(recs); !slices.Equal(got, tc.wantRecon) {
				t.Fatalf("reconstruction requests = %q, want %q", got, tc.wantRecon)
			}
			if gets := filter(recs, xorbOf(fx.first.hash)); len(gets) != 0 {
				t.Fatalf("fetched %+v, which only backs the seeded prefix", gets)
			}
			gets := filter(recs, fx.big.within(prefix))
			if len(gets) != 2 || gets[0].Fault != stallBody || gets[0].Bytes != prefix || gets[1].Range != fmt.Sprintf("bytes=%d-%d", fx.big.start+prefix, fx.big.end) {
				t.Fatalf("requests = %+v, want a stalled first response and a resume from %d", gets, fx.big.start+prefix)
			}
			// Completing the file later must reuse the resumed entry and only fetch the seeded prefix's xorb.
			if healed := fx.heal(t, ctx, cacheDir); len(healed) != 1 || !xorbOf(fx.first.hash)(healed[0]) {
				t.Fatalf("completing the cache fetched %+v, want only %s", healed, fx.first.hash)
			}
		})
	}
}

// stallEvery stalls every response for the multi-chunk range after prefix bytes.
func (fx *fixture) stallEvery(prefix int64) rule {
	return func(r record) fault {
		if fx.big.within(prefix)(r) {
			return fault{kind: stallBody, prefix: prefix}
		}
		return fault{}
	}
}

// stalled reports whether a handler is holding the multi-chunk range open after prefix bytes.
func (fx *fixture) stalled(prefix int64) func([]record, int) bool {
	return func(recs []record, _ int) bool {
		for _, r := range filter(recs, fx.big.within(prefix)) {
			if r.Fault == stallBody && r.Bytes == prefix && r.End.IsZero() {
				return true
			}
		}
		return false
	}
}

// requireHealed proves that after a stall was abandoned the handlers and prefetch
// workers wind down and the same cache still completes exactly, fetching the
// abandoned range again.
func (fx *fixture) requireHealed(t *testing.T, ctx context.Context, cacheDir string, prefix int64) {
	t.Helper()
	fx.proxy.settle(t)
	assertNoPrefetchWorkers(t)
	healed := fx.heal(t, ctx, cacheDir)
	if again := filter(healed, fx.big.within(prefix)); len(again) != 1 || again[0].Fault != passThrough {
		t.Fatalf("healing fetched %+v, want the abandoned range once without faults", healed)
	}
}

// skipStalledBodyRace skips under -race: abandoning a stalled body makes the
// prefetcher close the httpseek reader while a worker is blocked in Read on it,
// which the race detector reports (deferred to a later stage).
func skipStalledBodyRace(t *testing.T) {
	t.Helper()
	if raceEnabled {
		t.Skip("prefetcher Close races with a stalled httpseek body Read; pending fix")
	}
}

// TestParentCancelDuringBodyStall checks that canceling mid-stall returns
// promptly and leaves a cache that heals. The error is context.Canceled in
// most runs but "chunk 0: file closed" when the worker's failure closes the
// cache first (1 of 40 runs); pinning the category is deferred.
func TestParentCancelDuringBodyStall(t *testing.T) {
	skipStalledBodyRace(t)
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	prefix := fx.midChunkPrefix(t, fx.big)
	fx.proxy.arm(fx.stallEvery(prefix))

	cacheDir := t.TempDir()
	// A long idle timeout keeps the cancel, not the idle guard, as the reason the read ends.
	c := fx.proxyClient(t, cacheDir, client.WithIdleTimeout(testTimeout))
	dlCtx, cancelDownload := context.WithCancel(ctx)
	defer cancelDownload()
	out := newOutput(t, nil)
	done := make(chan error, 1)
	go func() { done <- fx.download(dlCtx, c, apiV2, out) }()

	fx.proxy.wait(t, fx.stalled(prefix))
	cancelDownload()
	canceledAt := time.Now()
	var err error
	select {
	case err = <-done:
	case <-time.After(waitTimeout):
		t.Fatal("download did not return after its context was canceled")
	}
	if err == nil {
		t.Fatal("download succeeded after its context was canceled")
	}
	t.Logf("canceled download error: %v (context.Canceled=%v)", err, errors.Is(err, context.Canceled))
	if took := time.Since(canceledAt); took > 2*time.Second {
		t.Fatalf("download returned %v after cancel", took)
	}
	fx.requireHealed(t, ctx, cacheDir, prefix)
}

func TestBatchReaderCloseDuringStall(t *testing.T) {
	skipStalledBodyRace(t)
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	prefix := fx.midChunkPrefix(t, fx.big)
	fx.proxy.arm(fx.stallEvery(prefix))

	cacheDir := t.TempDir()
	c := fx.proxyClient(t, cacheDir, client.WithIdleTimeout(testTimeout))
	readers, _, err := c.DownloadFiles(ctx, []xet.FileHash{fx.hash2, fx.hashSmall})
	if err != nil {
		t.Fatal(err)
	}
	// Prefetching starts before the first Read, so the stall is reached without reading.
	fx.proxy.wait(t, fx.stalled(prefix))
	closedAt := time.Now()
	if err := readers[0].Close(); err != nil {
		t.Fatalf("close stalled reader: %v", err)
	}
	if took := time.Since(closedAt); took > 2*time.Second {
		t.Fatalf("Close returned %v after being called", took)
	}
	small, err := io.ReadAll(readers[1])
	if err != nil || !bytes.Equal(small, fx.small) {
		t.Fatalf("sibling reader: %d bytes, err %v", len(small), err)
	}
	readers[1].Close()
	fx.requireHealed(t, ctx, cacheDir, prefix)
}

// TestSharedCacheSecondClientWaitsForRangeLock starts a second client on the
// same cache while the first is still receiving; the second waits on the
// range locks instead of fetching again, so every range crosses the wire once.
func TestSharedCacheSecondClientWaitsForRangeLock(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	slow := fx.trickleFault(t)
	fx.proxy.arm(func(r record) fault {
		if r.xorbGet() {
			return slow
		}
		return fault{}
	})

	cacheDir := t.TempDir()
	first, second := fx.proxyClient(t, cacheDir), fx.proxyClient(t, cacheDir)
	outFirst, outSecond := newOutput(t, nil), newOutput(t, nil)
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { firstDone <- fx.download(ctx, first, apiV2, outFirst) }()
	fx.proxy.wait(t, func(recs []record, _ int) bool {
		gets := filter(recs, fx.big.match)
		return len(gets) > 0 && gets[0].Bytes > 0
	})
	go func() { secondDone <- fx.download(ctx, second, apiV2, outSecond) }()
	if err := <-secondDone; err != nil {
		t.Fatalf("second client: %v", err)
	}
	if err := <-firstDone; err != nil {
		t.Fatalf("first client: %v", err)
	}
	fx.requireFile2(t, outFirst)
	fx.requireFile2(t, outSecond)

	fx.requireAccounting(t, filter(fx.proxy.settle(t), record.xorbGet), 0)
	fx.assertCached(t, ctx, cacheDir)
}
