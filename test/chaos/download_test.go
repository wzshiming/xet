package chaos_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime/pprof"
	"slices"
	"strconv"
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
			fx.mustDownload(t, ctx, fx.proxyClient(t, cacheDir, client.WithIdleTimeout(idleTimeout)), api)
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
				var opts []client.Options
				if tc.kind == stallBody || tc.kind == resetBody { // the client may only notice a reset through the idle guard
					opts = append(opts, client.WithIdleTimeout(idleTimeout))
				}
				fx.mustDownload(t, ctx, fx.proxyClient(t, cacheDir, opts...), api)

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
	fx.mustDownload(t, ctx, fx.proxyClient(t, cacheDir, client.WithIdleTimeout(idleTimeout)), apiV2)
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

// retryableStatuses are answered by retrying, matching xet-core's transient set.
var retryableStatuses = []int{
	http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError,
	http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout,
}

// statusLog lists the status of each record in order.
func statusLog(recs []record) []int {
	out := make([]int, 0, len(recs))
	for _, r := range recs {
		out = append(out, r.Status)
	}
	return out
}

// TestTransientStatusThenHeal answers the reconstruction query and the
// multi-chunk range with a retryable status twice before serving them.
func TestTransientStatusThenHeal(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	for _, status := range retryableStatuses {
		for _, api := range []apiVersion{apiAuto, apiBatch} {
			t.Run(fmt.Sprintf("%d/%s", status, api), func(t *testing.T) {
				fx.proxy.arm(func(r record) fault {
					if (r.reconstruction() || fx.big.match(r)) && r.Seq < 2 {
						return fault{kind: injectStatus, status: status}
					}
					return fault{}
				})
				cacheDir := t.TempDir()
				fx.mustDownload(t, ctx, fx.proxyClient(t, cacheDir), api)

				recs := fx.proxy.settle(t)
				if got, want := statusLog(filter(recs, record.reconstruction)), []int{status, status, http.StatusOK}; !slices.Equal(got, want) {
					t.Fatalf("reconstruction statuses = %v, want %v", got, want)
				}
				gets := filter(recs, fx.big.match)
				if got, want := statusLog(gets), []int{status, status, http.StatusPartialContent}; !slices.Equal(got, want) {
					t.Fatalf("statuses for %s = %v, want %v: %+v", fx.big.rangeHeader(), got, want, gets)
				}
				if last := gets[2]; last.Fault != passThrough || last.Bytes != fx.big.end-fx.big.start+1 {
					t.Fatalf("third request = %+v, want the full range served", last)
				}
				fx.assertCached(t, ctx, cacheDir)
			})
		}
	}
}

// TestTransientStatusOnResumeThenHeal aborts the multi-chunk range mid-chunk
// and answers the first two resume requests with 503 before serving them.
func TestTransientStatusOnResumeThenHeal(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	prefix := fx.midChunkPrefix(t, fx.big)
	resume := fmt.Sprintf("bytes=%d-%d", fx.big.start+prefix, fx.big.end)
	fx.proxy.arm(func(r record) fault {
		switch {
		case fx.big.match(r):
			return fault{kind: abortBody, prefix: prefix}
		case fx.big.within(prefix)(r) && r.Range == resume && r.Seq < 2:
			return fault{kind: injectStatus, status: http.StatusServiceUnavailable}
		}
		return fault{}
	})
	cacheDir := t.TempDir()
	fx.mustDownload(t, ctx, fx.proxyClient(t, cacheDir), apiV2)

	gets := filter(fx.proxy.settle(t), fx.big.within(prefix))
	want := []int{http.StatusPartialContent, http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusPartialContent}
	if got := statusLog(gets); !slices.Equal(got, want) {
		t.Fatalf("statuses = %v, want %v: %+v", got, want, gets)
	}
	if gets[0].Bytes != prefix || gets[3].Range != resume || gets[3].Bytes != fx.big.end-fx.big.start-prefix+1 {
		t.Fatalf("requests = %+v, want %d bytes aborted and the rest served by the third resume", gets, prefix)
	}
	fx.assertCached(t, ctx, cacheDir)
}

// TestStatusBudgets pins how many requests a permanent status costs: retryable
// ones retries+1, terminal ones a single request; only a 403 re-queries the
// reconstruction, so every other xorb status leaves it at one query. The cache
// heals either way.
func TestStatusBudgets(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	const retries = 2

	for _, tc := range []struct {
		name                  string
		match                 func(record) bool
		status, want, queries int
	}{
		{"xorb 503", fx.big.match, http.StatusServiceUnavailable, retries + 1, 1},
		{"xorb 401", fx.big.match, http.StatusUnauthorized, 1, 1},
		{"xorb 404", fx.big.match, http.StatusNotFound, 1, 1},
		{"reconstruction 503", record.reconstruction, http.StatusServiceUnavailable, retries + 1, retries + 1},
		{"reconstruction 401", record.reconstruction, http.StatusUnauthorized, 1, 1},
		{"reconstruction 404", record.reconstruction, http.StatusNotFound, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx.proxy.arm(func(r record) fault {
				if tc.match(r) {
					return fault{kind: injectStatus, status: tc.status}
				}
				return fault{}
			})
			cacheDir := t.TempDir()
			err := fx.download(ctx, fx.proxyClient(t, cacheDir, client.WithRetries(retries)), apiV1, newOutput(t, nil))
			if err == nil || !strings.Contains(err.Error(), strconv.Itoa(tc.status)) {
				t.Fatalf("download error = %v, want status %d reported", err, tc.status)
			}
			recs := fx.proxy.settle(t)
			got := statusLog(filter(recs, tc.match))
			if len(got) != tc.want || slices.Contains(got, http.StatusOK) || slices.Contains(got, http.StatusPartialContent) {
				t.Fatalf("%s answered %v, want %d attempts", tc.name, got, tc.want)
			}
			if got := len(filter(recs, record.reconstruction)); got != tc.queries {
				t.Fatalf("%s led to %d reconstruction queries, want %d", tc.name, got, tc.queries)
			}
			if healed := filter(fx.heal(t, ctx, cacheDir), fx.big.match); len(healed) != 1 {
				t.Fatalf("healing fetched %+v, want the multi-chunk range once", healed)
			}
		})
	}
}

const reconstructionPrefix = 16

func TestReconstructionBodyFaultThenHeal(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	cases := []struct {
		kind faultKind
		apis []apiVersion
	}{
		{abortBody, []apiVersion{apiV1, apiV2, apiBatch}},
		{shortBody, []apiVersion{apiV1}},
		{shortChunked, []apiVersion{apiV2}},
		{resetBody, []apiVersion{apiV1}},
		{stallBody, []apiVersion{apiV2, apiBatch}},
	}
	for _, tc := range cases {
		for _, api := range tc.apis {
			t.Run(tc.kind.String()+"/"+api.String(), func(t *testing.T) {
				fx.proxy.arm(func(r record) fault {
					if r.reconstruction() && r.Seq == 0 {
						return fault{kind: tc.kind, prefix: reconstructionPrefix}
					}
					return fault{}
				})
				cacheDir := t.TempDir()
				var opts []client.Options
				if tc.kind == stallBody || tc.kind == resetBody {
					opts = append(opts, client.WithIdleTimeout(idleTimeout))
				}
				fx.mustDownload(t, ctx, fx.proxyClient(t, cacheDir, opts...), api)

				recs := filter(fx.proxy.settle(t), record.reconstruction)
				if len(recs) != 2 || recs[0].Fault != tc.kind || recs[0].Status != http.StatusOK || recs[0].Bytes != reconstructionPrefix {
					t.Fatalf("reconstruction requests = %+v, want the faulted answer (%d bytes) and one retry", recs, reconstructionPrefix)
				}
				if retry := recs[1]; retry.Fault != passThrough || retry.Status != http.StatusOK || retry.Query != recs[0].Query || retry.Range != recs[0].Range {
					t.Fatalf("retry = %+v, want the same query served whole", retry)
				}
				fx.assertCached(t, ctx, cacheDir)
			})
		}
	}
}

func TestReconstructionBodyFaultBudget(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	const retries = 2

	t.Run("status then cut then heal", func(t *testing.T) {
		fx.proxy.arm(func(r record) fault {
			switch {
			case r.reconstruction() && r.Seq == 0:
				return fault{kind: injectStatus, status: http.StatusServiceUnavailable}
			case r.reconstruction() && r.Seq == 1:
				return fault{kind: abortBody, prefix: reconstructionPrefix}
			}
			return fault{}
		})
		cacheDir := t.TempDir()
		fx.mustDownload(t, ctx, fx.proxyClient(t, cacheDir, client.WithRetries(retries)), apiV1)

		recs := filter(fx.proxy.settle(t), record.reconstruction)
		want := []int{http.StatusServiceUnavailable, http.StatusOK, http.StatusOK}
		if got := statusLog(recs); !slices.Equal(got, want) || recs[1].Bytes != reconstructionPrefix {
			t.Fatalf("reconstruction requests = %+v, want statuses %v with the second cut after %d bytes", recs, want, reconstructionPrefix)
		}
		fx.assertCached(t, ctx, cacheDir)
	})

	t.Run("permanent cut", func(t *testing.T) {
		fx.proxy.arm(func(r record) fault {
			if r.reconstruction() {
				return fault{kind: abortBody, prefix: reconstructionPrefix}
			}
			return fault{}
		})
		cacheDir := t.TempDir()
		err := fx.download(ctx, fx.proxyClient(t, cacheDir, client.WithRetries(retries)), apiV1, newOutput(t, nil))
		if !errors.Is(err, io.ErrUnexpectedEOF) || ctx.Err() != nil {
			t.Fatalf("download error = %v, want the cut body reported before the context ends", err)
		}
		if recs := filter(fx.proxy.settle(t), record.reconstruction); len(recs) != retries+1 {
			t.Fatalf("reconstruction requests = %+v, want %d attempts", recs, retries+1)
		}
		fx.heal(t, ctx, cacheDir)
	})
}

// TestRetryBackoffSpacesAttempts answers the reconstruction query and the
// multi-chunk range with 503 three times: every retry must start no sooner
// than half the doubled base after the previous answer, the jitter floor.
func TestRetryBackoffSpacesAttempts(t *testing.T) {
	const base = 40 * time.Millisecond
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	fx.proxy.arm(func(r record) fault {
		if (r.reconstruction() || fx.big.match(r)) && r.Seq < 3 {
			return fault{kind: injectStatus, status: http.StatusServiceUnavailable}
		}
		return fault{}
	})
	cacheDir := t.TempDir()
	fx.mustDownload(t, ctx, fx.proxyClient(t, cacheDir, client.WithRetryBackoff(base)), apiV1)

	recs := fx.proxy.settle(t)
	for _, path := range []struct {
		name string
		recs []record
		ok   int
	}{
		{"reconstruction", filter(recs, record.reconstruction), http.StatusOK},
		{"xorb", filter(recs, fx.big.match), http.StatusPartialContent},
	} {
		if got, want := statusLog(path.recs), []int{503, 503, 503, path.ok}; !slices.Equal(got, want) {
			t.Fatalf("%s statuses = %v, want %v", path.name, got, want)
		}
		for i := 1; i < len(path.recs); i++ {
			gap, floor := path.recs[i].Start.Sub(path.recs[i-1].End), base<<(i-1)/2
			t.Logf("%s retry %d started %v after the previous answer (floor %v)", path.name, i, gap, floor)
			if gap < floor {
				t.Fatalf("%s retry %d started %v after the previous answer, want at least %v", path.name, i, gap, floor)
			}
		}
	}
	fx.assertCached(t, ctx, cacheDir)
}

// TestWrongContentRangeOnResumeFails aborts the multi-chunk range mid-chunk and
// answers its resume with a 206 for a shifted range: the download must fail
// without accepting the bytes or re-asking for the same deterministic answer.
func TestWrongContentRangeOnResumeFails(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	prefix := fx.midChunkPrefix(t, fx.big)
	fx.proxy.arm(func(r record) fault {
		switch {
		case fx.big.match(r):
			return fault{kind: abortBody, prefix: prefix}
		case fx.big.within(prefix)(r):
			return fault{kind: shiftRange}
		}
		return fault{}
	})
	cacheDir := t.TempDir()
	err := fx.download(ctx, fx.proxyClient(t, cacheDir), apiV2, newOutput(t, nil))
	if err == nil || !strings.Contains(err.Error(), "Content-Range") {
		t.Fatalf("download error = %v, want the Content-Range mismatch reported", err)
	}
	gets := filter(fx.proxy.settle(t), fx.big.within(prefix))
	if len(gets) != 2 || gets[0].Fault != abortBody || gets[1].Fault != shiftRange || gets[1].Status != http.StatusPartialContent {
		t.Fatalf("requests = %+v, want the aborted response and a single shifted resume", gets)
	}
	if healed := filter(fx.heal(t, ctx, cacheDir), fx.big.within(prefix)); len(healed) != 1 {
		t.Fatalf("healing fetched %+v, want the multi-chunk range once", healed)
	}
}

// TestRangeAnswersFailSafely serves the multi-chunk range with mismatched
// range metadata: the whole xorb as 200, or a 206 describing a shifted range.
// Both must fail at once instead of being taken for the requested bytes, while
// a 206 lacking Content-Range is still accepted as is.
func TestRangeAnswersFailSafely(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	for _, tc := range []struct {
		kind    faultKind
		wantErr string // substring of the failure; "" means the download must succeed
		gets    int    // requests for the range, counting the one-shot fallback
	}{
		{ignoreRange, "status 200 OK", 1},
		{shiftRange, "Content-Range", 1},
		{noContentRange, "", 2},
	} {
		t.Run(tc.kind.String(), func(t *testing.T) {
			fx.proxy.arm(func(r record) fault {
				if fx.big.match(r) {
					return fault{kind: tc.kind}
				}
				return fault{}
			})
			cacheDir := t.TempDir()
			err := fx.download(ctx, fx.proxyClient(t, cacheDir), apiV2, newOutput(t, nil))
			if tc.wantErr == "" && err != nil {
				t.Fatalf("download: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("download error = %v, want %q reported", err, tc.wantErr)
			}
			gets := filter(fx.proxy.settle(t), fx.big.match)
			if len(gets) != tc.gets {
				t.Fatalf("requests for %s = %+v, want %d", fx.big.rangeHeader(), gets, tc.gets)
			}
			if tc.wantErr == "" {
				fx.assertCached(t, ctx, cacheDir)
			} else if healed := filter(fx.heal(t, ctx, cacheDir), fx.big.match); len(healed) != 1 {
				t.Fatalf("healing fetched %+v, want the multi-chunk range once", healed)
			}
		})
	}
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
			if err := fx.download(ctx, fx.proxyClient(t, cacheDir, client.WithIdleTimeout(idleTimeout)), apiAuto, out); err != nil {
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

// TestParentCancelDuringBodyStall checks that canceling mid-stall returns
// promptly with context.Canceled and leaves a cache that heals.
func TestParentCancelDuringBodyStall(t *testing.T) {
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
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("download error = %v, want context.Canceled", err)
	}
	if took := time.Since(canceledAt); took > 2*time.Second {
		t.Fatalf("download returned %v after cancel", took)
	}
	fx.requireHealed(t, ctx, cacheDir, prefix)
}

func TestBatchReaderCloseDuringStall(t *testing.T) {
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

// urlGen matches xorb GETs through URLs tagged with generation n.
func urlGen(n int) func(record) bool {
	return func(r record) bool { return r.xorbGet() && r.Query == fmt.Sprintf("gen=%d", n) }
}

// file2Xorb reports whether r fetches one of file2's xorbs rather than small's.
func (fx *fixture) file2Xorb(r record) bool {
	for _, term := range fx.terms {
		if xorbOf(term.Hash)(r) {
			return true
		}
	}
	return false
}

// prefetchWorkers counts the running prefetch worker goroutines.
func prefetchWorkers() int {
	var buf bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&buf, 2)
	return strings.Count(buf.String(), "download.(*prefetcher).runJob(")
}

// waitFor polls cond until it holds or waitTimeout passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(waitTimeout); !cond(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %v", what, waitTimeout)
		}
	}
}

// rangeKeys lists "<path> <Range>" of recs in order, so two phases can be compared range by range.
func rangeKeys(recs []record) []string {
	out := make([]string, 0, len(recs))
	for _, r := range recs {
		out = append(out, r.Path+" "+r.Range)
	}
	slices.Sort(out)
	return out
}

// expireGen tags the xorb URLs of every reconstruction answer with its sequence
// number and refuses the generations below n as an expired signed URL would.
func expireGen(n int) rule {
	return func(r record) fault {
		switch {
		case r.reconstruction():
			return fault{kind: tagURLs, tag: r.Seq}
		case r.xorbGet():
			var gen int
			if _, err := fmt.Sscanf(r.Query, "gen=%d", &gen); err != nil || gen < n {
				return fault{kind: injectStatus, status: http.StatusForbidden}
			}
		}
		return fault{}
	}
}

// TestForbiddenURLRefreshedThenHeal refuses the first URL generation: the
// client must repeat the reconstruction query once, unchanged, and fetch every
// range through the new URLs without rewinding a seeded output. Ranges that
// start after the refresh take the new URL directly, so with one worker only
// the first range is ever refused.
func TestForbiddenURLRefreshedThenHeal(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	seeded := int64(fx.terms[0].UnpackedLength + fx.terms[1].UnpackedLength/2)

	for _, tc := range []struct {
		api     apiVersion
		seeded  int64
		workers int
	}{{apiV1, 0, 4}, {apiV2, 0, 4}, {apiBatch, 0, 4}, {apiV2, seeded, 4}, {apiV1, 0, 1}, {apiBatch, 0, 1}} {
		t.Run(fmt.Sprintf("%s/seeded=%d/workers=%d", tc.api, tc.seeded, tc.workers), func(t *testing.T) {
			fx.proxy.arm(expireGen(1))
			cacheDir := t.TempDir()
			out := newOutput(t, fx.file2[:tc.seeded])
			if err := fx.download(ctx, fx.proxyClient(t, cacheDir, client.WithConcurrency(tc.workers)), tc.api, out); err != nil {
				t.Fatal(err)
			}
			fx.requireFile2(t, out)
			if out.rewound || out.written != int64(len(fx.file2))-tc.seeded {
				t.Fatalf("wrote %d bytes (rewound=%v), want %d after the seeded prefix", out.written, out.rewound, int64(len(fx.file2))-tc.seeded)
			}

			recs := fx.proxy.settle(t)
			recon := filter(recs, record.reconstruction)
			if len(recon) != 2 || recon[1].Path != recon[0].Path || recon[1].Query != recon[0].Query || recon[1].Range != recon[0].Range {
				t.Fatalf("reconstruction requests = %+v, want the same query repeated once", recon)
			}
			gets := filter(recs, record.xorbGet)
			refused, served := filter(gets, urlGen(0)), filter(gets, urlGen(1))
			if len(refused)+len(served) != len(gets) || len(refused) == 0 {
				t.Fatalf("xorb GETs = %+v, want the first generation refused and the second served", gets)
			}
			for _, g := range gets {
				if g.Fault == injectStatus && g.Status != http.StatusForbidden || g.Fault == passThrough && g.Status != http.StatusPartialContent {
					t.Fatalf("xorb GET %+v, want generation 0 refused and generation 1 served", g)
				}
			}
			for _, key := range rangeKeys(refused) {
				if !slices.Contains(rangeKeys(served), key) {
					t.Fatalf("refused range %q was never fetched through the new URL: %+v", key, gets)
				}
			}
			if tc.workers == 1 {
				if late := filter(refused, func(r record) bool { return fx.file2Xorb(r) && !fx.first.match(r) }); len(late) != 0 {
					t.Fatalf("ranges refused after the first: %+v, want them started at the new URL", late)
				}
			}
			if tc.seeded == 0 {
				extra := 0
				if tc.api == apiBatch {
					extra = 1 // small's own xorb
				}
				fx.requireAccounting(t, served, extra)
				fx.assertCached(t, ctx, cacheDir)
				return
			}
			if gets := filter(recs, xorbOf(fx.first.hash)); len(gets) != 0 {
				t.Fatalf("fetched %+v, which only backs the seeded prefix", gets)
			}
			if healed := fx.heal(t, ctx, cacheDir); len(healed) != 1 || !xorbOf(fx.first.hash)(healed[0]) {
				t.Fatalf("completing the cache fetched %+v, want only %s", healed, fx.first.hash)
			}
		})
	}
}

// TestForbiddenURLBudget refuses the multi-chunk range in every URL generation:
// it costs retries+1 GETs with one reconstruction query per retry before the
// 403 is reported, the served range is not fetched again, and WithRetries(0)
// never re-queries.
func TestForbiddenURLBudget(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	for _, retries := range []int{0, 2} {
		t.Run(fmt.Sprintf("retries=%d", retries), func(t *testing.T) {
			fx.proxy.arm(func(r record) fault {
				switch {
				case r.reconstruction():
					return fault{kind: tagURLs, tag: r.Seq}
				case fx.big.match(r):
					return fault{kind: injectStatus, status: http.StatusForbidden}
				}
				return fault{}
			})
			cacheDir := t.TempDir()
			err := fx.download(ctx, fx.proxyClient(t, cacheDir, client.WithRetries(retries)), apiV1, newOutput(t, nil))
			if err == nil || !strings.Contains(err.Error(), "403") {
				t.Fatalf("download error = %v, want the 403 reported", err)
			}
			recs := fx.proxy.settle(t)
			if got := statusLog(filter(recs, record.reconstruction)); len(got) != retries+1 || slices.Contains(got, http.StatusForbidden) {
				t.Fatalf("reconstruction statuses = %v, want %d queries", got, retries+1)
			}
			if got := statusLog(filter(recs, fx.big.match)); len(got) != retries+1 || slices.Contains(got, http.StatusPartialContent) {
				t.Fatalf("statuses for %s = %v, want %d refused attempts", fx.big.rangeHeader(), got, retries+1)
			}
			if got := statusLog(filter(recs, fx.first.match)); !slices.Equal(got, []int{http.StatusPartialContent}) {
				t.Fatalf("statuses for %s = %v, want the range served once", fx.first.rangeHeader(), got)
			}
			// Tiny sibling ranges of the refused xorb may still be in flight when the reader fails, so only the served xorb is pinned.
			healed := fx.heal(t, ctx, cacheDir)
			if len(filter(healed, fx.big.match)) != 1 || len(filter(healed, xorbOf(fx.first.hash))) != 0 {
				t.Fatalf("healing fetched %+v, want the refused range once and the served xorb never", healed)
			}
		})
	}
}

// TestBatchReaderCloseWhileRefreshStalls refuses every first-generation URL
// and withholds the answer to the first re-query. file2's reader is refused at
// once and issues that query; small's range answers 503 first, so its reader
// is refused only after the backoff and waits for file2's answer. Closing the
// waiting reader must stop its worker without touching the pending query;
// closing the querying reader must let the waiter re-query on its own context
// and finish.
func TestBatchReaderCloseWhileRefreshStalls(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	stalledQuery := func(r record) bool { return r.reconstruction() && r.Fault == stallHeaders && r.End.IsZero() }
	smallRefused := func(r record) bool {
		return r.xorbGet() && !fx.file2Xorb(r) && r.Status == http.StatusForbidden && !r.End.IsZero()
	}
	expired := expireGen(2)
	rule := func(r record) fault {
		switch {
		case r.reconstruction() && r.Seq == 1:
			return fault{kind: stallHeaders}
		case r.xorbGet() && !fx.file2Xorb(r) && r.Seq == 0:
			return fault{kind: injectStatus, status: http.StatusServiceUnavailable}
		}
		return expired(r)
	}

	for _, closeFirst := range []string{"waiter", "leader"} {
		t.Run("close "+closeFirst, func(t *testing.T) {
			fx.proxy.arm(rule)
			cacheDir := t.TempDir()
			c := fx.proxyClient(t, cacheDir, client.WithRetryBackoff(600*time.Millisecond))
			readers, _, err := c.DownloadFiles(ctx, []xet.FileHash{fx.hash2, fx.hashSmall})
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range readers {
				defer r.Close()
			}
			fx.proxy.wait(t, func(recs []record, _ int) bool {
				return len(filter(recs, stalledQuery)) == 1 && len(filter(recs, smallRefused)) == 1
			})
			recs := fx.proxy.records()
			if q, s := filter(recs, stalledQuery)[0], filter(recs, smallRefused)[0]; !q.Start.Before(s.Start) {
				t.Fatalf("fixture: small was refused at %v before the re-query stalled at %v", s.Start, q.Start)
			}
			// file2's workers wait on its answer; small's single worker waits for it too.
			workers := prefetchWorkers()
			if workers < 2 {
				t.Fatalf("%d prefetch workers running, want file2's and small's", workers)
			}

			leader, waiter := readers[0], readers[1]
			if closeFirst == "leader" {
				closedAt := time.Now()
				leader.Close()
				if took := time.Since(closedAt); took > 2*time.Second {
					t.Fatalf("Close returned %v after being called", took)
				}
				if small, err := io.ReadAll(waiter); err != nil || !bytes.Equal(small, fx.small) {
					t.Fatalf("waiting reader after the querying one closed: %d bytes, %v; want small served", len(small), err)
				}
				recs = fx.proxy.settle(t)
				if got := statusLog(filter(recs, record.reconstruction)); !slices.Equal(got, []int{http.StatusOK, 0, http.StatusOK}) {
					t.Fatalf("reconstruction statuses = %v, want the answer, the abandoned re-query and the waiter's own", got)
				}
				if got := filter(recs, func(r record) bool { return !fx.file2Xorb(r) && urlGen(2)(r) }); len(got) != 1 || got[0].Status != http.StatusPartialContent {
					t.Fatalf("small's fetches through its own answer = %+v, want its range served once", got)
				}
			} else {
				closedAt := time.Now()
				waiter.Close()
				if took := time.Since(closedAt); took > 2*time.Second {
					t.Fatalf("Close returned %v after being called", took)
				}
				waitFor(t, "small's worker exit", func() bool { return prefetchWorkers() == workers-1 })
				if stalled := filter(fx.proxy.records(), stalledQuery); len(stalled) != 1 || ctx.Err() != nil {
					t.Fatalf("after closing small: pending queries %+v, ctx %v; want file2's query still pending", stalled, ctx.Err())
				}
				leader.Close()
				recs = fx.proxy.settle(t)
				if got := statusLog(filter(recs, record.reconstruction)); !slices.Equal(got, []int{http.StatusOK, 0}) {
					t.Fatalf("reconstruction statuses = %v, want the answer and one abandoned re-query", got)
				}
			}
			assertNoPrefetchWorkers(t)
			fx.heal(t, ctx, cacheDir)
		})
	}
}

// TestForbiddenOnResumeFailsCleanly aborts the multi-chunk range mid-chunk and
// refuses its resume: a 403 inside a body is not refreshed, so the download
// must fail at once with the status reported, without re-querying, and the
// cache must heal. Only the first request of a range is refreshed.
func TestForbiddenOnResumeFailsCleanly(t *testing.T) {
	fx := newFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	prefix := fx.midChunkPrefix(t, fx.big)
	fx.proxy.arm(func(r record) fault {
		switch {
		case fx.big.match(r):
			return fault{kind: abortBody, prefix: prefix}
		case fx.big.within(prefix)(r):
			return fault{kind: injectStatus, status: http.StatusForbidden}
		}
		return fault{}
	})
	cacheDir := t.TempDir()
	err := fx.download(ctx, fx.proxyClient(t, cacheDir), apiV2, newOutput(t, nil))
	if err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("download error = %v, want the 403 reported", err)
	}
	recs := fx.proxy.settle(t)
	if got := statusLog(filter(recs, fx.big.within(prefix))); !slices.Equal(got, []int{http.StatusPartialContent, http.StatusForbidden}) {
		t.Fatalf("statuses for %s = %v, want the aborted body and one refused resume", fx.big.rangeHeader(), got)
	}
	if n := len(filter(recs, record.reconstruction)); n != 1 {
		t.Fatalf("%d reconstruction queries, want the refused resume left unrefreshed", n)
	}
	if healed := filter(fx.heal(t, ctx, cacheDir), fx.big.within(prefix)); len(healed) != 1 {
		t.Fatalf("healing fetched %+v, want the multi-chunk range once", healed)
	}
}
