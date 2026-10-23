package chaos_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
)

const (
	dlReconPrefix  = 16                 // reconstruction body bytes relayed before a body fault
	dlOpenAttempts = defaultRetries + 2 // Range opens httpseek tries before the handler at client.go:125 gives up
	dlAttempts     = defaultRetries + 1 // doWithNetworkRetry attempts
	dlCancelBound  = 2 * time.Second    // how long an abandoned download may take to return
	dlGenerous     = 5 * time.Second    // how long an impaired download that must still finish may take

	// dlBodyRace names the data race an abandoned body read trips under -race.
	dlBodyRace = "abandoning a stalled body races httpseek: download/prefetcher.go:252 closes the body from context.AfterFunc while runJob's decoder reads it, and OpenSeeker (openseek.go:104, :341) is not safe for concurrent use"
)

// TestDLClean pins a fault-free file2 download per API: one query, every range once, nothing rewound, the cache whole.
func TestDLClean(t *testing.T) {
	fx := newFixture(t)
	for _, api := range []apiVersion{apiAuto, apiV1, apiBatch} {
		t.Run(api.String(), func(t *testing.T) {
			// batch stays cold so small's own xorb is the one extra GET; its merge worker then outlives a leak check.
			warm, extra := fx.small, 0
			if api == apiBatch {
				warm, extra = nil, 1
			}
			r := dlStart(t, fx, warm)
			out := fx.mustDownload(t, r.ctx, r.c, api, fx.file2)

			recs := fx.fp.settle(t)
			if len(filter(recs, record.reconstruction)) == 0 {
				t.Fatal("no reconstruction request traversed the proxy")
			}
			for _, rec := range recs {
				if (rec.Status != http.StatusOK && rec.Status != http.StatusPartialContent) || rec.End.IsZero() {
					t.Fatalf("request %+v did not complete with 200 or 206", rec)
				}
			}
			fx.requireAccounting(t, filter(recs, record.xorbGet), fx.file2, extra)
			if out.rewinds != 0 {
				t.Fatalf("output rewound %d times, want none", out.rewinds)
			}
			if warm != nil {
				fx.noLeakedWorkers(t, r.before)
			}
			fx.assertCached(t, r.ctx, fx.file2, r.cacheDir)
		})
	}
}

// TestDLXorbFaults pins xorb-body recovery: a withheld header costs one idle timeout and one retry,
// a broken body resumes at the wire offset, and a trickle never trips the idle guard.
func TestDLXorbFaults(t *testing.T) {
	fx := newFixture(t)
	big := fx.big

	t.Run("HeaderStallOnce", func(t *testing.T) {
		for _, api := range []apiVersion{apiV2, apiV1} {
			t.Run(api.String(), func(t *testing.T) {
				r := dlStart(t, fx, fx.small, shortIdle)
				fx.fp.arm(dlWhen(dlFirst(big.match), fault{kind: stallHeaders}))
				began := time.Now()
				fx.mustDownload(t, r.ctx, r.c, api, fx.file2)
				if elapsed := time.Since(began); elapsed < idleTimeout {
					t.Fatalf("download took %v, want at least the idle timeout %v", elapsed, idleTimeout)
				}
				gets := filter(fx.fp.settle(t), big.match)
				if len(gets) != 2 || gets[0].Fault != stallHeaders || gets[0].Status != 0 || gets[0].Bytes != 0 {
					t.Fatalf("requests for %s = %+v, want one withheld answer and one retry", big.rangeHeader(), gets)
				}
				if retry := gets[1]; retry.Fault != passThrough || retry.Status != http.StatusPartialContent || retry.Bytes != big.end-big.start+1 {
					t.Fatalf("retry = %+v, want the full range served with 206", retry)
				}
				r.cached(t, fx)
			})
		}
	})

	t.Run("BodyFaultResumes", func(t *testing.T) {
		mid := fx.midChunkPrefix(t, big)
		boundary := dlChunkBoundaryPrefix(t, fx, big, 1)
		for _, tc := range []struct {
			name   string
			kind   faultKind
			api    apiVersion
			prefix int64
		}{
			{"stallBody/v2", stallBody, apiV2, mid},
			{"stallBody/batch", stallBody, apiBatch, mid},
			{"abortBody/v1", abortBody, apiV1, mid},
			{"abortBody/batch", abortBody, apiBatch, mid},
			{"resetBody/v2", resetBody, apiV2, mid},
			{"shortBody/v1", shortBody, apiV1, mid},
			{"shortChunked/v2", shortChunked, apiV2, mid},
			{"shortChunkedAtBoundary/v1", shortChunked, apiV1, boundary},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var opts []client.Options
				if tc.kind == stallBody || tc.kind == resetBody { // a reset may only be noticed through the idle guard
					opts = append(opts, shortIdle)
				}
				r := dlStart(t, fx, fx.small, opts...)
				fx.fp.arm(dlWhen(dlFirst(big.match), fault{kind: tc.kind, prefix: tc.prefix}))
				if out := fx.mustDownload(t, r.ctx, r.c, tc.api, fx.file2); out.rewinds != 0 {
					t.Fatalf("output rewound %d times, want none", out.rewinds)
				}

				gets := filter(fx.fp.settle(t), big.within(tc.prefix))
				if len(gets) != 2 || gets[0].Fault != tc.kind || gets[0].Range != big.rangeHeader() || gets[0].Status != http.StatusPartialContent || gets[0].Bytes != tc.prefix {
					t.Fatalf("requests = %+v, want the faulted first answer (%d bytes) and one resume", gets, tc.prefix)
				}
				resume := gets[1]
				start, end := parseRange(t, resume.Range)
				if tc.kind == resetBody {
					if start < big.start || start > big.start+tc.prefix {
						t.Fatalf("resume Range %q, want a start within %d bytes after %d", resume.Range, tc.prefix, big.start)
					}
				} else if start != big.start+tc.prefix {
					t.Fatalf("resume Range %q, want bytes=%d-%d", resume.Range, big.start+tc.prefix, big.end)
				}
				if end != big.end || resume.Fault != passThrough || resume.Status != http.StatusPartialContent || resume.Bytes != end-start+1 {
					t.Fatalf("resume = %+v, want the remaining %d bytes served with 206", resume, big.end-start+1)
				}
				r.cached(t, fx)
			})
		}
	})

	t.Run("TrickleNoRetry", func(t *testing.T) {
		const idle = 100 * time.Millisecond
		slow := dlTrickle(t, fx, idle)
		r := dlStart(t, fx, fx.small, client.WithIdleTimeout(idle))
		fx.fp.arm(dlWhen(record.xorbGet, slow))
		began := time.Now()
		fx.mustDownload(t, r.ctx, r.c, apiV2, fx.file2)
		if elapsed := time.Since(began); elapsed < idle {
			t.Fatalf("download took %v, want the trickle to outlive the idle timeout %v", elapsed, idle)
		}
		for _, g := range dlGets(t, fx, 2) {
			start, end := parseRange(t, g.Range)
			if g.Fault != trickle || g.Status != http.StatusPartialContent || g.Bytes != end-start+1 {
				t.Fatalf("request %+v was retried or cut short", g)
			}
		}
		r.cached(t, fx)
	})
}

// TestDLStatus pins status handling: 5xx answers to the query are retried, and a permanent
// status costs a bounded number of requests before the cache heals.
func TestDLStatus(t *testing.T) {
	fx := newFixture(t)
	big := fx.big

	t.Run("TransientRecon", func(t *testing.T) {
		for _, tc := range []struct {
			status int
			api    apiVersion
		}{
			{http.StatusInternalServerError, apiAuto},
			{http.StatusBadGateway, apiAuto},
			{http.StatusServiceUnavailable, apiAuto},
			{http.StatusGatewayTimeout, apiAuto},
			{http.StatusServiceUnavailable, apiBatch},
		} {
			t.Run(fmt.Sprintf("%d/%s", tc.status, tc.api), func(t *testing.T) {
				r := dlStart(t, fx, fx.small)
				fx.fp.arm(dlWhen(dlTwice(record.reconstruction), fault{kind: injectStatus, status: tc.status}))
				fx.mustDownload(t, r.ctx, r.c, tc.api, fx.file2)

				recs := fx.fp.settle(t)
				if got, want := dlStatusLog(filter(recs, record.reconstruction)), []int{tc.status, tc.status, http.StatusOK}; !slices.Equal(got, want) {
					t.Fatalf("reconstruction statuses = %v, want %v", got, want)
				}
				if gets := filter(recs, big.match); len(gets) != 1 {
					t.Fatalf("requests for %s = %+v, want exactly one", big.rangeHeader(), gets)
				}
				r.cached(t, fx)
			})
		}
	})

	t.Run("Budgets", func(t *testing.T) {
		for _, tc := range []struct {
			name                  string
			match                 func(record) bool
			status, want, queries int
			gap                   string
		}{
			{"xorb401", big.match, http.StatusUnauthorized, 1, 1, ""},
			{"xorb404", big.match, http.StatusNotFound, 1, 1, ""},
			{"recon503", record.reconstruction, http.StatusServiceUnavailable, dlAttempts, dlAttempts, ""},
			{"recon401", record.reconstruction, http.StatusUnauthorized, 1, 1, ""},
			{"recon404", record.reconstruction, http.StatusNotFound, 1, 1, ""},
			{"xorb503", big.match, http.StatusServiceUnavailable, dlAttempts, 1, "xorb 5xx single attempt: client/xorb.go:133 returns httpseek's status response as is, client/client.go:205 isServerError only guards doWithNetworkRetry"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				r := dlStart(t, fx, fx.small, client.WithRetries(defaultRetries))
				fx.fp.arm(dlWhen(tc.match, fault{kind: injectStatus, status: tc.status}))
				if tc.gap != "" {
					knownGap(t, tc.gap)
				}
				err := fx.download(r.ctx, r.c, apiV1, fx.file2, newOutput(t, nil))
				if err == nil || !strings.Contains(err.Error(), strconv.Itoa(tc.status)) {
					t.Fatalf("download error = %v, want status %d reported", err, tc.status)
				}

				recs := fx.fp.settle(t)
				got := dlStatusLog(filter(recs, tc.match))
				if len(got) != tc.want || slices.Contains(got, http.StatusOK) || slices.Contains(got, http.StatusPartialContent) {
					t.Fatalf("%s answered %v, want %d attempts", tc.name, got, tc.want)
				}
				if n := len(filter(recs, record.reconstruction)); n != tc.queries {
					t.Fatalf("%s led to %d reconstruction queries, want %d", tc.name, n, tc.queries)
				}
				if healed := filter(r.healed(t, fx), big.match); len(healed) != 1 {
					t.Fatalf("healing fetched %+v, want the multi-chunk range once", healed)
				}
			})
		}
	})
}

// TestDLRangeAnswers pins range metadata on V2: a 206 without Content-Range still completes through
// the one-shot fallback, a resume answered with the wrong range fails within the open budget, a
// missing V2 falls back to V1 mid-fault, and a seeded output resumes without refetching its prefix.
func TestDLRangeAnswers(t *testing.T) {
	fx := newFixture(t)
	big := fx.big
	p := fx.midChunkPrefix(t, big)
	resumeRange := fmt.Sprintf("bytes=%d-%d", big.start+p, big.end)

	t.Run("NoContentRangeOriginal", func(t *testing.T) {
		r := dlStart(t, fx, fx.small)
		fx.fp.arm(dlWhen(big.match, fault{kind: noContentRange}))
		fx.mustDownload(t, r.ctx, r.c, apiV2, fx.file2)
		// httpseek rejects every open, then xorb.go:139 falls back to a one-shot GET.
		gets := filter(fx.fp.settle(t), big.match)
		if len(gets) < 2 || len(gets) > dlOpenAttempts+1 {
			t.Fatalf("requests for %s = %+v, want between 2 and %d", big.rangeHeader(), gets, dlOpenAttempts+1)
		}
		for _, g := range gets {
			if g.Status != http.StatusPartialContent {
				t.Fatalf("request %+v, want 206", g)
			}
		}
		r.cached(t, fx)
	})

	t.Run("ResumeFaults", func(t *testing.T) {
		for _, kind := range []faultKind{ignoreRange, shiftRange, noContentRange} {
			t.Run(kind.String(), func(t *testing.T) {
				r := dlStart(t, fx, fx.small)
				fx.fp.arm(dlResumeFault(fx, p, kind, nil))
				// The cause reaches the caller only when the reader hits it before the worker; TestDLKnownGaps/ResumeFaultCause pins it.
				if err := fx.download(r.ctx, r.c, apiV2, fx.file2, newOutput(t, nil)); err == nil {
					t.Fatal("download succeeded, want the misanswered resume to fail it")
				}
				gets := filter(fx.fp.settle(t), big.within(p))
				if len(gets) < 2 || len(gets) > 1+dlOpenAttempts || gets[0].Fault != abortBody || gets[0].Status != http.StatusPartialContent || gets[0].Bytes != p {
					t.Fatalf("requests = %+v, want the aborted answer (%d bytes) and 1 to %d resumes", gets, p, dlOpenAttempts)
				}
				for _, g := range gets[1:] {
					if g.Fault != kind || g.Range != resumeRange {
						t.Fatalf("resume %+v, want %s answered with %s", g, resumeRange, kind)
					}
				}
				if healed := filter(r.healed(t, fx), big.within(p)); len(healed) != 1 {
					t.Fatalf("healing fetched %+v, want the multi-chunk range once", healed)
				}
			})
		}
	})

	t.Run("V2Fallback", func(t *testing.T) {
		r := dlStart(t, fx, fx.small)
		fx.fp.arm(func(rec record) fault {
			switch {
			case dlV2Recon(rec):
				return fault{kind: injectStatus, status: http.StatusNotFound}
			case big.match(rec) && rec.Seq == 0:
				return fault{kind: abortBody, prefix: p}
			}
			return fault{}
		})
		fx.mustDownload(t, r.ctx, r.c, apiAuto, fx.file2)

		recs := fx.fp.settle(t)
		if got, want := dlReconLog(recs), []string{"/v2 404 ", "/v1 200 "}; !slices.Equal(got, want) {
			t.Fatalf("reconstruction requests = %q, want %q", got, want)
		}
		gets := filter(recs, big.within(p))
		if len(gets) != 2 || gets[0].Fault != abortBody || gets[0].Bytes != p || gets[1].Range != resumeRange {
			t.Fatalf("requests = %+v, want an aborted first answer and a resume from %d", gets, big.start+p)
		}
		r.cached(t, fx)
	})

	t.Run("Seeded", func(t *testing.T) {
		seeded := dlSeeded(fx)
		for _, tc := range []struct {
			name      string
			kind      faultKind
			v2        bool
			wantRecon []string
		}{
			{"v2", stallBody, true, []string{fmt.Sprintf("/v2 200 bytes=%d-", seeded)}},
			{"v1fallback", abortBody, false, []string{fmt.Sprintf("/v2 404 bytes=%d-", seeded), fmt.Sprintf("/v1 200 bytes=%d-", seeded)}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var opts []client.Options
				if tc.kind == stallBody {
					opts = append(opts, shortIdle)
				}
				r := dlStart(t, fx, fx.small, opts...)
				fx.fp.arm(func(rec record) fault {
					switch {
					case !tc.v2 && dlV2Recon(rec):
						return fault{kind: injectStatus, status: http.StatusNotFound}
					case big.match(rec) && rec.Seq == 0:
						return fault{kind: tc.kind, prefix: p}
					}
					return fault{}
				})
				out := newOutput(t, fx.file2.data[:seeded])
				if err := fx.download(r.ctx, r.c, apiAuto, fx.file2, out); err != nil {
					t.Fatalf("seeded download: %v", err)
				}
				requireBytes(t, out, fx.file2.data)
				if want := int64(len(fx.file2.data)) - seeded; out.rewinds != 0 || out.written != want {
					t.Fatalf("wrote %d bytes with %d rewinds, want %d after the seeded prefix and none", out.written, out.rewinds, want)
				}

				recs := fx.fp.settle(t)
				if got := dlReconLog(recs); !slices.Equal(got, tc.wantRecon) {
					t.Fatalf("reconstruction requests = %q, want %q", got, tc.wantRecon)
				}
				if gets := filter(recs, xorbOf(fx.first.hash)); len(gets) != 0 {
					t.Fatalf("fetched %+v, which only backs the seeded prefix", gets)
				}
				gets := filter(recs, big.within(p))
				if len(gets) != 2 || gets[0].Fault != tc.kind || gets[0].Bytes != p || gets[1].Range != resumeRange {
					t.Fatalf("requests = %+v, want a %s first answer and a resume from %d", gets, tc.kind, big.start+p)
				}
				// Completing the file later reuses the resumed entry and fetches only the seeded prefix's xorb.
				if healed := r.healed(t, fx); len(healed) != 1 || !xorbOf(fx.first.hash)(healed[0]) {
					t.Fatalf("completing the cache fetched %+v, want only %s", healed, fx.first.hash)
				}
			})
		}
	})
}

// TestDLAbandon pins what an abandoned download leaves behind: a parent cancel or a reader Close
// returns promptly with no worker left and a cache that heals, and a second client on the same
// cache waits for the range instead of fetching it again. file1 warms the shared term, so the
// reader waits in prefetcher.Get on the stalled range when the abandonment lands.
func TestDLAbandon(t *testing.T) {
	fx := newFixture(t)
	big := fx.big

	t.Run("ParentCancel", func(t *testing.T) {
		// A body stall inside chunk 0 keeps the range unpublished, dodging the loadTo/Done race.
		for _, tc := range dlStalls(dlFirstChunkPrefix(t, fx, big)) {
			t.Run(tc.name, func(t *testing.T) {
				r := dlStart(t, fx, fx.file1, client.WithIdleTimeout(testTimeout))
				fx.fp.arm(dlWhen(big.within(tc.f.prefix), tc.f))
				if tc.gap != "" {
					knownGap(t, tc.gap)
				}
				dlCtx, cancelDownload := context.WithCancel(r.ctx)
				defer cancelDownload()
				out, done := newOutput(t, nil), make(chan error, 1)
				go func() { done <- fx.download(dlCtx, r.c, apiV2, fx.file2, out) }()

				fx.fp.wait(t, dlStalled(fx, tc.f))
				cancelDownload()
				canceledAt := time.Now()
				var err error
				select {
				case err = <-done:
				case <-time.After(waitTimeout):
					t.Fatal("download did not return after its context was canceled")
				}
				if took := time.Since(canceledAt); took > dlCancelBound {
					t.Fatalf("download returned %v after cancel, want within %v", took, dlCancelBound)
				}
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("download error = %v, want context.Canceled", err)
				}
				fx.fp.settle(t)
				if healed := filter(r.healed(t, fx), big.match); len(healed) != 1 || healed[0].Fault != passThrough {
					t.Fatalf("healing fetched %+v, want the abandoned range once without faults", healed)
				}
			})
		}
	})

	t.Run("BatchReaderClose", func(t *testing.T) {
		// A body stall past the first chunk leaves the range published and half loaded.
		for _, tc := range dlStalls(fx.midChunkPrefix(t, big)) {
			t.Run(tc.name, func(t *testing.T) {
				r := dlStart(t, fx, fx.file1, client.WithIdleTimeout(testTimeout))
				fx.fp.arm(dlWhen(big.within(tc.f.prefix), tc.f))
				if tc.gap != "" {
					knownGap(t, tc.gap)
				}
				readers, _, err := r.c.DownloadFiles(r.ctx, []xet.FileHash{fx.file2.hash, fx.small.hash})
				if err != nil {
					t.Fatal(err)
				}
				// Prefetching starts before the first Read, so the stall is reached without reading.
				fx.fp.wait(t, dlStalled(fx, tc.f))
				closedAt := time.Now()
				if err := readers[0].Close(); err != nil {
					t.Fatalf("close stalled reader: %v", err)
				}
				if took := time.Since(closedAt); took > dlCancelBound {
					t.Fatalf("Close returned %v after being called, want within %v", took, dlCancelBound)
				}
				small, err := io.ReadAll(readers[1])
				if err != nil {
					t.Fatalf("sibling reader: %v", err)
				}
				if err := diffBytes(small, fx.small.data); err != nil {
					t.Fatalf("sibling reader: %v", err)
				}
				readers[1].Close()
				fx.fp.settle(t)
				if healed := filter(r.healed(t, fx), big.match); len(healed) != 1 || healed[0].Fault != passThrough {
					t.Fatalf("healing fetched %+v, want the abandoned range once without faults", healed)
				}
			})
		}
	})

	t.Run("SharedCache", func(t *testing.T) {
		r := dlStart(t, fx, fx.small)
		follower := fx.newClient(t, r.cacheDir)
		fx.fp.arm(dlWhen(record.xorbGet, fault{kind: trickle, piece: 64 << 10, gap: 20 * time.Millisecond}))
		outLeader, outFollower := newOutput(t, nil), newOutput(t, nil)
		leaderDone, followerDone := make(chan error, 1), make(chan error, 1)
		go func() { leaderDone <- fx.download(r.ctx, r.c, apiV2, fx.file2, outLeader) }()
		fx.fp.wait(t, func(recs []record, _ int) bool {
			gets := filter(recs, big.match)
			return len(gets) > 0 && gets[0].Bytes > 0
		})
		go func() { followerDone <- fx.download(r.ctx, follower, apiV2, fx.file2, outFollower) }()
		if err := <-followerDone; err != nil {
			t.Fatalf("follower client: %v", err)
		}
		if err := <-leaderDone; err != nil {
			t.Fatalf("leader client: %v", err)
		}
		requireBytes(t, outLeader, fx.file2.data)
		requireBytes(t, outFollower, fx.file2.data)
		dlGets(t, fx, 2)
		r.cached(t, fx)
	})
}

// TestDLTCP pins the client under netem-like impairments: latency and a rate above the idle guard
// cost only time, a rate below it resumes in a chain, a reset or a blackholed first connection costs
// one retry, refused connections are retried up to the budget, and one connection slot still
// completes a concurrent download.
func TestDLTCP(t *testing.T) {
	fx := newFixture(t)
	big := fx.big
	const piece = 32 << 10

	t.Run("Latency", func(t *testing.T) {
		const d, j = 20 * time.Millisecond, 10 * time.Millisecond
		r, counts := dlTCPStart(t, fx, func() {
			fx.tcp.delay(toServer, d, j)
			fx.tcp.delay(toClient, d, j)
		})
		began := time.Now()
		fx.mustDownload(t, r.ctx, r.c, apiAuto, fx.file2)
		// The query and then the xorb GETs each cross the proxy both ways, at least d-j per way.
		if elapsed := time.Since(began); elapsed < 4*(d-j) || elapsed > dlGenerous {
			t.Fatalf("download took %v, want between %v and %v", elapsed, 4*(d-j), dlGenerous)
		}
		dlGets(t, fx, 2)
		if n := len(filter(fx.fp.records(), record.reconstruction)); n != 1 {
			t.Fatalf("%d reconstruction queries, want 1", n)
		}
		if got := counts.since(fx); got.cuts != 0 || got.refused != 0 {
			t.Fatalf("tcp counters %+v, want no cuts or refusals", got)
		}
		dlTCPFinish(t, fx, r)
	})

	t.Run("RateAboveIdle", func(t *testing.T) {
		r, _ := dlTCPStart(t, fx, func() { fx.tcp.rate(toClient, 1600<<10, piece) }, shortIdle)
		began := time.Now()
		fx.mustDownload(t, r.ctx, r.c, apiAuto, fx.file2)
		if elapsed := time.Since(began); elapsed < 250*time.Millisecond {
			t.Fatalf("download took %v, want at least 250ms at 1600 KiB/s per connection", elapsed)
		}
		dlGets(t, fx, 2)
		dlTCPFinish(t, fx, r)
	})

	t.Run("RateBelowIdle", func(t *testing.T) {
		r, _ := dlTCPStart(t, fx, func() { fx.tcp.rate(toClient, 320<<10, piece) }, client.WithIdleTimeout(50*time.Millisecond))
		if out := fx.mustDownload(t, r.ctx, r.c, apiAuto, fx.file2); out.rewinds != 0 {
			t.Fatalf("output rewound %d times, want none", out.rewinds)
		}
		// Every piece after the first waits 100ms, so each GET delivers one piece before the idle guard resumes.
		gets := filter(fx.fp.settle(t), record.xorbGet)
		for _, tk := range []task{fx.first, big} {
			chain := filter(gets, xorbOf(tk.hash))
			if len(chain) < 4 {
				t.Fatalf("requests for xorb %s = %+v, want at least 4 in a resume chain", tk.hash, chain)
			}
			dlResumeChain(t, chain)
		}
		dlTCPFinish(t, fx, r)
	})

	t.Run("RstMidBody", func(t *testing.T) {
		r, counts := dlTCPStart(t, fx, func() { fx.tcp.cutOnce(toClient, 256<<10, cutRst) }, shortIdle)
		fx.mustDownload(t, r.ctx, r.c, apiAuto, fx.file2)
		if got := counts.since(fx); got.cuts != 1 {
			t.Fatalf("tcp counters %+v, want exactly one cut", got)
		}
		gets := dlGets(t, fx, 3)
		pairs := 0
		for _, tk := range []task{fx.first, big} {
			if chain := filter(gets, xorbOf(tk.hash)); len(chain) == 2 {
				dlResumeChain(t, chain)
				pairs++
			}
		}
		if pairs != 1 {
			t.Fatalf("xorb GETs = %+v, want exactly one xorb resumed", gets)
		}
		dlTCPFinish(t, fx, r)
	})

	t.Run("BlackholeHeadersOnce", func(t *testing.T) {
		r, counts := dlTCPStart(t, fx, func() { fx.tcp.cutOnce(toClient, 0, cutBlackhole) }, shortIdle)
		began := time.Now()
		fx.mustDownload(t, r.ctx, r.c, apiV1, fx.file2)
		if elapsed := time.Since(began); elapsed < idleTimeout {
			t.Fatalf("download took %v, want at least the idle timeout %v", elapsed, idleTimeout)
		}
		if got := counts.since(fx); got.cuts != 1 || got.conns < 2 {
			t.Fatalf("tcp counters %+v, want one cut and a second connection", got)
		}
		dlGets(t, fx, 2)
		// The request head may slip through before the hole opens, so the proxy may log the swallowed answer too.
		recon := filter(fx.fp.records(), record.reconstruction)
		if len(recon) < 1 || len(recon) > 2 || slices.ContainsFunc(recon, func(rec record) bool { return rec.Status != http.StatusOK }) {
			t.Fatalf("reconstruction requests = %+v, want 1 or 2 answered 200", recon)
		}
		dlTCPFinish(t, fx, r)
	})

	t.Run("Refused", func(t *testing.T) {
		for _, k := range []int{defaultRetries, dlAttempts} {
			t.Run(fmt.Sprintf("k=%d", k), func(t *testing.T) {
				r, counts := dlTCPStart(t, fx, func() { fx.tcp.refuse(k) })
				if k < dlAttempts {
					fx.mustDownload(t, r.ctx, r.c, apiV1, fx.file2)
					if got := counts.since(fx); got.refused != k {
						t.Fatalf("tcp counters %+v, want %d refusals", got, k)
					}
					dlGets(t, fx, 2)
					if n := len(filter(fx.fp.records(), record.reconstruction)); n != 1 {
						t.Fatalf("%d reconstruction queries reached the proxy, want 1", n)
					}
					dlTCPFinish(t, fx, r)
					return
				}
				err := fx.download(r.ctx, r.c, apiV1, fx.file2, newOutput(t, nil))
				if want := fmt.Sprintf("network error after %d attempts", dlAttempts); err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("download error = %v, want %q", err, want)
				}
				if got := counts.since(fx); got.refused != k {
					t.Fatalf("tcp counters %+v, want %d refusals", got, k)
				}
				if recs := fx.fp.settle(t); len(recs) != 0 {
					t.Fatalf("requests reached the proxy: %+v, want none", recs)
				}
				r.healed(t, fx)
			})
		}
	})

	t.Run("MaxConnsOne", func(t *testing.T) {
		r, counts := dlTCPStart(t, fx, func() { fx.tcp.maxConns(1) }, shortIdle, client.WithConcurrency(4))
		began := time.Now()
		fx.mustDownload(t, r.ctx, r.c, apiAuto, fx.file2)
		if elapsed := time.Since(began); elapsed > dlGenerous {
			t.Fatalf("download took %v, want within %v", elapsed, dlGenerous)
		}
		// The second xorb GET waits in the backlog until the idle guard retries it over the one live connection.
		if got := counts.since(fx); got.conns != 1 || got.refused != 0 {
			t.Fatalf("tcp counters %+v, want one proxied connection and no refusals", got)
		}
		dlGets(t, fx, 2)
		dlDrainBacklog(t, fx)
		dlTCPFinish(t, fx, r)
	})
}

// TestDLKnownGaps asserts the desired behavior on paths master gets wrong; each subtest arms,
// names its gap and skips unless XET_CHAOS_KNOWN_GAPS=1.
func TestDLKnownGaps(t *testing.T) {
	fx := newFixture(t)
	big := fx.big
	p := fx.midChunkPrefix(t, big)
	resumeRange := fmt.Sprintf("bytes=%d-%d", big.start+p, big.end)

	t.Run("StatusRetry", func(t *testing.T) {
		const xorbGap = "a xorb GET status is terminal after one attempt: client/xorb.go:133 returns httpseek's status response as is"
		const reconGap = "408 and 429 are terminal on the query: client/client.go:205 isServerError retries only 500, 502, 503 and 504"
		for _, tc := range []struct {
			name         string
			match        func(record) bool
			status, want int
			gap          string
		}{
			{"xorb408", big.match, http.StatusRequestTimeout, http.StatusPartialContent, xorbGap},
			{"xorb429", big.match, http.StatusTooManyRequests, http.StatusPartialContent, xorbGap},
			{"xorb500", big.match, http.StatusInternalServerError, http.StatusPartialContent, xorbGap},
			{"xorb503", big.match, http.StatusServiceUnavailable, http.StatusPartialContent, xorbGap},
			{"recon408", record.reconstruction, http.StatusRequestTimeout, http.StatusOK, reconGap},
			{"recon429", record.reconstruction, http.StatusTooManyRequests, http.StatusOK, reconGap},
		} {
			t.Run(tc.name, func(t *testing.T) {
				r := dlStart(t, fx, nil)
				fx.fp.arm(dlWhen(dlTwice(tc.match), fault{kind: injectStatus, status: tc.status}))
				knownGap(t, tc.gap)
				fx.mustDownload(t, r.ctx, r.c, apiV1, fx.file2)
				if got, want := dlStatusLog(filter(fx.fp.settle(t), tc.match)), []int{tc.status, tc.status, tc.want}; !slices.Equal(got, want) {
					t.Fatalf("%s statuses = %v, want %v", tc.name, got, want)
				}
				fx.assertCached(t, r.ctx, fx.file2, r.cacheDir)
			})
		}
	})

	t.Run("StatusOnResume", func(t *testing.T) {
		r := dlStart(t, fx, nil)
		fx.fp.arm(func(rec record) fault {
			switch {
			case big.match(rec):
				return fault{kind: abortBody, prefix: p}
			case big.within(p)(rec) && rec.Seq < 2:
				return fault{kind: injectStatus, status: http.StatusServiceUnavailable}
			}
			return fault{}
		})
		knownGap(t, "a non-2xx answer to a resume is sticky: httpseek openseek.go open() keeps ErrUnsupported and Seek to the same offset preserves it, so the retries under client.go:125 never reach the wire")
		fx.mustDownload(t, r.ctx, r.c, apiV2, fx.file2)
		gets := filter(fx.fp.settle(t), big.within(p))
		want := []int{http.StatusPartialContent, http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusPartialContent}
		if got := dlStatusLog(gets); !slices.Equal(got, want) {
			t.Fatalf("statuses = %v, want %v: %+v", got, want, gets)
		}
		if gets[0].Bytes != p || gets[3].Range != resumeRange || gets[3].Bytes != big.end-big.start-p+1 {
			t.Fatalf("requests = %+v, want %d bytes aborted and the rest served by the third resume", gets, p)
		}
		fx.assertCached(t, r.ctx, fx.file2, r.cacheDir)
	})

	t.Run("ResumeFaultCause", func(t *testing.T) {
		for _, tc := range []struct {
			kind    faultKind
			wantErr string
		}{
			{ignoreRange, "206"},
			{shiftRange, "Content-Range"},
		} {
			t.Run(tc.kind.String(), func(t *testing.T) {
				r := dlStart(t, fx, nil)
				// A trickled shared term keeps the reader on term 0 while the worker exhausts big's resumes.
				fx.fp.arm(dlResumeFault(fx, p, tc.kind, &fault{kind: trickle, piece: 64 << 10, gap: 20 * time.Millisecond}))
				knownGap(t, "a published range loses its failure: download/prefetcher.go:280 Done()s the cache when LoadAll fails, so the reader reports chunk_cache.go:573 'file closed' or :561 'out of range' instead of the cause")
				err := fx.download(r.ctx, r.c, apiV2, fx.file2, newOutput(t, nil))
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("download error = %v, want %q reported", err, tc.wantErr)
				}
				fx.heal(t, r.ctx, fx.file2, r.cacheDir)
			})
		}
	})

	t.Run("ReconBody", func(t *testing.T) {
		const gap = "a reconstruction body fault is terminal: client/reconstruction.go:46 decodes doWithNetworkRetry's answer once, with no retry of the query"
		for _, tc := range []struct {
			kind faultKind
			api  apiVersion
		}{
			{abortBody, apiV1},
			{shortChunked, apiV2},
			{stallBody, apiV2},
			{resetBody, apiV1},
		} {
			t.Run(tc.kind.String()+"/"+tc.api.String(), func(t *testing.T) {
				var opts []client.Options
				if tc.kind == stallBody || tc.kind == resetBody {
					opts = append(opts, shortIdle)
				}
				r := dlStart(t, fx, nil, opts...)
				fx.fp.arm(dlWhen(dlFirst(record.reconstruction), fault{kind: tc.kind, prefix: dlReconPrefix}))
				knownGap(t, gap)
				fx.mustDownload(t, r.ctx, r.c, tc.api, fx.file2)
				recs := filter(fx.fp.settle(t), record.reconstruction)
				if len(recs) != 2 || recs[0].Fault != tc.kind || recs[0].Status != http.StatusOK || recs[0].Bytes != dlReconPrefix {
					t.Fatalf("reconstruction requests = %+v, want the faulted answer (%d bytes) and one retry", recs, dlReconPrefix)
				}
				if retry := recs[1]; retry.Fault != passThrough || retry.Status != http.StatusOK || retry.Query != recs[0].Query || retry.Range != recs[0].Range {
					t.Fatalf("retry = %+v, want the same query served whole", retry)
				}
				fx.assertCached(t, r.ctx, fx.file2, r.cacheDir)
			})
		}
		t.Run("permanentCut", func(t *testing.T) {
			r := dlStart(t, fx, nil)
			fx.fp.arm(dlWhen(record.reconstruction, fault{kind: abortBody, prefix: dlReconPrefix}))
			knownGap(t, gap)
			err := fx.download(r.ctx, r.c, apiV1, fx.file2, newOutput(t, nil))
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("download error = %v, want the cut body reported", err)
			}
			if recs := filter(fx.fp.settle(t), record.reconstruction); len(recs) != dlAttempts {
				t.Fatalf("reconstruction requests = %+v, want %d attempts", recs, dlAttempts)
			}
			fx.heal(t, r.ctx, fx.file2, r.cacheDir)
		})
	})

	t.Run("ForbiddenRefreshed", func(t *testing.T) {
		r := dlStart(t, fx, nil)
		fx.fp.arm(dlExpireGen(1))
		knownGap(t, "a 403 on a xorb GET is terminal: client/xorb.go:150 reports it and download/prefetcher.go:244 fetches one URL per task, so no query refreshes the URLs")
		fx.mustDownload(t, r.ctx, r.c, apiV1, fx.file2)

		recs := fx.fp.settle(t)
		recon := filter(recs, record.reconstruction)
		if len(recon) != 2 || recon[1].Path != recon[0].Path || recon[1].Query != recon[0].Query || recon[1].Range != recon[0].Range {
			t.Fatalf("reconstruction requests = %+v, want the same query repeated once", recon)
		}
		gets := filter(recs, record.xorbGet)
		refused, served := filter(gets, dlURLGen(0)), filter(gets, dlURLGen(1))
		if len(refused)+len(served) != len(gets) || len(refused) == 0 {
			t.Fatalf("xorb GETs = %+v, want the first generation refused and the second served", gets)
		}
		for _, g := range gets {
			if (g.Fault == injectStatus && g.Status != http.StatusForbidden) || (g.Fault == passThrough && g.Status != http.StatusPartialContent) {
				t.Fatalf("xorb GET %+v, want generation 0 refused and generation 1 served", g)
			}
		}
		for _, key := range dlRangeKeys(refused) {
			if !slices.Contains(dlRangeKeys(served), key) {
				t.Fatalf("refused range %q was never fetched through the new URL: %+v", key, gets)
			}
		}
		fx.requireAccounting(t, served, fx.file2, 0)
		fx.assertCached(t, r.ctx, fx.file2, r.cacheDir)
	})

	t.Run("ResumedHashVerified", func(t *testing.T) {
		r := dlStart(t, fx, nil)
		corrupt := slices.Clone(fx.file2.data[:dlSeeded(fx)])
		corrupt[100] ^= 0xff
		out := &dlReadableOutput{output: newOutput(t, corrupt)}
		knownGap(t, "a resumed download never verifies the file hash: client/download.go:147 verifies only when resumeOffset == 0 (design TBD: the client would have to read the seeded prefix back)")
		if err := r.c.DownloadFile(r.ctx, fx.file2.hash, out); err == nil {
			t.Fatal("resuming onto a corrupt prefix succeeded, want a hash mismatch")
		}
	})
}

// dlRun is one subtest's client on a fresh cache, with the goroutines its leak check compares against.
type dlRun struct {
	ctx      context.Context
	cacheDir string
	c        *client.Client
	before   workerSet
}

// dlStart quiets the fixture and builds the subtest's client. A download of warm first starts the
// client's cache merge worker (download/cache_manager.go noteMergeCandidate), which lives 2s past
// the last seal and would otherwise count as leaked; nil skips the warm-up.
func dlStart(t *testing.T, fx *fixture, warm *blob, opts ...client.Options) dlRun {
	t.Helper()
	ctx, _ := testCtx(t, testTimeout)
	fx.fp.arm(nil)
	fx.tcp.clear()
	fx.tcp.closeExisting()
	r := dlRun{ctx: ctx, cacheDir: t.TempDir()}
	r.c = fx.newClient(t, r.cacheDir, opts...)
	if warm != nil {
		fx.mustDownload(t, ctx, r.c, apiV1, warm)
		fx.fp.arm(nil)
	}
	r.before = snapshotWorkers()
	return r
}

// cached proves the workers gone and the cache whole.
func (r dlRun) cached(t *testing.T, fx *fixture) {
	t.Helper()
	fx.noLeakedWorkers(t, r.before)
	fx.assertCached(t, r.ctx, fx.file2, r.cacheDir)
}

// healed proves the workers gone, then completes file2 on the cache and returns the xorb GETs that took.
func (r dlRun) healed(t *testing.T, fx *fixture) []record {
	t.Helper()
	fx.noLeakedWorkers(t, r.before)
	return fx.heal(t, r.ctx, fx.file2, r.cacheDir)
}

// dlGets settles the proxy and returns its xorb GETs, requiring n of them that account for file2.
func dlGets(t *testing.T, fx *fixture, n int) []record {
	t.Helper()
	gets := filter(fx.fp.settle(t), record.xorbGet)
	if len(gets) != n {
		t.Fatalf("xorb GETs = %+v, want %d", gets, n)
	}
	fx.requireAccounting(t, gets, fx.file2, 0)
	return gets
}

// dlWhen applies f to the requests match picks.
func dlWhen(match func(record) bool, f fault) rule {
	return func(r record) fault {
		if match(r) {
			return f
		}
		return fault{}
	}
}

// dlFirst narrows match to the first request of each method, path and Range.
func dlFirst(match func(record) bool) func(record) bool {
	return func(r record) bool { return match(r) && r.Seq == 0 }
}

// dlTwice narrows match to the first two requests of each method, path and Range.
func dlTwice(match func(record) bool) func(record) bool {
	return func(r record) bool { return match(r) && r.Seq < 2 }
}

// dlResumeFault aborts the big range after prefix bytes and answers every resume with kind; shared, when set, faults the shared term.
func dlResumeFault(fx *fixture, prefix int64, kind faultKind, shared *fault) rule {
	return func(r record) fault {
		switch {
		case shared != nil && xorbOf(fx.first.hash)(r):
			return *shared
		case fx.big.match(r):
			return fault{kind: abortBody, prefix: prefix}
		case fx.big.within(prefix)(r):
			return fault{kind: kind}
		}
		return fault{}
	}
}

// dlSeeded is the resume offset halfway into file2's second term.
func dlSeeded(fx *fixture) int64 {
	terms := fx.file2.layout.Terms
	return int64(terms[0].UnpackedLength + terms[1].UnpackedLength/2)
}

// dlStatusLog lists the status of each record in order.
func dlStatusLog(recs []record) []int {
	out := make([]int, 0, len(recs))
	for _, r := range recs {
		out = append(out, r.Status)
	}
	return out
}

// dlV2Recon matches the V2 reconstruction query.
func dlV2Recon(r record) bool {
	return r.Method == http.MethodGet && strings.HasPrefix(r.Path, "/v2/reconstructions/")
}

// dlReconLog renders the reconstruction requests as "<version> <status> <Range>".
func dlReconLog(recs []record) []string {
	var out []string
	for _, r := range filter(recs, record.reconstruction) {
		out = append(out, fmt.Sprintf("%s %d %s", r.Path[:3], r.Status, r.Range))
	}
	return out
}

// dlStall is one way to leave a download hanging on the big range.
type dlStall struct {
	name string
	f    fault
	gap  string
}

// dlStalls lists a header stall and a body stall after prefix bytes; the body one is a known gap under -race.
func dlStalls(prefix int64) []dlStall {
	return []dlStall{
		{"headers", fault{kind: stallHeaders}, ""},
		{"body", fault{kind: stallBody, prefix: prefix}, dlBodyRace},
	}
}

// dlStalled reports whether a handler is holding the big range open under stall f, its prefix relayed.
func dlStalled(fx *fixture, f fault) func([]record, int) bool {
	return func(recs []record, _ int) bool {
		for _, r := range filter(recs, fx.big.within(f.prefix)) {
			if r.Fault == f.kind && r.Bytes == f.prefix && r.End.IsZero() {
				return true
			}
		}
		return false
	}
}

// dlTrickle paces xorb bodies so the largest packed chunk of big takes longer than idle to arrive
// while every gap stays far below it.
func dlTrickle(t *testing.T, fx *fixture, idle time.Duration) fault {
	t.Helper()
	const gap = 40 * time.Millisecond
	largest := fx.maxChunk(t, fx.big)
	piece := largest / 4
	if piece <= 0 {
		t.Fatalf("fixture: a largest chunk of %d bytes cannot be trickled", largest)
	}
	if pieces := (largest + piece - 1) / piece; time.Duration(pieces-1)*gap <= idle || 2*gap >= idle {
		t.Fatalf("fixture: %d pieces of %d bytes every %v cannot span the %v idle timeout", pieces, piece, gap, idle)
	}
	return fault{kind: trickle, piece: piece, gap: gap}
}

// dlChunkBoundaryPrefix returns the offset from the task's start at which its chunk idx ends.
func dlChunkBoundaryPrefix(t *testing.T, fx *fixture, tk task, idx int) int64 {
	t.Helper()
	bounds := fx.chunkBounds(t, tk)
	if idx < 0 || idx >= len(bounds)-1 { // the last bound is the task's own end
		t.Fatalf("fixture: chunk %d is not an inner boundary of %v", idx, bounds)
	}
	return bounds[idx] - tk.start
}

// dlFirstChunkPrefix returns an offset strictly inside the task's first chunk, so a fault there keeps the range unpublished.
func dlFirstChunkPrefix(t *testing.T, fx *fixture, tk task) int64 {
	t.Helper()
	size := fx.chunkBounds(t, tk)[0] - tk.start
	if size < 16 {
		t.Fatalf("fixture: first chunk of %+v spans %d bytes, too few to cut inside", tk, size)
	}
	return size / 2
}

// dlResumeChain checks that gets continue one range: the same end, starts that never go back and
// move forward overall, each at most where the previous answer stopped. A start repeats only when
// an attempt timed out before its first byte.
func dlResumeChain(t *testing.T, gets []record) {
	t.Helper()
	if len(gets) < 2 {
		t.Fatalf("resume chain %+v, want at least two requests", gets)
	}
	firstStart, prevEnd := parseRange(t, gets[0].Range)
	prevStart, prevBytes := firstStart, gets[0].Bytes
	for _, g := range gets[1:] {
		start, end := parseRange(t, g.Range)
		if end != prevEnd || start < prevStart || start > prevStart+prevBytes {
			t.Fatalf("resume %+v after a start at %d with %d bytes served: want the same end %d and a start in [%d, %d]", g, prevStart, prevBytes, prevEnd, prevStart, prevStart+prevBytes)
		}
		prevStart, prevBytes = start, g.Bytes
	}
	if prevStart <= firstStart {
		t.Fatalf("resume chain %+v never moved past its first start %d", gets, firstStart)
	}
}

// dlURLGen matches xorb GETs through URLs tagged with generation n.
func dlURLGen(n int) func(record) bool {
	return func(r record) bool { return r.xorbGet() && r.Query == fmt.Sprintf("gen=%d", n) }
}

// dlRangeKeys lists "<path> <Range>" of recs, sorted, so two phases compare range by range.
func dlRangeKeys(recs []record) []string {
	out := make([]string, 0, len(recs))
	for _, r := range recs {
		out = append(out, r.Path+" "+r.Range)
	}
	slices.Sort(out)
	return out
}

// dlExpireGen tags the xorb URLs of every reconstruction answer with its sequence number and
// refuses the generations below n, as an expired signed URL would; the server ignores the query.
func dlExpireGen(n int) rule {
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

// dlReadableOutput is an output a client could read back to verify a seeded prefix.
type dlReadableOutput struct {
	*output
	off int64
}

func (o *dlReadableOutput) Read(p []byte) (int, error) {
	n, err := o.f.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}

// dlTCPCounts is a snapshot of the TCP proxy's cumulative counters.
type dlTCPCounts struct {
	conns, refused, cuts int
}

func dlTCPSnapshot(fx *fixture) dlTCPCounts {
	return dlTCPCounts{conns: fx.tcp.conns(), refused: fx.tcp.refused(), cuts: fx.tcp.cuts()}
}

// since returns the counts added after the snapshot.
func (c dlTCPCounts) since(fx *fixture) dlTCPCounts {
	now := dlTCPSnapshot(fx)
	return dlTCPCounts{conns: now.conns - c.conns, refused: now.refused - c.refused, cuts: now.cuts - c.cuts}
}

// dlTCPStart builds a subtest's client on a quiet proxy, arms the impairment, drops the live
// connections so only the download's carry it, and snapshots the counters it is measured by.
func dlTCPStart(t *testing.T, fx *fixture, arm func(), opts ...client.Options) (dlRun, dlTCPCounts) {
	t.Helper()
	r := dlStart(t, fx, fx.small, opts...)
	arm()
	fx.tcp.closeExisting()
	return r, dlTCPSnapshot(fx)
}

// dlTCPFinish lifts the impairment, then proves the workers gone and the cache whole.
func dlTCPFinish(t *testing.T, fx *fixture, r dlRun) {
	t.Helper()
	fx.tcp.clear()
	r.cached(t, fx)
}

// dlDrainBacklog frees the proxy's slots while refusing every connection, so the ones the client
// gave up on in the listen backlog are reset instead of replaying their buffered GETs, and returns
// once the refusals stop.
func dlDrainBacklog(t *testing.T, fx *fixture) {
	t.Helper()
	fx.tcp.refuse(-1)
	fx.tcp.closeExisting()
	deadline := time.Now().Add(waitTimeout)
	for last := -1; ; {
		time.Sleep(20 * time.Millisecond)
		n := fx.tcp.refused()
		if n == last {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("backlog still draining after %v: %d connections refused", waitTimeout, n)
		}
		last = n
	}
}
