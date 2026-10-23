package chaos_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet/client/hf"
	"github.com/wzshiming/xet/client/hf/hftest"
)

const (
	hubReadToken  = "/xet-read-token/"
	hubWriteToken = "/xet-write-token/"
	hubStall      = 300 * time.Millisecond // the ctx of a call the hub stalls
)

// TestHubToken pins the hub client's CAS read token: fetched once, cached, renewed inside its margin, and failing bounded under faults on its endpoint.
func TestHubToken(t *testing.T) {
	fx := newFixture(t, withHub())
	ctx, _ := testCtx(t, testTimeout)
	readToken := func(seq int, f fault) rule { return hubRule(http.MethodGet, hubReadToken, seq, f) }
	expiringIn := func(d time.Duration) fault { return hubJSON(hubTokenJSON(fx.casURL, time.Now().Add(d))) }
	fetches := func(t *testing.T) int {
		t.Helper()
		return len(hubRecs(t, fx.hubProxy, http.MethodGet, hubReadToken))
	}

	t.Run("down", func(t *testing.T) {
		hubArm(t, fx, readToken(-1, hubStatus(http.StatusServiceUnavailable)), nil)
		hc := fx.newHFClient(t, t.TempDir())
		hubWantErr(t, hubDL(ctx, fx, hc, newOutput(t, nil)), "503", "xet-read-token")
		if n := fetches(t); n != 1 {
			t.Errorf("%d read-token fetches; want 1", n)
		}
		hubNoCAS(t, fx)
	})
	t.Run("transient5xx", func(t *testing.T) {
		hubArm(t, fx, readToken(0, hubStatus(http.StatusServiceUnavailable)), nil)
		knownGap(t, "token fetch not retried: client/hf/token.go:75-83")
		hc := fx.newHFClient(t, t.TempDir())
		if err := hubDL(ctx, fx, hc, newOutput(t, nil)); err != nil {
			t.Fatalf("download after one 503 read-token answer: %v", err)
		}
		if n := fetches(t); n != 2 {
			t.Errorf("%d read-token fetches; want 2, the 503 and its retry", n)
		}
	})
	t.Run("stallCtx", func(t *testing.T) {
		hubArm(t, fx, readToken(-1, fault{kind: stallHeaders}), nil)
		hc := fx.newHFClient(t, t.TempDir())
		stall, _ := testCtx(t, hubStall)
		hubStalled(t, stall, func() error { return hubDL(stall, fx, hc, newOutput(t, nil)) })
		hubNoCAS(t, fx)
		if recs := hubRecs(t, fx.hubProxy, http.MethodGet, hubReadToken); len(recs) != 1 || recs[0].Status != 0 || recs[0].End.IsZero() {
			t.Errorf("read-token records %+v; want one never answered and finished once the client left", recs)
		}
	})
	t.Run("malformed", func(t *testing.T) {
		hubArm(t, fx, readToken(-1, hubJSON([]byte(`{"accessToken":`))), nil)
		hc := fx.newHFClient(t, t.TempDir())
		hubWantErr(t, hubDL(ctx, fx, hc, newOutput(t, nil)), "decode auth response")
		hubNoCAS(t, fx)
	})
	t.Run("offOrigin", func(t *testing.T) {
		hubArm(t, fx, readToken(-1, hubJSON(hubTokenJSON("cas.invalid", time.Now().Add(time.Hour)))), nil)
		hc := fx.newHFClient(t, t.TempDir())
		hubWantErr(t, hubDL(ctx, fx, hc, newOutput(t, nil)), "invalid base URL")
		hubNoCAS(t, fx)
	})
	t.Run("renewalWindow", func(t *testing.T) {
		hubArm(t, fx, readToken(-1, expiringIn(30*time.Second)), nil)
		hc := fx.newHFClient(t, t.TempDir())
		for range 2 {
			if err := hubDL(ctx, fx, hc, newOutput(t, nil)); err != nil {
				t.Fatalf("download with a token 30 s from expiry: %v", err)
			}
		}
		if n := fetches(t); n != 2 {
			t.Errorf("%d read-token fetches for 2 downloads with a token 30 s from expiry; want 2, one each inside the 1 min renewal margin", n)
		}
		fx.hubProxy.arm(readToken(-1, expiringIn(time.Hour)))
		for range 2 {
			if err := hubDL(ctx, fx, hc, newOutput(t, nil)); err != nil {
				t.Fatalf("download renewing to a 1 h token: %v", err)
			}
		}
		if n := fetches(t); n != 1 {
			t.Errorf("%d read-token fetches for 2 downloads renewing to a 1 h token; want 1", n)
		}
	})
	t.Run("singleFlight", func(t *testing.T) {
		hubArm(t, fx, readToken(-1, fault{kind: trickle, piece: 16, gap: 20 * time.Millisecond}), nil)
		hc := fx.newHFClient(t, t.TempDir())
		outs := []*output{newOutput(t, nil), newOutput(t, nil), newOutput(t, nil), newOutput(t, nil)}
		errs := make(chan error, len(outs))
		for _, out := range outs {
			go func() { errs <- hubDL(ctx, fx, hc, out) }()
		}
		for range outs {
			if err := <-errs; err != nil {
				t.Errorf("concurrent download: %v", err)
			}
		}
		if n := fetches(t); n != 1 {
			t.Errorf("%d read-token fetches for %d concurrent downloads; want 1 they all share", n, len(outs))
		}
		if recon := filter(fx.fp.settle(t), record.reconstruction); len(recon) != len(outs) {
			t.Errorf("%d reconstruction requests; want %d, one per download", len(recon), len(outs))
		}
	})
	t.Run("refreshFailRecovers", func(t *testing.T) {
		hubArm(t, fx, readToken(-1, expiringIn(30*time.Second)), nil)
		hc := fx.newHFClient(t, t.TempDir())
		if err := hubDL(ctx, fx, hc, newOutput(t, nil)); err != nil {
			t.Fatalf("download with a token 30 s from expiry: %v", err)
		}
		fx.hubProxy.arm(readToken(-1, hubStatus(http.StatusServiceUnavailable)))
		fx.fp.arm(nil)
		hubWantErr(t, hubDL(ctx, fx, hc, newOutput(t, nil)), "503")
		hubNoCAS(t, fx)
		fx.hubProxy.arm(nil)
		for i := range 2 {
			if err := hubDL(ctx, fx, hc, newOutput(t, nil)); err != nil {
				t.Fatalf("download %d once the token endpoint is back: %v", i+1, err)
			}
			if n := fetches(t); n != 1 {
				t.Errorf("%d read-token fetches by download %d once the endpoint is back; want 1, its 1 h token then cached", n, i+1)
			}
		}
	})
}

// TestHubTokenExpiredAtCAS pins a token the CAS rejects as expired: one terminal 401, no refetch while its exp looks valid, a fresh client recovers.
func TestHubTokenExpiredAtCAS(t *testing.T) {
	fx := newFixture(t, withHub(), withAuth(time.Hour))
	ctx, _ := testCtx(t, testTimeout)
	hubArm(t, fx, nil, nil)
	hc := fx.newHFClient(t, t.TempDir())
	if err := hubDL(ctx, fx, hc, newOutput(t, nil)); err != nil {
		t.Fatalf("download with a fresh issuer token: %v", err)
	}

	fx.clock.advance(2 * time.Hour)
	fx.hubProxy.arm(nil)
	fx.fp.arm(nil)
	hubWantErr(t, fx.download(ctx, hc.Client, apiAuto, fx.small, newOutput(t, nil)), "401")
	recs := fx.fp.settle(t)
	if recon := filter(recs, record.reconstruction); len(recon) != 1 || recon[0].Status != http.StatusUnauthorized {
		t.Errorf("reconstruction records %+v; want one 401, retried neither as is nor over V1", recon)
	}
	if gets := filter(recs, record.xorbGet); len(gets) != 0 {
		t.Errorf("xorb GETs %+v after the 401; want none", gets)
	}
	if n := len(hubRecs(t, fx.hubProxy, http.MethodGet, hubReadToken)); n != 0 {
		t.Errorf("%d read-token fetches; want 0, the provider trusts the token's exp", n)
	}

	fx.hubProxy.arm(nil)
	if err := hubDL(ctx, fx, fx.newHFClient(t, t.TempDir()), newOutput(t, nil)); err != nil {
		t.Fatalf("download through a fresh hub client: %v", err)
	}
	if n := len(hubRecs(t, fx.hubProxy, http.MethodGet, hubReadToken)); n != 1 {
		t.Errorf("%d read-token fetches for a fresh hub client; want 1", n)
	}
}

// TestHubResolve pins Resolve's single HEAD under hub faults and a hub-resolved download healing a CAS body fault.
func TestHubResolve(t *testing.T) {
	fx := newFixture(t, withHub())
	ctx, _ := testCtx(t, testTimeout)
	resolveHead := func(seq int, f fault) rule { return hubRule(http.MethodHead, "/resolve/", seq, f) }
	heads := func(t *testing.T) []record {
		t.Helper()
		return hubRecs(t, fx.hubProxy, http.MethodHead, "/resolve/")
	}

	t.Run("stallCtx", func(t *testing.T) {
		hubArm(t, fx, resolveHead(-1, fault{kind: stallHeaders}), nil)
		hc := fx.newHFClient(t, t.TempDir())
		stall, _ := testCtx(t, hubStall)
		hubStalled(t, stall, func() error {
			_, err := hc.Resolve(stall, "file2")
			return err
		})
		if recs := heads(t); len(recs) != 1 || recs[0].Status != 0 {
			t.Errorf("resolve records %+v; want one never answered", recs)
		}
	})
	t.Run("stallIdle", func(t *testing.T) {
		hubArm(t, fx, resolveHead(-1, fault{kind: stallHeaders}), nil)
		knownGap(t, "hub idle guard fixed at client.DefaultIdleTimeout (60 s), WithIdleTimeout never reaches it: client/hf/client.go:76")
		hc := fx.newHFClient(t, t.TempDir(), hf.WithClientOptions(shortIdle))
		guarded, _ := testCtx(t, 3*time.Second)
		hubIdleGuarded(t, guarded, func() error {
			_, err := hc.Resolve(guarded, "file2")
			return err
		})
	})
	t.Run("down", func(t *testing.T) {
		hubArm(t, fx, resolveHead(-1, hubStatus(http.StatusInternalServerError)), nil)
		hc := fx.newHFClient(t, t.TempDir())
		_, err := hc.Resolve(ctx, "file2")
		hubWantErr(t, err, "500")
		if recs := heads(t); len(recs) != 1 {
			t.Errorf("%d resolve requests; want 1", len(recs))
		}
	})
	t.Run("transient5xx", func(t *testing.T) {
		hubArm(t, fx, resolveHead(0, hubStatus(http.StatusServiceUnavailable)), nil)
		knownGap(t, "Resolve not retried: client/hf/resolve.go:25")
		hc := fx.newHFClient(t, t.TempDir())
		if got, err := hc.Resolve(ctx, "file2"); err != nil || got.Hash != fx.file2.hash {
			t.Fatalf("Resolve(file2) after one 503 = %+v, %v; want hash %s", got, err, fx.file2.hash)
		}
		if recs := heads(t); len(recs) != 2 {
			t.Errorf("%d resolve requests; want 2, the 503 and its retry", len(recs))
		}
	})
	t.Run("notFound", func(t *testing.T) {
		hubArm(t, fx, nil, nil)
		hc := fx.newHFClient(t, t.TempDir())
		_, err := hc.Resolve(ctx, "missing.bin")
		hubWantErr(t, err, "404", "Entry not found")
	})
	t.Run("missingLink", func(t *testing.T) {
		hubArm(t, fx, resolveHead(-1, fault{kind: injectBody, status: http.StatusFound, contentType: "text/plain", body: []byte("Found")}), nil)
		hc := fx.newHFClient(t, t.TempDir())
		_, err := hc.Resolve(ctx, "file2")
		hubWantErr(t, err, "missing xet-reconstruction-info link (status 302)")
	})
	t.Run("redirectNotFollowed", func(t *testing.T) {
		hubArm(t, fx, nil, nil)
		hc := fx.newHFClient(t, t.TempDir())
		seen := len(fx.hub.Requests())
		if got, err := hc.Resolve(ctx, "file2"); err != nil || got.Hash != fx.file2.hash {
			t.Fatalf("Resolve(file2) = %+v, %v; want hash %s", got, err, fx.file2.hash)
		}
		if recs := fx.hubProxy.settle(t); len(recs) != 1 || recs[0].Status != http.StatusFound {
			t.Errorf("hub proxy records %+v; want the one 302", recs)
		}
		if slices.ContainsFunc(fx.hub.Requests()[seen:], func(r hftest.Request) bool { return strings.Contains(r.Path, "/cdn/") }) {
			t.Error("the client followed the 302 to the hub's CDN")
		}
	})
	t.Run("casRecovery", func(t *testing.T) {
		hubArm(t, fx, nil, nil)
		cacheDir := t.TempDir()
		hc := fx.newHFClient(t, cacheDir)
		// The warm-up starts hc's cache merge worker, which idles 2 s, before the snapshot.
		if err := hubDL(ctx, fx, hc, newOutput(t, nil)); err != nil {
			t.Fatalf("warm-up download: %v", err)
		}
		before := snapshotWorkers()
		resolved, err := hc.Resolve(ctx, "file2")
		if err != nil || resolved.Hash != fx.file2.hash {
			t.Fatalf("Resolve(file2) = %+v, %v; want hash %s", resolved, err, fx.file2.hash)
		}
		prefix := fx.midChunkPrefix(t, fx.big)
		fx.fp.arm(func(r record) fault {
			if fx.big.match(r) && r.Seq == 0 {
				return fault{kind: abortBody, prefix: prefix}
			}
			return fault{}
		})
		out := newOutput(t, nil)
		if err := hc.DownloadFile(ctx, resolved.Hash, out); err != nil {
			t.Fatalf("file2 download with its big term aborted mid-chunk: %v", err)
		}
		requireBytes(t, out, fx.file2.data)
		recs := fx.fp.settle(t)
		big := filter(recs, fx.big.within(prefix))
		if gets := filter(recs, record.xorbGet); len(gets) < 2 || len(big) < 2 || big[0].Fault != abortBody {
			t.Errorf("xorb GETs %+v; want the big term aborted after %d bytes, then resumed", gets, prefix)
		}
		fx.noLeakedWorkers(t, before)
		fx.assertCached(t, ctx, fx.file2, cacheDir)
	})
}

// TestHubCommit pins Commit under faults on the hub's preupload, write token and commit endpoints and on the CAS's xorb POST.
func TestHubCommit(t *testing.T) {
	fx := newFixture(t, withHub())
	ctx, _ := testCtx(t, testTimeout)
	preupload := func(seq int, f fault) rule { return hubRule(http.MethodPost, "/preupload/", seq, f) }
	count := func(t *testing.T, p *faultProxy, method, part string) int {
		t.Helper()
		return len(hubRecs(t, p, method, part))
	}

	t.Run("preuploadDown", func(t *testing.T) {
		path, data := hubData(0)
		hubArm(t, fx, preupload(-1, hubStatus(http.StatusServiceUnavailable)), nil)
		hc := fx.newHFClient(t, t.TempDir())
		base := len(fx.hub.Commits())
		hubWantErr(t, hubCommit(ctx, hc, path, data), "503", "preupload")
		if n := count(t, fx.hubProxy, http.MethodPost, "/preupload/"); n != 1 {
			t.Errorf("%d preupload requests; want 1", n)
		}
		if n := count(t, fx.hubProxy, http.MethodGet, hubWriteToken); n != 0 {
			t.Errorf("%d write-token fetches; want 0", n)
		}
		hubNoCAS(t, fx)
		hubNewCommits(t, fx, base, 0)
	})
	t.Run("preuploadTransient5xx", func(t *testing.T) {
		path, data := hubData(1)
		hubArm(t, fx, preupload(0, hubStatus(http.StatusServiceUnavailable)), nil)
		knownGap(t, "hub POST not retried: client/hf/commit.go:192-200")
		hc := fx.newHFClient(t, t.TempDir())
		base := len(fx.hub.Commits())
		if err := hubCommit(ctx, hc, path, data); err != nil {
			t.Fatalf("Commit after one 503 preupload answer: %v", err)
		}
		if n := count(t, fx.hubProxy, http.MethodPost, "/preupload/"); n != 2 {
			t.Errorf("%d preupload requests; want 2, the 503 and its retry", n)
		}
		hubNewCommits(t, fx, base, 1)
	})
	t.Run("writeTokenDown", func(t *testing.T) {
		path, data := hubData(2)
		hubArm(t, fx, hubRule(http.MethodGet, hubWriteToken, -1, hubStatus(http.StatusServiceUnavailable)), nil)
		cacheDir := t.TempDir()
		hc := fx.newHFClient(t, cacheDir)
		base := len(fx.hub.Commits())
		hubWantErr(t, hubCommit(ctx, hc, path, data), "xet-write-token", "503")
		hubNoCAS(t, fx)
		hubNewCommits(t, fx, base, 0)
		if files := stagingFiles(t, cacheDir); len(files) != 0 {
			t.Errorf("staging files %q left behind; want none", files)
		}
	})
	t.Run("xorbResetThenHeal", func(t *testing.T) {
		path, data := hubData(3)
		hubArm(t, fx, nil, hubRule(http.MethodPost, "/v1/xorbs/", 0, fault{kind: cutRequestBody, prefix: 4096}))
		hc := fx.newHFClient(t, t.TempDir())
		base := len(fx.hub.Commits())
		before := snapshotWorkers()
		if err := hubCommit(ctx, hc, path, data); err != nil {
			t.Fatalf("Commit with its first xorb POST reset after 4096 bytes: %v", err)
		}
		fx.noLeakedWorkers(t, before)
		if posts := hubRecs(t, fx.fp, http.MethodPost, "/v1/xorbs/"); len(posts) != 2 || posts[0].Fault != cutRequestBody || posts[1].Status != http.StatusOK {
			t.Errorf("xorb POSTs %+v; want the cut one, then a 200 retry", posts)
		}
		hubNewCommits(t, fx, base, 1)
		sum := sha256.Sum256(data)
		if oid, size := hubCommitLFS(t, fx, path); oid != hex.EncodeToString(sum[:]) || size != int64(len(data)) {
			t.Errorf("commit line for %s: oid %s, size %d; want %x and %d", path, oid, size, sum, len(data))
		}
		fx.requireStored(t, data)
		resolved, err := hc.Resolve(ctx, path)
		if err != nil {
			t.Fatalf("Resolve(%s): %v", path, err)
		}
		out := newOutput(t, nil)
		if err := hc.DownloadFile(ctx, resolved.Hash, out); err != nil {
			t.Fatalf("download of the committed %s: %v", path, err)
		}
		requireBytes(t, out, data)
	})
	t.Run("commitPostDownThenRetry", func(t *testing.T) {
		path, data := hubData(4)
		hubArm(t, fx, hubRule(http.MethodPost, "/commit/", 0, hubStatus(http.StatusServiceUnavailable)), nil)
		hc := fx.newHFClient(t, t.TempDir())
		base := len(fx.hub.Commits())
		hubWantErr(t, hubCommit(ctx, hc, path, data), "503", "commit")
		hubNewCommits(t, fx, base, 0)
		if n := count(t, fx.fp, http.MethodPost, "/v1/xorbs/"); n < 1 {
			t.Errorf("%d xorb POSTs before the failed commit; want the upload done", n)
		}
		// Only the CAS phase restarts: the hub's keeps counting, so the next commit POST passes.
		fx.fp.arm(nil)
		if err := hubCommit(ctx, hc, path, data); err != nil {
			t.Fatalf("second Commit of the same file: %v", err)
		}
		hubNewCommits(t, fx, base, 1)
		if n := count(t, fx.fp, http.MethodPost, "/v1/xorbs/"); n != 0 {
			t.Errorf("%d xorb POSTs re-sent bytes the CAS holds; want 0", n)
		}
		fx.fp.arm(nil)
		if err := hubCommit(ctx, hc, path, data); !errors.Is(err, hf.ErrNoChanges) {
			t.Fatalf("third Commit of the same bytes: %v; want hf.ErrNoChanges", err)
		}
		hubNewCommits(t, fx, base, 1)
		hubNoCAS(t, fx)
	})
	t.Run("preuploadStallCtx", func(t *testing.T) {
		path, data := hubData(5)
		hubArm(t, fx, preupload(-1, fault{kind: stallHeaders}), nil)
		hc := fx.newHFClient(t, t.TempDir())
		base := len(fx.hub.Commits())
		stall, _ := testCtx(t, hubStall)
		hubStalled(t, stall, func() error { return hubCommit(stall, hc, path, data) })
		hubNoCAS(t, fx)
		hubNewCommits(t, fx, base, 0)
	})
	t.Run("postStallIdle", func(t *testing.T) {
		path, data := hubData(6)
		hubArm(t, fx, preupload(-1, fault{kind: stallHeaders}), nil)
		knownGap(t, "hub POST never idle-guarded: client/idle.go:31-32, and the hub guard is fixed at 60 s: client/hf/client.go:76")
		hc := fx.newHFClient(t, t.TempDir(), hf.WithClientOptions(shortIdle))
		guarded, _ := testCtx(t, 3*time.Second)
		hubIdleGuarded(t, guarded, func() error { return hubCommit(guarded, hc, path, data) })
	})
}

// hubArm starts a phase with h on the hub proxy and f on the CAS proxy; both settle when t ends.
func hubArm(t *testing.T, fx *fixture, h, f rule) {
	t.Helper()
	fx.hubProxy.arm(h)
	fx.fp.arm(f)
	t.Cleanup(func() {
		fx.hubProxy.settle(t)
		fx.fp.settle(t)
	})
}

// hubRule applies f to method requests whose path contains part: the phase's first such request for seq 0, all of them for seq < 0.
func hubRule(method, part string, seq int, f fault) rule {
	return func(r record) fault {
		if r.Method == method && strings.Contains(r.Path, part) && (seq < 0 || r.Seq == seq) {
			return f
		}
		return fault{}
	}
}

// hubRecs returns p's records of method requests whose path contains part, once p settles.
func hubRecs(t *testing.T, p *faultProxy, method, part string) []record {
	t.Helper()
	return filter(p.settle(t), func(r record) bool { return r.Method == method && strings.Contains(r.Path, part) })
}

func hubStatus(code int) fault {
	return fault{kind: injectStatus, status: code}
}

func hubJSON(body []byte) fault {
	return fault{kind: injectBody, status: http.StatusOK, contentType: "application/json", body: body}
}

// hubTokenJSON is a token endpoint answer naming casURL and expiring at exp.
func hubTokenJSON(casURL string, exp time.Time) []byte {
	body, _ := json.Marshal(map[string]any{"accessToken": "chaos-tok", "casUrl": casURL, "exp": exp.Unix()})
	return body
}

// hubDL downloads small through hc over the V2 API into out and checks its bytes.
func hubDL(ctx context.Context, fx *fixture, hc *hf.Client, out *output) error {
	if err := fx.download(ctx, hc.Client, apiV2, fx.small, out); err != nil {
		return err
	}
	got, err := out.readAll()
	if err != nil {
		return err
	}
	return diffBytes(got, fx.small.data)
}

// hubData is commit subtest i's file: 256 KiB from seed 48+i at m5-i.bin.
func hubData(i int) (string, []byte) {
	return fmt.Sprintf("m5-%d.bin", i), pseudoRandom(256<<10, uint32(48+i))
}

func hubCommit(ctx context.Context, hc *hf.Client, path string, data []byte) error {
	_, err := hc.Commit(ctx, "chaos "+path, hf.CommitFile{Path: path, Content: bytes.NewReader(data)})
	return err
}

// hubCommitLFS returns the oid and size of path's lfsFile line in the last commit the hub received.
func hubCommitLFS(t *testing.T, fx *fixture, path string) (string, int64) {
	t.Helper()
	for _, req := range slices.Backward(fx.hub.Requests()) {
		if req.Method != http.MethodPost || !strings.Contains(req.Path, "/commit/") {
			continue
		}
		for line := range strings.SplitSeq(strings.TrimSpace(string(req.Body)), "\n") {
			var l struct {
				Key   string `json:"key"`
				Value struct {
					OID  string `json:"oid"`
					Path string `json:"path"`
					Size int64  `json:"size"`
				} `json:"value"`
			}
			if err := json.Unmarshal([]byte(line), &l); err != nil {
				t.Fatalf("commit line %q: %v", line, err)
			}
			if l.Key == "lfsFile" && l.Value.Path == path {
				return l.Value.OID, l.Value.Size
			}
		}
		t.Fatalf("the last commit has no lfsFile line for %s:\n%s", path, req.Body)
	}
	t.Fatal("the hub received no commit")
	return "", 0
}

// hubNewCommits requires the hub to have created want commits since it held base.
func hubNewCommits(t *testing.T, fx *fixture, base, want int) {
	t.Helper()
	if got := len(fx.hub.Commits()) - base; got != want {
		t.Errorf("the hub created %d commits; want %d", got, want)
	}
}

// hubNoCAS requires the phase to have sent nothing to the CAS.
func hubNoCAS(t *testing.T, fx *fixture) {
	t.Helper()
	if recs := fx.fp.settle(t); len(recs) != 0 {
		t.Errorf("the CAS saw %d requests %+v; want none", len(recs), recs)
	}
}

// hubWantErr requires err to mention every part.
func hubWantErr(t *testing.T, err error, parts ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("err = nil; want one mentioning %q", parts)
	}
	for _, part := range parts {
		if !strings.Contains(err.Error(), part) {
			t.Fatalf("err = %v; want it to mention %q", err, part)
		}
	}
}

// hubStalled requires call, stalled at the hub, to end on ctx's deadline well within 2 s.
func hubStalled(t *testing.T, ctx context.Context, call func() error) {
	t.Helper()
	start := time.Now()
	err := call()
	if elapsed := time.Since(start); !errors.Is(err, context.DeadlineExceeded) || ctx.Err() == nil || elapsed >= 2*time.Second {
		t.Fatalf("stalled call returned %v after %v, ctx err %v; want ctx's %v deadline to end it within 2 s", err, elapsed, ctx.Err(), hubStall)
	}
}

// hubIdleGuarded requires call, stalled at the hub, to fail on the idle timeout within 1 s while ctx still runs.
func hubIdleGuarded(t *testing.T, ctx context.Context, call func() error) {
	t.Helper()
	start := time.Now()
	err := call()
	if elapsed := time.Since(start); err == nil || ctx.Err() != nil || elapsed >= time.Second {
		t.Fatalf("stalled call returned %v after %v, ctx err %v; want the %v idle timeout to fail it within 1 s", err, elapsed, ctx.Err(), idleTimeout)
	}
}
