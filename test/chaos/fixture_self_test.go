package chaos_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/download"
)

// TestFixtureSeeded pins the default fixture's files, downloads, layout geometry and oracles on one shared fixture.
func TestFixtureSeeded(t *testing.T) {
	fx := newFixture(t)
	ctx, _ := testCtx(t, testTimeout)
	t.Run("downloads", func(t *testing.T) { selfDownloads(t, ctx, fx) })
	t.Run("file2Shape", func(t *testing.T) { selfFile2Shape(t, fx) })
	t.Run("cacheOracle", func(t *testing.T) { selfCacheOracle(t, ctx, fx) })
	t.Run("accounting", func(t *testing.T) { selfAccounting(t, ctx, fx) })
	t.Run("uploadOracles", func(t *testing.T) { selfUploadOracles(t, ctx, fx) })
	t.Run("workerLeaks", func(t *testing.T) { selfWorkerLeaks(t, ctx, fx) })
	t.Run("heal", func(t *testing.T) { selfHeal(t, ctx, fx) })
}

// TestFixtureKnownGap pins knownGap skipping unless XET_CHAOS_KNOWN_GAPS=1.
func TestFixtureKnownGap(t *testing.T) {
	if knownGapsEnv != "XET_CHAOS_KNOWN_GAPS" {
		t.Fatalf("knownGapsEnv = %q; want XET_CHAOS_KNOWN_GAPS", knownGapsEnv)
	}
	t.Setenv(knownGapsEnv, "")
	if err := os.Unsetenv(knownGapsEnv); err != nil {
		t.Fatal(err)
	}
	if gapsEnabled() {
		t.Error("gapsEnabled() = true with the env unset")
	}
	if sub, reached := selfGapRun(t, "unset"); !sub.Skipped() || reached {
		t.Errorf("env unset: skipped %t, ran past knownGap %t; want a skip", sub.Skipped(), reached)
	}

	t.Setenv(knownGapsEnv, "1")
	if !gapsEnabled() {
		t.Error("gapsEnabled() = false with the env at 1")
	}
	if sub, reached := selfGapRun(t, "set"); sub.Skipped() || !reached {
		t.Errorf("env at 1: skipped %t, ran past knownGap %t; want a full run", sub.Skipped(), reached)
	}
}

// TestFixtureMultiXorbSize pins multiXorbSize spilling 256 KiB past one xorb.
func TestFixtureMultiXorbSize(t *testing.T) {
	if multiXorbSize != xet.MaxXorbSize+256<<10 {
		t.Fatalf("multiXorbSize = %d; want xet.MaxXorbSize + 256<<10 = %d", multiXorbSize, xet.MaxXorbSize+256<<10)
	}
}

// TestFixtureOutput pins output keeping its seed, counting written bytes and counting only seeks that land earlier.
func TestFixtureOutput(t *testing.T) {
	seed := []byte("seed-bytes")
	out := newOutput(t, seed)
	if _, ok := any(out).(io.ReaderFrom); ok {
		t.Error("output implements io.ReaderFrom, so io.Copy would bypass Write")
	}
	if got := out.bytes(t); out.written != 0 || !bytes.Equal(got, seed) {
		t.Fatalf("seeded output: written %d, bytes %q; want 0 and %q", out.written, got, seed)
	}
	for _, step := range []struct {
		whence  int
		pos     int64
		data    string
		rewinds int
	}{
		{io.SeekEnd, int64(len(seed)), "tail", 0},
		{io.SeekStart, 0, "HEAD", 1},
	} {
		if pos, err := out.Seek(0, step.whence); pos != step.pos || err != nil {
			t.Fatalf("Seek(0, %d) = %d, %v; want %d", step.whence, pos, err, step.pos)
		}
		if n, err := out.Write([]byte(step.data)); n != len(step.data) || err != nil {
			t.Fatalf("Write(%q) = %d, %v", step.data, n, err)
		}
		if out.rewinds != step.rewinds {
			t.Errorf("rewinds = %d after Seek(0, %d) and Write; want %d", out.rewinds, step.whence, step.rewinds)
		}
	}
	if got, want := out.bytes(t), "HEAD-bytestail"; out.written != 8 || string(got) != want {
		t.Errorf("written %d, bytes %q; want 8 and %q", out.written, got, want)
	}
}

// TestFixtureHub pins withHub resolving and downloading file2 through the hub proxy and a CAS behind the fault proxy.
func TestFixtureHub(t *testing.T) {
	fx := newFixture(t, withHub())
	ctx, _ := testCtx(t, testTimeout)
	hc := fx.newHFClient(t, t.TempDir())
	fx.hubProxy.arm(nil)
	fx.fp.arm(nil)
	seen := len(fx.hub.Requests())

	resolved, err := hc.Resolve(ctx, "file2")
	if err != nil {
		t.Fatalf("Resolve(file2): %v", err)
	}
	if resolved.Hash != fx.file2.hash {
		t.Fatalf("Resolve(file2) = %s; want file2.hash %s", resolved.Hash, fx.file2.hash)
	}
	out := newOutput(t, nil)
	if err := hc.DownloadFile(ctx, resolved.Hash, out); err != nil {
		t.Fatalf("hf DownloadFile(file2): %v", err)
	}
	if err := selfDiff(out.bytes(t), fx.file2.data); err != nil {
		t.Fatalf("hf DownloadFile(file2) output: %v", err)
	}

	var routes []string
	for _, r := range fx.hub.Requests()[seen:] {
		routes = append(routes, r.Route())
	}
	for _, want := range []string{"HEAD resolve", "GET xet-read-token"} {
		if !slices.Contains(routes, want) {
			t.Errorf("hub requests %q lack %q", routes, want)
		}
	}
	proxied := fx.hubProxy.settle(t)
	for _, want := range []struct{ method, path string }{{http.MethodHead, "/resolve/main/file2"}, {http.MethodGet, "/xet-read-token/"}} {
		if !slices.ContainsFunc(proxied, func(r record) bool { return r.Method == want.method && strings.Contains(r.Path, want.path) }) {
			t.Errorf("hubProxy records %+v lack %s %s", proxied, want.method, want.path)
		}
	}
	if recon := filter(fx.fp.settle(t), record.reconstruction); len(recon) == 0 {
		t.Error("the hf download's reconstruction request bypassed the fault proxy")
	}
	for _, f := range []struct {
		name string
		f    *blob
	}{{"file1", fx.file1}, {"small", fx.small}} {
		if got, err := hc.Resolve(ctx, f.name); err != nil || got.Hash != f.f.hash {
			t.Errorf("Resolve(%s) = %+v, %v; want hash %s", f.name, got, err, f.f.hash)
		}
	}
}

// TestFixtureAuth pins withAuth gating the CAS on issuer tokens that expire on the fixture's fake clock.
func TestFixtureAuth(t *testing.T) {
	fx := newFixture(t, withAuth(0), withHub())
	ctx, _ := testCtx(t, testTimeout)
	selfFetch(t, ctx, fx, fx.newClient(t, t.TempDir()), apiAuto, fx.file1)

	garbage := fx.newClient(t, t.TempDir(), client.WithUpstreamProvider(client.StaticUpstreamProvider(fx.casURL, "garbage")))
	selfRequire401(t, "a garbage token", fx.download(ctx, garbage, apiV1, fx.small, newOutput(t, nil)))

	minted := fx.newClient(t, t.TempDir(), client.WithUpstreamProvider(client.StaticUpstreamProvider(fx.casURL, fx.token(t, auth.Read))))
	selfFetch(t, ctx, fx, minted, apiV1, fx.small)
	fx.clock.advance(2 * time.Hour)
	selfRequire401(t, "a token minted 2h before", fx.download(ctx, minted, apiV1, fx.small, newOutput(t, nil)))

	selfFetch(t, ctx, fx, fx.newHFClient(t, t.TempDir()).Client, apiV1, fx.small)
}

// selfDownloads pins the seeded contents and each file's byte-exact download through every API over both proxies.
func selfDownloads(t *testing.T, ctx context.Context, fx *fixture) {
	files := []struct {
		name string
		f    *blob
	}{{"file1", fx.file1}, {"file2", fx.file2}, {"small", fx.small}, {"empty", fx.empty}}

	t.Run("contents", func(t *testing.T) {
		file1 := selfLCG(1<<20, 1)
		want := map[string][]byte{
			"file1": file1,
			"file2": append(file1[:512<<10:512<<10], selfLCG(512<<10, 2)...),
			"small": bytes.Repeat([]byte("chaos"), 4096),
			"empty": {},
		}
		for _, file := range files {
			data := want[file.name]
			if err := selfDiff(file.f.data, data); err != nil {
				t.Errorf("%s.data: %v", file.name, err)
			}
			if len(data) == 0 {
				if file.f.hash != (xet.FileHash{}) || file.f.layout != nil {
					t.Errorf("%s: hash %s, layout %+v; want the zero file hash and a nil layout", file.name, file.f.hash, file.f.layout)
				}
				continue
			}
			if file.f.sha256 != sha256.Sum256(data) || file.f.layout == nil {
				t.Errorf("%s: sha256 %x, layout %v; want the SHA-256 of its data and a V1 layout", file.name, file.f.sha256, file.f.layout)
			}
		}
	})

	c := fx.newClient(t, t.TempDir())
	apis := []struct {
		name, path string // path prefixes the first reconstruction request
		api        apiVersion
	}{
		{"v1", "/v1/reconstructions/", apiV1},
		{"auto", "/v2/reconstructions/", apiAuto},
		{"v2", "/v2/reconstructions/", apiV2},
		{"batch", "/reconstructions", apiBatch},
	}
	for _, file := range files {
		for _, a := range apis {
			if a.api == apiBatch && file.f == fx.small {
				continue
			}
			t.Run(file.name+"_"+a.name, func(t *testing.T) {
				fx.fp.arm(nil)
				out := newOutput(t, nil)
				if err := fx.download(ctx, c, a.api, file.f, out); err != nil {
					t.Fatalf("download: %v", err)
				}
				if err := selfDiff(out.bytes(t), file.f.data); err != nil {
					t.Fatalf("output: %v", err)
				}
				recs := fx.fp.settle(t)
				recon := filter(recs, record.reconstruction)
				if len(recon) == 0 || !strings.HasPrefix(recon[0].Path, a.path) {
					t.Fatalf("reconstruction records %+v; want the first on %s", recon, a.path)
				}
				// file1 over v1 is the shared client's first download, so its cache is cold.
				if gets := filter(recs, record.xorbGet); file.f == fx.file1 && a.api == apiV1 && (len(recon) != 1 || len(gets) == 0) {
					t.Fatalf("cold file1 phase: %d reconstruction and %d xorb GET records; want 1 and at least 1", len(recon), len(gets))
				}
			})
		}
	}
	if n := fx.tcp.conns(); n < 1 {
		t.Errorf("tcp.conns() = %d after the downloads; want at least 1", n)
	}
}

// selfFile2Shape pins file2's two-term layout, its tasks and the chunk geometry helpers against storage.
func selfFile2Shape(t *testing.T, fx *fixture) {
	layout := selfLayout(t, "file2", fx.file2)
	if len(layout.Terms) != 2 || layout.Terms[0].Hash == layout.Terms[1].Hash {
		t.Fatalf("file2 terms %+v; want 2 from different xorbs", layout.Terms)
	}
	if file1 := selfLayout(t, "file1", fx.file1); len(file1.Terms) == 0 || file1.Terms[0].Hash != layout.Terms[0].Hash {
		t.Errorf("file2 term0 xorb %s; want file1's xorb (file1 terms %+v)", layout.Terms[0].Hash, file1.Terms)
	}
	if r := layout.Terms[1].Range; r.End-r.Start < 2 {
		t.Errorf("file2 term1 chunks %+v; want at least 2", r)
	}
	for i, tk := range []task{fx.first, fx.big} {
		term := layout.Terms[i]
		entries := layout.FetchInfo[term.Hash]
		if len(entries) != 1 {
			t.Errorf("term%d has fetch entries %+v; want exactly 1", i, entries)
			continue
		}
		u := entries[0].URLRange
		if tk.hash != term.Hash || tk.chunks != term.Range || tk.start != u.Start || tk.end != u.End {
			t.Errorf("task of term%d = %+v; want hash %s, chunks %+v, bytes %d-%d", i, tk, term.Hash, term.Range, u.Start, u.End)
		}
	}

	ends := selfChunkEnds(t, fx, fx.big)
	bounds := fx.chunkBounds(t, fx.big)
	if len(bounds) == 0 || bounds[len(bounds)-1] != fx.big.end+1 {
		t.Errorf("chunkBounds(big) = %v; want the last element big.end+1 = %d", bounds, fx.big.end+1)
	}
	// The range start is a chunk boundary too, so a leading big.start is accepted.
	if len(bounds) > 0 && bounds[0] == fx.big.start {
		bounds = bounds[1:]
	}
	if !slices.Equal(bounds, ends) {
		t.Errorf("chunkBounds(big) = %v; want the chunk ends %v from storage", bounds, ends)
	}
	if len(ends) < 3 {
		t.Fatalf("big has %d chunks; chunk index 2 needs 3", len(ends))
	}
	prefix := fx.midChunkPrefix(t, fx.big)
	if at := fx.big.start + prefix; at <= ends[1] || at >= ends[2] {
		t.Errorf("midChunkPrefix(big) = %d cuts at byte %d; want strictly inside chunk index 2, bytes %d-%d", prefix, at, ends[1], ends[2]-1)
	}
	largest := ends[0] - fx.big.start
	for k := 1; k < len(ends); k++ {
		largest = max(largest, ends[k]-ends[k-1])
	}
	if got := fx.maxChunk(t, fx.big); got != largest {
		t.Errorf("maxChunk(big) = %d; want the largest packed chunk %d", got, largest)
	}
}

// selfCacheOracle pins the cache oracle accepting a filled cache, rejecting an empty one and leaving xorbs flowing.
func selfCacheOracle(t *testing.T, ctx context.Context, fx *fixture) {
	fx.fp.arm(nil)
	cacheDir := t.TempDir()
	fx.mustDownload(t, ctx, fx.newClient(t, cacheDir), apiV1, fx.small)

	if err := fx.cachedErr(ctx, fx.small, t.TempDir()); err == nil || (!strings.Contains(err.Error(), "403") && !strings.Contains(err.Error(), "xorb GET")) {
		t.Fatalf("cachedErr for an empty cacheDir: %v; want an error naming the denied xorb GETs or their 403", err)
	}
	fx.assertCached(t, ctx, fx.small, cacheDir)
	// assertCached disarms the proxy, so a cold cache may fetch xorbs again.
	fx.mustDownload(t, ctx, fx.newClient(t, t.TempDir()), apiV1, fx.small)
}

// selfAccounting pins accountingErr accepting a clean file2 download and rejecting duplicate, missing and unexpected GETs.
func selfAccounting(t *testing.T, ctx context.Context, fx *fixture) {
	fx.fp.arm(nil)
	fx.mustDownload(t, ctx, fx.newClient(t, t.TempDir()), apiV1, fx.file2)
	gets := filter(fx.fp.settle(t), record.xorbGet)
	if len(gets) == 0 {
		t.Fatal("a cold file2 download left no xorb GET record")
	}

	if err := accountingErr(gets, fx.file2, 0); err != nil {
		t.Errorf("clean gets %+v with extra 0: %v", gets, err)
	}
	if accountingErr(append(slices.Clone(gets), gets[0]), fx.file2, 0) == nil {
		t.Errorf("accepted %+v fetched twice", gets[0])
	}
	missing := slices.DeleteFunc(slices.Clone(gets), func(r record) bool { return strings.Contains(r.Path, fx.big.hash) })
	if accountingErr(missing, fx.file2, 0) == nil {
		t.Errorf("accepted gets %+v without term1's xorb %s", missing, fx.big.hash)
	}
	if accountingErr(gets, fx.file2, 1) == nil {
		t.Error("accepted extra 1 for gets with no xorb outside file2")
	}
	foreign := record{Method: http.MethodGet, Path: "/v1/xorbs/default/" + strings.Repeat("5e", 32), Range: "bytes=0-99"}
	if err := accountingErr(append(slices.Clone(gets), foreign), fx.file2, 1); err != nil {
		t.Errorf("one GET of a xorb outside file2 with extra 1: %v", err)
	}
}

// selfUploadOracles pins serverHas, storedErr and stagingFiles observing a client upload.
func selfUploadOracles(t *testing.T, ctx context.Context, fx *fixture) {
	fx.fp.arm(nil)
	cacheDir := t.TempDir()
	if got := stagingFiles(t, cacheDir); len(got) != 0 {
		t.Errorf("stagingFiles of a cache without upload/staging = %q; want none", got)
	}
	fresh := bytes.Repeat([]byte("self-upload "), 2048)
	if fx.storedErr(fresh) == nil {
		t.Error("storedErr returned nil for data the server never stored")
	}
	absent := xet.ComputeFileHash([]xet.ChunkHash{xet.ComputeChunkHash([]byte("self"))}, []uint64{4})
	if fx.serverHas(t, absent) {
		t.Errorf("serverHas(%s) = true for a file never stored", absent)
	}

	c := fx.newClient(t, cacheDir)
	hash, err := c.UploadFile(ctx, bytes.NewReader(fx.small.data))
	if err != nil {
		t.Fatalf("UploadFile(small): %v", err)
	}
	if hash != fx.small.hash {
		t.Errorf("UploadFile(small) = %s; want small.hash %s", hash, fx.small.hash)
	}
	if !fx.serverHas(t, hash) {
		t.Errorf("serverHas(%s) = false after uploading small", hash)
	}
	fx.requireStored(t, fx.small.data)

	freshHash, err := c.UploadFile(ctx, bytes.NewReader(fresh))
	if err != nil {
		t.Fatalf("UploadFile(fresh): %v", err)
	}
	if !fx.serverHas(t, freshHash) {
		t.Errorf("serverHas(%s) = false after uploading data new to the server", freshHash)
	}
	fx.requireStored(t, fresh)

	if got := stagingFiles(t, cacheDir); len(got) != 0 {
		t.Errorf("stagingFiles after the uploads = %q; want none", got)
	}
	staging := filepath.Join(cacheDir, "upload", "staging")
	planted := filepath.Join(staging, "xorb-self1")
	if err := os.MkdirAll(staging, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{planted, filepath.Join(staging, "self-other")} {
		if err := os.WriteFile(name, []byte("self"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if got := stagingFiles(t, cacheDir); len(got) != 1 || (got[0] != planted && got[0] != filepath.Base(planted)) {
		t.Errorf("stagingFiles after planting %s = %q; want exactly that file", planted, got)
	}
}

// selfWorkerLeaks pins the worker oracles seeing a stalled download's goroutines and their exit once it is cancelled.
func selfWorkerLeaks(t *testing.T, ctx context.Context, fx *fixture) {
	fx.fp.arm(nil)
	t.Cleanup(func() { fx.fp.arm(nil) })
	c := fx.newClient(t, t.TempDir())
	// Sealing a cache entry starts c's merge worker for its 2 s debounce; starting it before the snapshot keeps the check quick.
	fx.mustDownload(t, ctx, c, apiV1, fx.file1)
	before := snapshotWorkers()
	fx.mustDownload(t, ctx, c, apiV1, fx.small)
	fx.noLeakedWorkers(t, before)

	fx.fp.arm(func(r record) fault {
		if r.xorbGet() {
			return fault{kind: stallHeaders}
		}
		return fault{}
	})
	stalledClient := fx.newClient(t, t.TempDir())
	out := newOutput(t, nil)
	before = snapshotWorkers()
	stalled, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	downloader := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		downloader <- selfGoroutineID()
		done <- fx.download(stalled, stalledClient, apiV1, fx.small, out)
	}()
	selfAwaitXorbGet(t, fx, done)
	leaked := leakedWorkers(before)
	if own := <-downloader; !slices.ContainsFunc(leaked, func(stack string) bool { return goroutineID(stack) != own }) {
		t.Errorf("leakedWorkers found %d stacks while a download waits on a stalled xorb GET, none beyond the downloading goroutine %s:\n%s", len(leaked), own, strings.Join(leaked, "\n\n"))
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Error("a download whose xorb GET stalled succeeded after cancel")
		}
	case <-time.After(waitTimeout):
		t.Fatalf("download still running %v after cancel", waitTimeout)
	}
	fx.noLeakedWorkers(t, before)
}

// selfHeal pins heal clearing the TCP impairments, completing f on cacheDir, returning only that completion's xorb GETs and leaving the cache warm.
func selfHeal(t *testing.T, ctx context.Context, fx *fixture) {
	fx.fp.arm(nil)
	fx.tcp.cutAfter(toClient, 4096, cutRst)
	t.Cleanup(fx.tcp.clear)
	cuts := fx.tcp.cuts()
	cacheDir := t.TempDir()
	gets := fx.heal(t, ctx, fx.file1, cacheDir)
	if fired := fx.tcp.cuts() - cuts; fired != 0 {
		t.Errorf("the armed cut fired %d times during heal; want heal to clear it first", fired)
	}
	if err := accountingErr(gets, fx.file1, 0); len(gets) == 0 || err != nil {
		t.Errorf("heal of a cold cache returned %d xorb GETs %+v, accounting %v; want file1's xorb fetched", len(gets), gets, err)
	}
	fx.assertCached(t, ctx, fx.file1, cacheDir)
	if again := fx.heal(t, ctx, fx.file1, cacheDir); len(again) != 0 {
		t.Errorf("heal of the warm cache returned %d xorb GETs %+v; want none", len(again), again)
	}
}

// selfLCG recomputes the contract's pseudoRandom: s = s*1664525 + 1013904223 from seed, then byte(s>>24), per byte.
func selfLCG(size int, seed uint32) []byte {
	b := make([]byte, size)
	s := seed
	for i := range b {
		s = s*1664525 + 1013904223
		b[i] = byte(s >> 24)
	}
	return b
}

// selfDiff reports how got differs from want: both lengths and the first differing offset.
func selfDiff(got, want []byte) error {
	if bytes.Equal(got, want) {
		return nil
	}
	i := 0
	for i < min(len(got), len(want)) && got[i] == want[i] {
		i++
	}
	return fmt.Errorf("%d bytes, want %d; first difference at offset %d", len(got), len(want), i)
}

// selfLayout returns f's V1 layout, failing t when the fixture left it nil.
func selfLayout(t *testing.T, name string, f *blob) *download.ReconstructionResponseV1 {
	t.Helper()
	if f.layout == nil {
		t.Fatalf("%s.layout is nil", name)
	}
	return f.layout
}

// selfChunkEnds reads the absolute end offset of each chunk of tk from storage, apart from the fixture helpers.
func selfChunkEnds(t *testing.T, fx *fixture, tk task) []int64 {
	t.Helper()
	hash, err := xet.ParseXorbHash(tk.hash)
	if err != nil {
		t.Fatalf("task hash %q: %v", tk.hash, err)
	}
	offsets, err := fx.storage.GetXorbChunkOffsets(t.Context(), hash)
	if err != nil {
		t.Fatalf("chunk offsets of %s: %v", tk.hash, err)
	}
	if tk.chunks.Start >= tk.chunks.End || int(tk.chunks.End) > len(offsets) {
		t.Fatalf("task chunks %+v outside the %d chunks of %s", tk.chunks, len(offsets), tk.hash)
	}
	var ends []int64
	for _, off := range offsets[tk.chunks.Start:tk.chunks.End] {
		ends = append(ends, int64(off))
	}
	return ends
}

// selfFetch downloads f with c through api and fails t unless the output matches f.data.
func selfFetch(t *testing.T, ctx context.Context, fx *fixture, c *client.Client, api apiVersion, f *blob) {
	t.Helper()
	if err := selfDiff(fx.mustDownload(t, ctx, c, api, f).bytes(t), f.data); err != nil {
		t.Fatalf("download output: %v", err)
	}
}

// selfRequire401 fails t unless the download with what ended in an error naming 401.
func selfRequire401(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Errorf("download with %s: err %v; want one naming 401", what, err)
	}
}

// selfGapRun runs a sub-test that calls knownGap after its setup, and reports that sub-test and whether it ran past the call.
func selfGapRun(t *testing.T, name string) (*testing.T, bool) {
	t.Helper()
	var sub *testing.T
	reached := false
	t.Run(name, func(t *testing.T) {
		sub = t
		_ = t.TempDir()
		knownGap(t, "self-test gap")
		reached = true
	})
	return sub, reached
}

// selfGoroutineID returns the id of the calling goroutine.
func selfGoroutineID() string {
	buf := make([]byte, 64)
	return goroutineID(string(buf[:runtime.Stack(buf, false)]))
}

// selfAwaitXorbGet waits for fx's fault proxy to record a xorb GET, failing t if the download reporting on done ends first.
func selfAwaitXorbGet(t *testing.T, fx *fixture, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for len(filter(fx.fp.records(), record.xorbGet)) == 0 {
		select {
		case err := <-done:
			t.Fatalf("download returned %v before any xorb GET reached the fault proxy", err)
		case <-time.After(5 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("no xorb GET reached the fault proxy within %v", waitTimeout)
		}
	}
}
