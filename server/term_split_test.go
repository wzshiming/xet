package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/storage/storagetest"
	"github.com/wzshiming/xet/xorb"
)

// tsChunks is the chunk count of the 64 MiB fixture: one term, one LZ4 xorb of 64 KiB chunks.
const tsChunks = 1024

// tsRanged requests file bytes [1, 17) MiB, chunks [16, 272) of the fixture.
const tsRanged = "bytes=1048576-17825791"

func tsStoreFile(t *testing.T) (storage.Storage, storagetest.File) {
	t.Helper()
	st, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	lz4 := slices.IndexFunc(storagetest.ChunkClasses, func(c storagetest.ChunkClass) bool { return c.Compression == xorb.CompressionLZ4 })
	return st, storagetest.PutXorbFile(t, t.Context(), st, storagetest.ChunkClasses[lz4].Xorbs(1, tsChunks, 64<<10))
}

func tsGet(t *testing.T, h http.Handler, path, rangeHeader string) []byte {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s with Range %q: status %d: %s", path, rangeHeader, rec.Code, rec.Body)
	}
	return rec.Body.Bytes()
}

func tsDecode(t *testing.T, body []byte) download.ReconstructionResponseV2 {
	t.Helper()
	var resp download.ReconstructionResponseV2
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode V2 body %s: %v", body, err)
	}
	return resp
}

func tsDecodeV1(t *testing.T, body []byte) download.ReconstructionResponseV1 {
	t.Helper()
	var resp download.ReconstructionResponseV1
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode V1 body %s: %v", body, err)
	}
	return resp
}

// tsMasterBody is master's V2 body: the builder's response to the Range header parsed against the file's size,
// JSON-encoded as the handler does.
func tsMasterBody(t *testing.T, h *Handler, sh *shard.Shard, f storagetest.File, path, rangeHeader string) []byte {
	t.Helper()
	rng := download.ParseByteRange(rangeHeader, int64(len(f.Content)))
	resp, err := download.BuildReconstructionResponseV2(t.Context(), h.reconstructionStorage(httptest.NewRequest(http.MethodGet, path, nil)), sh, f.FileHash, rng)
	if err != nil {
		t.Fatal(err)
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(resp); err != nil {
		t.Fatal(err)
	}
	return body.Bytes()
}

// tsMasterBodyV1 is master's V1 body: the builder's response to the Range header parsed against the file's size,
// JSON-encoded as the handler does.
func tsMasterBodyV1(t *testing.T, h *Handler, sh *shard.Shard, f storagetest.File, path, rangeHeader string) []byte {
	t.Helper()
	rng := download.ParseByteRange(rangeHeader, int64(len(f.Content)))
	resp, err := download.BuildReconstructionResponseV1(t.Context(), h.reconstructionStorage(httptest.NewRequest(http.MethodGet, path, nil)), sh, f.FileHash, rng)
	if err != nil {
		t.Fatal(err)
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(resp); err != nil {
		t.Fatal(err)
	}
	return body.Bytes()
}

// tsBounds is first, first+step, ... through last.
func tsBounds(first, step, last uint32) []uint32 {
	var bounds []uint32
	for b := first; b <= last; b += step {
		bounds = append(bounds, b)
	}
	return bounds
}

// tsPieces is the response of the fixture cut at chunk bounds: a term and a descriptor per piece, under url.
func tsPieces(t *testing.T, st storage.Storage, f storagetest.File, url string, offset int64, bounds []uint32) download.ReconstructionResponseV2 {
	t.Helper()
	offsets, err := st.GetXorbChunkOffsets(t.Context(), f.XorbHashes[0])
	if err != nil {
		t.Fatal(err)
	}
	hash := f.XorbHashes[0].String()
	resp := download.ReconstructionResponseV2{OffsetIntoFirstRange: offset}
	fetch := download.XorbMultiRangeFetch{URL: url}
	for i := range len(bounds) - 1 {
		chunks := download.ChunkRange{Start: bounds[i], End: bounds[i+1]}
		start, end, err := xorb.ChunkDataRangeFromOffsets(offsets, chunks.Start, chunks.End)
		if err != nil {
			t.Fatal(err)
		}
		resp.Terms = append(resp.Terms, download.Term{Hash: hash, UnpackedLength: uint64(chunks.End-chunks.Start) * (64 << 10), Range: chunks})
		fetch.Ranges = append(fetch.Ranges, download.XorbRangeDescriptor{Chunks: chunks, Bytes: download.ByteRange{Start: start, End: end}})
	}
	resp.Xorbs = map[string][]download.XorbMultiRangeFetch{hash: {fetch}}
	return resp
}

// tsPiecesV1 is tsPieces as a V1 response: a fetch_info entry under url per piece.
func tsPiecesV1(t *testing.T, st storage.Storage, f storagetest.File, url string, offset int64, bounds []uint32) download.ReconstructionResponseV1 {
	t.Helper()
	v2 := tsPieces(t, st, f, url, offset, bounds)
	resp := download.ReconstructionResponseV1{OffsetIntoFirstRange: v2.OffsetIntoFirstRange, Terms: v2.Terms, FetchInfo: map[string][]download.FetchInfoEntry{}}
	for hash, fetches := range v2.Xorbs {
		for _, rg := range fetches[0].Ranges {
			resp.FetchInfo[hash] = append(resp.FetchInfo[hash], download.FetchInfoEntry{Range: rg.Chunks, URL: url, URLRange: rg.Bytes})
		}
	}
	return resp
}

// tsSummary shows a response's offset, terms and descriptors.
func tsSummary(resp download.ReconstructionResponseV2) string {
	var b strings.Builder
	fmt.Fprintf(&b, "offset %d, terms", resp.OffsetIntoFirstRange)
	for _, term := range resp.Terms {
		fmt.Fprintf(&b, " [%d,%d):%d", term.Range.Start, term.Range.End, term.UnpackedLength)
	}
	for _, fetches := range resp.Xorbs {
		for _, fetch := range fetches {
			fmt.Fprintf(&b, ", fetch %s", fetch.URL)
			for _, rg := range fetch.Ranges {
				fmt.Fprintf(&b, " [%d,%d)@%d-%d", rg.Chunks.Start, rg.Chunks.End, rg.Bytes.Start, rg.Bytes.End)
			}
		}
	}
	return b.String()
}

// tsSummaryV1 shows a V1 response's offset, terms and fetch_info entries with their URLs.
func tsSummaryV1(resp download.ReconstructionResponseV1) string {
	var b strings.Builder
	fmt.Fprintf(&b, "offset %d, terms", resp.OffsetIntoFirstRange)
	for _, term := range resp.Terms {
		fmt.Fprintf(&b, " [%d,%d):%d", term.Range.Start, term.Range.End, term.UnpackedLength)
	}
	for _, entries := range resp.FetchInfo {
		var urls []string
		b.WriteString(", entries")
		for _, e := range entries {
			fmt.Fprintf(&b, " [%d,%d)@%d-%d", e.Range.Start, e.Range.End, e.URLRange.Start, e.URLRange.End)
			if !slices.Contains(urls, e.URL) {
				urls = append(urls, e.URL)
			}
		}
		fmt.Fprintf(&b, " under %s", strings.Join(urls, ", "))
	}
	return b.String()
}

// tsChunkBytes maps the chunk ranges of v1's fetch_info and of v2's descriptors to their packed bytes.
func tsChunkBytes(v1 download.ReconstructionResponseV1, v2 download.ReconstructionResponseV2) (map[string]download.ByteRange, map[string]download.ByteRange) {
	got, want := map[string]download.ByteRange{}, map[string]download.ByteRange{}
	for hash, entries := range v1.FetchInfo {
		for _, e := range entries {
			got[fmt.Sprintf("%s[%d,%d)", hash, e.Range.Start, e.Range.End)] = e.URLRange
		}
	}
	for hash, fetches := range v2.Xorbs {
		for _, fetch := range fetches {
			for _, rg := range fetch.Ranges {
				want[fmt.Sprintf("%s[%d,%d)", hash, rg.Chunks.Start, rg.Chunks.End)] = rg.Bytes
			}
		}
	}
	return got, want
}

// tsMismatchedShards claims one more unpacked byte for the first chunk of each shard's first CAS block.
type tsMismatchedShards struct{ storage.Storage }

func (s tsMismatchedShards) GetShard(ctx context.Context, fileHash xet.FileHash) (*shard.Shard, error) {
	sh, err := s.Storage.GetShard(ctx, fileHash)
	if err != nil {
		return nil, err
	}
	bad := *sh
	bad.CASInfos = slices.Clone(sh.CASInfos)
	bad.CASInfos[0].Chunks = slices.Clone(bad.CASInfos[0].Chunks)
	bad.CASInfos[0].Chunks[0].UnpackedSegBytes++
	return &bad, nil
}

// tsNewClient is a client of the server at srvURL with a cache of its own.
func tsNewClient(t *testing.T, srvURL string) *client.Client {
	t.Helper()
	c, err := client.NewClient(client.WithCache(client.NewCache(t.TempDir(), 0, 0)), client.WithUpstreamProvider(client.StaticUpstreamProvider(srvURL, "")))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// tsDownload resumes DownloadFileV1 or V2, as version says, of f from srvURL after its first offset bytes and wants all of f.
func tsDownload(t *testing.T, srvURL string, f storagetest.File, offset, version int) {
	t.Helper()
	name := filepath.Join(t.TempDir(), "download")
	if err := os.WriteFile(name, f.Content[:offset], 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := os.OpenFile(name, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	c := tsNewClient(t, srvURL)
	fetch := c.DownloadFileV2
	if version == 1 {
		fetch = c.DownloadFileV1
	}
	if err := fetch(t.Context(), f.FileHash, out); err != nil {
		t.Fatalf("DownloadFileV%d from byte %d off a WithTermSplit(16, 1 MiB) server: %v", version, offset, err)
	}
	if _, err := out.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	n, err := io.Copy(h, out)
	if sum := hex.EncodeToString(h.Sum(nil)); err != nil || n != int64(len(f.Content)) || sum != f.SHA256Hex {
		t.Fatalf("DownloadFileV%d from byte %d off a WithTermSplit(16, 1 MiB) server left %d bytes of SHA-256 %s, %v; want the file's %d bytes of %s", version, offset, n, sum, err, len(f.Content), f.SHA256Hex)
	}
}

func TestReconstructionV2TermSplit(t *testing.T) {
	st, f := tsStoreFile(t)
	sh, err := st.GetShard(t.Context(), f.FileHash)
	if err != nil {
		t.Fatal(err)
	}
	v2 := "/v2/reconstructions/" + f.FileHash.String()
	plain := NewHandler(WithStorage(st))
	split := NewHandler(WithStorage(st), WithTermSplit(16, 1<<20))
	url := tsDecode(t, tsGet(t, plain, v2, "")).Xorbs[f.XorbHashes[0].String()][0].URL

	t.Run("default serves master's body", func(t *testing.T) {
		for _, rangeHeader := range []string{"", tsRanged} {
			if got, want := tsGet(t, plain, v2, rangeHeader), tsMasterBody(t, plain, sh, f, v2, rangeHeader); !bytes.Equal(got, want) {
				t.Errorf("V2 with Range %q without WithTermSplit = %s, want master's %s", rangeHeader, got, want)
			}
		}
	})

	t.Run("WithTermSplit(0, 0) serves the default body", func(t *testing.T) {
		off := NewHandler(WithStorage(st), WithTermSplit(0, 0))
		for _, rangeHeader := range []string{"", tsRanged} {
			if got, want := tsGet(t, off, v2, rangeHeader), tsGet(t, plain, v2, rangeHeader); !bytes.Equal(got, want) {
				t.Errorf("V2 with Range %q and WithTermSplit(0, 0) = %s, want the default %s", rangeHeader, got, want)
			}
		}
	})

	t.Run("WithTermSplit(16, 1 MiB) serves 16 terms of 4 MiB", func(t *testing.T) {
		want := tsPieces(t, st, f, url, 0, tsBounds(0, 64, tsChunks))
		if got := tsDecode(t, tsGet(t, split, v2, "")); !reflect.DeepEqual(got, want) {
			t.Errorf("V2 of a 64 MiB term with WithTermSplit(16, 1 MiB) = %s\nwant %s\n(halved into 16 terms of 4 MiB, each with its own descriptor)", tsSummary(got), tsSummary(want))
		}
	})

	t.Run("ranged request has no term past its end", func(t *testing.T) {
		// The term trimmed to the 16 MiB requested halves into 16 pieces of 1 MiB.
		want := tsPieces(t, st, f, url, 0, tsBounds(16, 16, 272))
		if got := tsDecode(t, tsGet(t, split, v2, tsRanged)); !reflect.DeepEqual(got, want) {
			t.Errorf("V2 with Range %s and WithTermSplit(16, 1 MiB) = %s\nwant %s\n(the term is trimmed to the chunks holding the requested bytes, then cut into pieces)", tsRanged, tsSummary(got), tsSummary(want))
		}
	})

	t.Run("pieceBytes keeps 1.5 MiB requested whole", func(t *testing.T) {
		// Requested [1, 2.5) MiB: every cut leaves a piece under the 1 MiB floor.
		const rangeHeader = "bytes=1048576-2621439"
		want := tsPieces(t, st, f, url, 0, []uint32{16, 40})
		if got := tsDecode(t, tsGet(t, split, v2, rangeHeader)); !reflect.DeepEqual(got, want) {
			t.Errorf("V2 with Range %s and WithTermSplit(16, 1 MiB) = %s\nwant %s\n(no cut leaves both pieces 1 MiB of requested bytes, so the trimmed term stays whole)", rangeHeader, tsSummary(got), tsSummary(want))
		}
	})

	t.Run("SplitTermsV2 error serves the default body", func(t *testing.T) {
		bad := tsMismatchedShards{st}
		// The builder alone serves the doctored shard.
		want := tsGet(t, NewHandler(WithStorage(bad)), v2, "")
		rec := httptest.NewRecorder()
		NewHandler(WithStorage(bad), WithTermSplit(16, 1<<20)).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, v2, nil))
		if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), want) {
			t.Errorf("V2 with WithTermSplit(16, 1 MiB) over a CAS block one byte past its term = status %d: %s\nwant 200 and the default body %s\n(a SplitTermsV2 error fails open to the unsplit response)", rec.Code, rec.Body, want)
		}
	})

	srv := httptest.NewServer(split)
	defer srv.Close()
	t.Run("DownloadFileV2 is byte-exact", func(t *testing.T) { tsDownload(t, srv.URL, f, 0, 2) })
	t.Run("DownloadFileV2 resumes byte-exact", func(t *testing.T) {
		// Inside the first piece, inside a later one, and in the last 1000 bytes.
		for _, offset := range []int{1000000, 48<<20 + 12345, len(f.Content) - 1000} {
			t.Run(fmt.Sprint(offset), func(t *testing.T) { tsDownload(t, srv.URL, f, offset, 2) })
		}
	})
}

func TestReconstructionV1TermSplit(t *testing.T) {
	st, f := tsStoreFile(t)
	sh, err := st.GetShard(t.Context(), f.FileHash)
	if err != nil {
		t.Fatal(err)
	}
	v1 := "/v1/reconstructions/" + f.FileHash.String()
	plain := NewHandler(WithStorage(st))
	split := NewHandler(WithStorage(st), WithTermSplit(16, 1<<20))
	url := tsDecodeV1(t, tsGet(t, plain, v1, "")).FetchInfo[f.XorbHashes[0].String()][0].URL
	// The whole file, a range, a resume and a suffix: requests WithTermSplit(16, 1 MiB) splits.
	ranges := []string{"", tsRanged, "bytes=1000000-", "bytes=-16777216"}
	// wantSplit stops t unless V1 splits the request under WithTermSplit(16, 1 MiB), so t cannot pass on unsplit bodies.
	wantSplit := func(t *testing.T, rangeHeader string) {
		t.Helper()
		if n := len(tsDecodeV1(t, tsGet(t, split, v1, rangeHeader)).Terms); n < 2 {
			t.Fatalf("precondition: V1 with Range %q and WithTermSplit(16, 1 MiB) = %d term(s), want it split", rangeHeader, n)
		}
	}

	t.Run("WithTermSplit(16, 1 MiB) serves 16 terms of 4 MiB", func(t *testing.T) {
		want := tsPiecesV1(t, st, f, url, 0, tsBounds(0, 64, tsChunks))
		if got := tsDecodeV1(t, tsGet(t, split, v1, "")); !reflect.DeepEqual(got, want) {
			t.Errorf("V1 of a 64 MiB term with WithTermSplit(16, 1 MiB) = %s\nwant %s\n(halved into 16 terms of 4 MiB, each with its own fetch_info entry under the term's URL)", tsSummaryV1(got), tsSummaryV1(want))
		}
	})

	t.Run("ranged request has no term past its end", func(t *testing.T) {
		// The term trimmed to the 16 MiB requested halves into 16 pieces of 1 MiB.
		want := tsPiecesV1(t, st, f, url, 0, tsBounds(16, 16, 272))
		if got := tsDecodeV1(t, tsGet(t, split, v1, tsRanged)); !reflect.DeepEqual(got, want) {
			t.Errorf("V1 with Range %s and WithTermSplit(16, 1 MiB) = %s\nwant %s\n(the term is trimmed to the chunks holding the requested bytes, then cut into pieces)", tsRanged, tsSummaryV1(got), tsSummaryV1(want))
		}
	})

	t.Run("pieceBytes keeps 1.5 MiB requested whole", func(t *testing.T) {
		wantSplit(t, "")
		// Requested [1, 2.5) MiB: every cut leaves a piece under the 1 MiB floor.
		const rangeHeader = "bytes=1048576-2621439"
		want := tsPiecesV1(t, st, f, url, 0, []uint32{16, 40})
		if got := tsDecodeV1(t, tsGet(t, split, v1, rangeHeader)); !reflect.DeepEqual(got, want) {
			t.Errorf("V1 with Range %s and WithTermSplit(16, 1 MiB) = %s\nwant %s\n(no cut leaves both pieces 1 MiB of requested bytes, so the trimmed term stays whole)", rangeHeader, tsSummaryV1(got), tsSummaryV1(want))
		}
	})

	t.Run("V1 serves V2's pieces", func(t *testing.T) {
		v2 := "/v2/reconstructions/" + f.FileHash.String()
		for _, rangeHeader := range ranges {
			got, want := tsDecodeV1(t, tsGet(t, split, v1, rangeHeader)), tsDecode(t, tsGet(t, split, v2, rangeHeader))
			gotBytes, wantBytes := tsChunkBytes(got, want)
			if got.OffsetIntoFirstRange != want.OffsetIntoFirstRange || !slices.Equal(got.Terms, want.Terms) || !maps.Equal(gotBytes, wantBytes) {
				t.Errorf("V1 with Range %q and WithTermSplit(16, 1 MiB) = %s\nV2 = %s\n(V1 serves V2's pieces, each url_range the bytes of the V2 descriptor of its chunks)", rangeHeader, tsSummaryV1(got), tsSummary(want))
			}
		}
	})

	t.Run("default serves master's body", func(t *testing.T) {
		for _, rangeHeader := range ranges {
			wantSplit(t, rangeHeader)
			if got, want := tsGet(t, plain, v1, rangeHeader), tsMasterBodyV1(t, plain, sh, f, v1, rangeHeader); !bytes.Equal(got, want) {
				t.Errorf("V1 with Range %q without WithTermSplit = %s, want master's %s", rangeHeader, got, want)
			}
		}
	})

	t.Run("WithTermSplit(0, 0) serves the default body", func(t *testing.T) {
		off := NewHandler(WithStorage(st), WithTermSplit(0, 0))
		for _, rangeHeader := range ranges {
			wantSplit(t, rangeHeader)
			if got, want := tsGet(t, off, v1, rangeHeader), tsGet(t, plain, v1, rangeHeader); !bytes.Equal(got, want) {
				t.Errorf("V1 with Range %q and WithTermSplit(0, 0) = %s, want the default %s", rangeHeader, got, want)
			}
		}
	})

	t.Run("SplitTermsV1 error serves the default body", func(t *testing.T) {
		wantSplit(t, "")
		bad := tsMismatchedShards{st}
		// The builder alone serves the doctored shard.
		want := tsGet(t, NewHandler(WithStorage(bad)), v1, "")
		rec := httptest.NewRecorder()
		NewHandler(WithStorage(bad), WithTermSplit(16, 1<<20)).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, v1, nil))
		if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), want) {
			t.Errorf("V1 with WithTermSplit(16, 1 MiB) over a CAS block one byte past its term = status %d: %s\nwant 200 and the default body %s\n(a SplitTermsV1 error fails open to the unsplit response)", rec.Code, rec.Body, want)
		}
	})

	t.Run("batch ignores WithTermSplit", func(t *testing.T) {
		wantSplit(t, "")
		batch := "/reconstructions?file_id=" + f.FileHash.String()
		if got, want := tsGet(t, split, batch, ""), tsGet(t, plain, batch, ""); !bytes.Equal(got, want) {
			t.Errorf("GET %s with WithTermSplit(16, 1 MiB) = %s, want the body without it %s\n(the batch endpoint is not split)", batch, got, want)
		}
	})

	srv := httptest.NewServer(split)
	defer srv.Close()
	t.Run("DownloadFileV1 is byte-exact", func(t *testing.T) {
		wantSplit(t, "")
		tsDownload(t, srv.URL, f, 0, 1)
	})
	t.Run("DownloadFileV1 resumes byte-exact", func(t *testing.T) {
		// Inside the first piece, inside a later one, and 3 MiB before the end, still cut in two.
		for _, offset := range []int{1000000, 48<<20 + 12345, 61<<20 + 777} {
			t.Run(fmt.Sprint(offset), func(t *testing.T) {
				wantSplit(t, fmt.Sprintf("bytes=%d-", offset))
				tsDownload(t, srv.URL, f, offset, 1)
			})
		}
	})
	t.Run("ranged NewReaderV1 is byte-exact", func(t *testing.T) {
		wantSplit(t, tsRanged)
		c := tsNewClient(t, srv.URL)
		resp, err := c.GetReconstructionV1(t.Context(), f.FileHash, http.Header{"Range": {tsRanged}})
		if err != nil {
			t.Fatal(err)
		}
		r, err := download.NewReaderV1(t.Context(), c, resp, download.WithCacheManager(download.NewCacheManager(t.TempDir(), 0)))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		got, err := io.ReadAll(r)
		// The response runs from byte 1 MiB through the end of the chunk holding the last byte requested.
		want := f.Content[1<<20:][:download.ExpectedLengthV1(resp)]
		if err != nil || !bytes.Equal(got, want) || len(got) < 16<<20 {
			t.Errorf("NewReaderV1 over V1 with Range %s off a WithTermSplit(16, 1 MiB) server read %d bytes, %v; want the %d file bytes from 1 MiB on, the 16 MiB requested among them", tsRanged, len(got), err, len(want))
		}
	})
}

func TestBatchReconstructionListsEveryTerm(t *testing.T) {
	st, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	// One single-chunk xorb per part: a file of three terms.
	f := storagetest.PutFile(t, t.Context(), st, [][]byte{[]byte("first part"), []byte("second part"), []byte("third part")})
	h := NewHandler(WithStorage(st))
	want := tsDecodeV1(t, tsGet(t, h, "/v1/reconstructions/"+f.FileHash.String(), "")).Terms
	if len(want) != 3 {
		t.Fatalf("precondition: V1 of a file of three parts = %d term(s), want 3", len(want))
	}
	batch := "/reconstructions?file_id=" + f.FileHash.String()
	var resp download.BatchReconstructionResponse
	if err := json.Unmarshal(tsGet(t, h, batch, ""), &resp); err != nil {
		t.Fatal(err)
	}
	if got := resp.Files[f.FileHash.String()]; !slices.Equal(got, want) {
		t.Errorf("GET %s lists %d term(s) %+v, want V1's %d %+v: the batch serves each file whole", batch, len(got), got, len(want), want)
	}
}
