package mirror_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/hf"
	"github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/upload"
)

const hfPath = "/acme/network-fixture/resolve/main/model.bin"
const hfCommit = "0123456789abcdef0123456789abcdef01234567"

// TestMirrorHTTPFirstThenXet is a complete deployment
// example: a fixed HF route fronts a real Xet server, a mirror fronts that HF
// service. A cold Xet-capable origin is served as bandwidth-limited HTTP first;
// only after conversion does an Xet client receive Xet metadata.
func TestMirrorHTTPFirstThenXet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	originDir := t.TempDir()
	// Include the per-run path so repeated tests cannot reuse a process-global
	// chunk cache and accidentally bypass the network fault fixture.
	fixture := []byte(originDir + "\n" + strings.Repeat("hf mirror end-to-end network fixture\n", 32_000))

	originFS, err := storage.NewFileStorage(storage.WithBasePath(originDir))
	if err != nil {
		t.Fatal(err)
	}
	originStore := &urlStorage{Storage: originFS}
	originHash, err := upload.UploadFile(ctx, storageUploadAdapter{originStore}, strings.NewReader(string(fixture)))
	if err != nil {
		t.Fatal(err)
	}

	var originURL string
	originCAS := server.NewHandler(server.WithStorage(originStore))
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case hfPath:
			// This is the fixed HF-style response consumed by both clients and
			// by the mirror cache fill.
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", strconv.Itoa(len(fixture)))
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("ETag", `"`+originHash.String()+`"`)
			w.Header().Set("X-Repo-Commit", hfCommit)
			w.Header().Set("Link", fmt.Sprintf("<%s/v1/reconstructions/%s>; rel=\"xet-reconstruction-info\", <%s/api/xet-auth>; rel=\"xet-auth\"", originURL, originHash, originURL))
			if r.Method != http.MethodHead {
				writeSlowly(w, fixture)
			}
		case "/api/xet-auth":
			_ = json.NewEncoder(w).Encode(map[string]any{"casUrl": originURL, "accessToken": "", "exp": time.Now().Add(time.Hour).Unix()})
		default:
			originCAS.ServeHTTP(w, r)
		}
	}))
	defer origin.Close()
	originURL = origin.URL
	originStore.baseURL = originURL

	mirrorRoot := t.TempDir()
	mirrorFS, err := storage.NewFileStorage(storage.WithBasePath(mirrorRoot))
	if err != nil {
		t.Fatal(err)
	}
	mirrorStore := &urlStorage{Storage: mirrorFS}
	// hf-xet parses the access token as a JWT to determine refresh timing.
	const mirrorToken = "eyJhbGciOiJub25lIn0.eyJleHAiOjQxMDI0NDQ4MDB9."
	proxy, err := mirror.NewHandler(mirror.Options{Upstream: originURL, CacheDir: filepath.Join(mirrorRoot, "mirror"), Storage: mirrorStore, Concurrency: 2, LocalToken: mirrorToken})
	if err != nil {
		t.Fatal(err)
	}
	mirrorHTTP := server.NewHandler(server.WithStorage(mirrorStore), server.WithAuthFunc(func(token string) bool { return token == mirrorToken }), server.WithNext(proxy))
	mirrorServer := httptest.NewServer(mirrorHTTP)
	defer mirrorServer.Close()
	mirrorStore.baseURL = mirrorServer.URL
	downloadURL := mirrorServer.URL + hfPath

	if _, _, err := hf.ResolveDownload(ctx, nil, downloadURL); err == nil {
		t.Fatal("cold Xet-capable upstream was advertised to downstream")
	}
	resp, err := http.DefaultClient.Get(downloadURL)
	if err != nil {
		t.Fatal(err)
	}
	httpData, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	assertDownload(t, "cold HTTP", struct {
		data []byte
		err  error
	}{httpData, err}, fixture)
	// The mirror must eventually commit the independently running cache fill.
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := http.Head(downloadURL)
		if err == nil && resp.Header.Get("X-Cache-Status") == "HIT" {
			resp.Body.Close()
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatal("mirror cache did not become ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
	hash, provider, err := hf.ResolveDownload(ctx, nil, downloadURL)
	if err != nil {
		t.Fatal(err)
	}
	out, err := os.CreateTemp(t.TempDir(), "xet-client-*")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	xetClient, _ := client.NewClient(client.WithRetries(8), client.WithConcurrency(2))
	if err = xetClient.DownloadFileWithAuthProvider(ctx, provider, hash, out); err == nil {
		_, err = out.Seek(0, io.SeekStart)
	}
	var xetData []byte
	if err == nil {
		xetData, err = io.ReadAll(out)
	}
	assertDownload(t, "warm Xet", struct {
		data []byte
		err  error
	}{xetData, err}, fixture)
	runHFCLIClients(ctx, t, mirrorServer.URL, fixture)
}

// runHFCLIClients validates the public Hugging Face CLI in both modes. The
// clients deliberately use separate homes and local directories, so neither
// one can satisfy the other from the HF cache and their output files differ.
func runHFCLIClients(ctx context.Context, t *testing.T, endpoint string, want []byte) {
	t.Helper()
	hfBinary, err := exec.LookPath("hf")
	if err != nil {
		if os.Getenv("HF_E2E_REQUIRE_CLI") == "1" {
			t.Fatal("HF_E2E_REQUIRE_CLI=1 but hf CLI is not installed on PATH")
		}
		t.Log("hf CLI not installed; skipping optional black-box hf download clients")
		return
	}
	type cliResult struct {
		name, path string
		output     []byte
		err        error
	}
	results := make(chan cliResult, 2)
	start := make(chan struct{})
	for _, tc := range []struct {
		name       string
		disableXet bool
	}{{"http", true}, {"xet", false}} {
		tc := tc
		go func() {
			<-start
			root := t.TempDir()
			localDir := filepath.Join(root, "output-"+tc.name)
			home := filepath.Join(root, "home-"+tc.name)
			args := []string{"download", "acme/network-fixture", "model.bin", "--revision", "main", "--local-dir", localDir, "--force-download"}
			cmd := exec.CommandContext(ctx, hfBinary, args...)
			disable := "0"
			if tc.disableXet {
				disable = "1"
			}
			cmd.Env = append(os.Environ(), "HF_ENDPOINT="+endpoint, "HF_HOME="+home, "HF_HUB_CACHE="+filepath.Join(home, "hub"), "HF_HUB_DISABLE_XET="+disable, "HF_HUB_DISABLE_TELEMETRY=1", "NO_PROXY=localhost,127.0.0.1,::1", "no_proxy=localhost,127.0.0.1,::1")
			output, err := cmd.CombinedOutput()
			results <- cliResult{name: tc.name, path: filepath.Join(localDir, "model.bin"), output: output, err: err}
		}()
	}
	close(start)
	seenPaths := map[string]struct{}{}
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("hf download (%s) failed: %v\n%s", result.name, result.err, result.output)
		}
		if _, duplicate := seenPaths[result.path]; duplicate {
			t.Fatalf("hf clients reused output path %q", result.path)
		}
		seenPaths[result.path] = struct{}{}
		got, err := os.ReadFile(result.path)
		if err != nil {
			t.Fatalf("read hf download (%s) output: %v", result.name, err)
		}
		if string(got) != string(want) {
			t.Fatalf("hf download (%s) content mismatch", result.name)
		}
	}
}

func assertDownload(t *testing.T, name string, got struct {
	data []byte
	err  error
}, want []byte) {
	t.Helper()
	if got.err != nil {
		t.Fatalf("%s download failed: %v", name, got.err)
	}
	if string(got.data) != string(want) {
		t.Fatalf("%s content mismatch: got %d bytes, want %d", name, len(got.data), len(want))
	}
}

func writeSlowly(w io.Writer, data []byte) {
	for len(data) > 0 {
		n := min(8*1024, len(data))
		_, _ = w.Write(data[:n])
		data = data[n:]
		time.Sleep(time.Millisecond)
	}
}

type urlStorage struct {
	storage.Storage
	baseURL string
}

func (s *urlStorage) GetXorbURL(namespace string, hash xet.Hash) string {
	return s.baseURL + "/v1/xorbs/" + namespace + "/" + hash.String()
}

type storageUploadAdapter struct{ storage.Storage }

func (a storageUploadAdapter) HasXorb(ctx context.Context, hash xet.Hash) (bool, error) {
	return a.Storage.HasXorb(ctx, "default", hash)
}
func (a storageUploadAdapter) UploadXorb(ctx context.Context, hash xet.Hash, r io.ReadSeeker) (*upload.XorbUploadResponse, error) {
	inserted, err := a.Storage.PutXorb(ctx, "default", hash, r)
	return &upload.XorbUploadResponse{WasInserted: inserted}, err
}
func (a storageUploadAdapter) UploadShard(ctx context.Context, value *shard.Shard) (*upload.ShardUploadResponse, error) {
	inserted, err := a.Storage.PutShard(ctx, value)
	result := 0
	if inserted {
		result = 1
	}
	return &upload.ShardUploadResponse{Result: result}, err
}
func (a storageUploadAdapter) QueryDedupShards(context.Context, []xet.Hash) (map[xet.Hash]*upload.DeduplicationResult, error) {
	return map[xet.Hash]*upload.DeduplicationResult{}, nil
}
