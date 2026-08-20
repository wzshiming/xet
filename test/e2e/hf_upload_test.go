package e2e_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/hf"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/token"
)

const (
	hubRepoID   = "org/repo"
	hubFileName = "model.bin"
	hubHFToken  = "e2e-hf-token"
	hubVerifyID = "verify-secret"
	hubCommit   = "0123456789abcdef0123456789abcdef01234567"
)

// TestHFUploadEndToEnd uploads through the hf helper against a fake Hub
// backed by a local CAS server, then reads the content back through the Go
// download helper, the sha256 bridge, and (when installed) the official hf
// CLI, proving both sides of the xet protocol interoperate.
func TestHFUploadEndToEnd(t *testing.T) {
	casURL, hub, hubURL := startHFStack(t)

	target := hf.Target{Endpoint: hubURL, RepoID: hubRepoID}
	data := deterministicData(3*128*1024 + 7919)
	digest := sha256.Sum256(data)
	wantSHA256 := hex.EncodeToString(digest[:])

	var fileHash xet.FileHash
	ok := t.Run("upload", func(t *testing.T) {
		result, err := hf.Upload(t.Context(), target, hubHFToken, bytes.NewReader(data),
			client.WithCacheDir(t.TempDir()))
		if err != nil {
			t.Fatalf("upload: %v", err)
		}
		if result.AlreadyExists {
			t.Fatal("AlreadyExists = true on first upload")
		}
		if result.OID != wantSHA256 {
			t.Fatalf("OID = %s, want %s", result.OID, wantSHA256)
		}
		if result.Size != int64(len(data)) {
			t.Fatalf("Size = %d, want %d", result.Size, len(data))
		}
		if result.FileHash == (xet.FileHash{}) {
			t.Fatal("FileHash is zero after upload")
		}
		verifies := hub.verifyCalls()
		if len(verifies) != 1 || verifies[0].OID != wantSHA256 || verifies[0].Size != int64(len(data)) {
			t.Fatalf("verify calls = %+v, want one call with oid %s size %d", verifies, wantSHA256, len(data))
		}
		fileHash = result.FileHash
	})
	if !ok {
		t.Fatal("upload failed; skipping downstream verification")
	}
	hub.addFile(hubFileName, wantSHA256, int64(len(data)), fileHash.String())

	t.Run("re-upload already exists", func(t *testing.T) {
		result, err := hf.Upload(t.Context(), target, hubHFToken, bytes.NewReader(data),
			client.WithCacheDir(t.TempDir()))
		if err != nil {
			t.Fatalf("re-upload: %v", err)
		}
		if !result.AlreadyExists {
			t.Fatal("AlreadyExists = false on re-upload")
		}
		if got := len(hub.verifyCalls()); got != 1 {
			t.Fatalf("verify calls = %d, want 1 (skipped upload must not verify)", got)
		}
	})

	t.Run("go resolve and download", func(t *testing.T) {
		resolveURL := hubURL + "/" + hubRepoID + "/resolve/main/" + hubFileName
		gotHash, provider, err := hf.ResolveDownload(t.Context(), nil, resolveURL)
		if err != nil {
			t.Fatalf("resolve download: %v", err)
		}
		if gotHash != fileHash {
			t.Fatalf("resolved hash = %s, want %s", gotHash, fileHash)
		}

		c, err := client.NewClient(client.WithCacheDir(t.TempDir()))
		if err != nil {
			t.Fatal(err)
		}
		out, err := os.Create(filepath.Join(t.TempDir(), hubFileName))
		if err != nil {
			t.Fatal(err)
		}
		defer out.Close()
		if err := c.DownloadFileWithAuthProvider(t.Context(), provider, gotHash, out); err != nil {
			t.Fatalf("download: %v", err)
		}
		assertFileSHA256(t, out.Name(), wantSHA256)
	})

	t.Run("xet-bridge download", func(t *testing.T) {
		resp := doRequest(t, http.MethodGet, casURL+"/xet-bridge/"+wantSHA256, nil)
		assertResponse(t, resp, http.StatusOK, data)
	})

	t.Run("hf cli download", func(t *testing.T) {
		hfBin, err := exec.LookPath("hf")
		if err != nil {
			t.Skip(`hf CLI not found on PATH; install with: pip install -U "huggingface_hub[cli,hf_xet]"`)
		}
		localDir := t.TempDir()
		cmd := exec.CommandContext(t.Context(), hfBin, "download", hubRepoID, hubFileName, "--local-dir", localDir)
		cmd.Env = hfCLIEnv(t, hubURL)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("hf download failed: %v\n%s", err, out)
		}
		assertFileSHA256(t, filepath.Join(localDir, hubFileName), wantSHA256)
	})
}

// startHFStack starts a token-guarded CAS server and a fake Hub wired to it,
// returning the CAS URL, the hub state, and the hub URL.
func startHFStack(t *testing.T) (string, *xetHub, string) {
	t.Helper()

	// The CAS URL must be known to storage before the handler exists so
	// reconstruction term URLs are absolute; route through an atomic holder.
	var inner atomic.Value
	casSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inner.Load().(http.Handler).ServeHTTP(w, r)
	}))
	t.Cleanup(casSrv.Close)

	stor, err := storage.NewFileStorage(
		storage.WithBasePath(t.TempDir()),
		storage.WithBaseURL(casSrv.URL),
	)
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := token.NewIssuer(nil, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	inner.Store(http.Handler(server.NewHandler(
		server.WithStorage(stor),
		server.WithAuthFunc(func(tok string) bool { return issuer.Validate(tok, time.Now()) }),
	)))

	hub := &xetHub{t: t, casURL: casSrv.URL, mint: issuer.Mint}
	hubSrv := httptest.NewServer(hub)
	t.Cleanup(hubSrv.Close)
	hub.hubURL = hubSrv.URL

	return casSrv.URL, hub, hubSrv.URL
}

// xetHub emulates the Hugging Face Hub endpoints involved in xet transfers:
// LFS batch negotiation, LFS verify, xet token minting, and file resolve.
type xetHub struct {
	t      *testing.T
	casURL string
	hubURL string
	mint   func(time.Time) (string, int64)

	mu       sync.Mutex
	existing map[string]int64      // verified OID -> size
	files    map[string]xetHubFile // filename -> resolve entry
	verifies []verifyCall
}

type xetHubFile struct {
	sha256   string
	size     int64
	fileHash string
}

type verifyCall struct {
	OID  string `json:"oid"`
	Size int64  `json:"size"`
}

func (h *xetHub) addFile(name, sha256Hex string, size int64, fileHash string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.files == nil {
		h.files = map[string]xetHubFile{}
	}
	h.files[name] = xetHubFile{sha256: sha256Hex, size: size, fileHash: fileHash}
}

func (h *xetHub) verifyCalls() []verifyCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]verifyCall(nil), h.verifies...)
}

func (h *xetHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/"+hubRepoID+".git/info/lfs/objects/batch":
		h.handleBatch(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/lfs-verify":
		h.handleVerify(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/api/models/"+hubRepoID+"/xet-read-token/main":
		h.handleToken(w)
	case strings.HasPrefix(r.URL.Path, "/"+hubRepoID+"/resolve/main/"):
		h.handleResolve(w, r)
	default:
		h.t.Logf("fake hub: unhandled %s %s", r.Method, r.URL.Path)
		http.NotFound(w, r)
	}
}

func (h *xetHub) handleBatch(w http.ResponseWriter, r *http.Request) {
	if auth := r.Header.Get("Authorization"); auth != "Bearer "+hubHFToken {
		h.t.Errorf("batch auth = %q, want bearer %s", auth, hubHFToken)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var req struct {
		Operation string       `json:"operation"`
		Transfers []string     `json:"transfers"`
		Objects   []verifyCall `json:"objects"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Operation != "upload" || len(req.Objects) != 1 {
		http.Error(w, "unexpected batch request", http.StatusUnprocessableEntity)
		return
	}
	obj := req.Objects[0]

	type action struct {
		Href   string            `json:"href"`
		Header map[string]string `json:"header,omitempty"`
	}
	respObj := map[string]any{"oid": obj.OID, "size": obj.Size}

	h.mu.Lock()
	_, exists := h.existing[obj.OID]
	h.mu.Unlock()
	if !exists {
		accessToken, _ := h.mint(time.Now())
		respObj["actions"] = map[string]action{
			"upload": {
				Href: h.casURL,
				Header: map[string]string{
					"X-Xet-Cas-Url":      h.casURL,
					"X-Xet-Access-Token": accessToken,
				},
			},
			"verify": {
				Href:   h.hubURL + "/lfs-verify",
				Header: map[string]string{"X-Verify-Id": hubVerifyID},
			},
		}
	}

	w.Header().Set("Content-Type", "application/vnd.git-lfs+json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"transfer": "xet",
		"objects":  []any{respObj},
	})
}

func (h *xetHub) handleVerify(w http.ResponseWriter, r *http.Request) {
	if got := r.Header.Get("X-Verify-Id"); got != hubVerifyID {
		h.t.Errorf("verify header X-Verify-Id = %q, want %q", got, hubVerifyID)
		http.Error(w, "bad verify header", http.StatusForbidden)
		return
	}
	var call verifyCall
	if err := json.NewDecoder(r.Body).Decode(&call); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	h.verifies = append(h.verifies, call)
	if h.existing == nil {
		h.existing = map[string]int64{}
	}
	h.existing[call.OID] = call.Size
	h.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (h *xetHub) handleToken(w http.ResponseWriter) {
	accessToken, exp := h.mint(time.Now())
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"casUrl":%q,"accessToken":%q,"exp":%d}`, h.casURL, accessToken, exp)
}

func (h *xetHub) handleResolve(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/"+hubRepoID+"/resolve/main/")
	h.mu.Lock()
	f, found := h.files[name]
	h.mu.Unlock()
	if !found {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("X-Repo-Commit", hubCommit)
	w.Header().Set("ETag", `"`+f.sha256+`"`)
	w.Header().Set("X-Linked-Size", fmt.Sprint(f.size))
	w.Header().Set("X-Xet-Hash", f.fileHash)
	w.Header().Add("Link", fmt.Sprintf("<%s/api/models/%s/xet-read-token/main>; rel=\"xet-auth\"", h.hubURL, hubRepoID))
	w.Header().Add("Link", fmt.Sprintf("<%s/v1/reconstructions/%s>; rel=\"xet-reconstruction-info\"", h.casURL, f.fileHash))
	w.Header().Set("Location", h.casURL+"/xet-bridge/"+f.sha256)
	w.WriteHeader(http.StatusFound)
}

// hfCLIEnv pins the hf CLI to the fake hub with isolated caches and no proxy
// interference on loopback traffic.
func hfCLIEnv(t *testing.T, endpoint string) []string {
	t.Helper()
	var env []string
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		switch strings.ToUpper(name) {
		case "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY":
			continue
		}
		if strings.HasPrefix(name, "HF_") || strings.HasPrefix(name, "HUGGING") {
			continue
		}
		env = append(env, kv)
	}
	return append(env,
		"HF_ENDPOINT="+endpoint,
		"HF_HOME="+t.TempDir(),
		"HF_HUB_DISABLE_TELEMETRY=1",
		"HF_HUB_DISABLE_PROGRESS_BARS=1",
		"HF_HUB_ETAG_TIMEOUT=60",
		"NO_PROXY=127.0.0.1,localhost",
		"no_proxy=127.0.0.1,localhost",
	)
}

func assertFileSHA256(t *testing.T, path, want string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != want {
		t.Fatalf("sha256 = %s, want %s", got, want)
	}
}
