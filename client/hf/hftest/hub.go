package hftest

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/storage"
)

// Options configures the repository NewHub serves; zero values take the recorded repository's.
type Options struct {
	Repo     string // owner/name, default wzshiming/test
	RepoType string // default model
	Revision string // default main
	Token    string // hub token every request must carry; "" leaves the hub open
	CAS      string // CAS base URL handed out in tokens and reconstruction links
	Issuer   *auth.Issuer
	// Storage is the CAS's store: GetFileHashBySHA256 links committed lfsFile oids to xet hashes and GetReconstructedFile backs the CDN redirect; nil knows no file.
	Storage storage.Storage
	// UploadMode picks a preupload entry's mode from its path, sample and size; nil takes the hub's choice: lfs for binary samples and files of 10 MiB or more, regular otherwise and for empty files.
	UploadMode func(path string, sample []byte, size int64) string
}

// Request is one request the hub received, its Authorization reduced to whether it carried the hub token.
type Request struct {
	Method        string
	Path          string      // path plus query as received
	Authorization string      // "", "Bearer <redacted>" (the options token) or "Bearer <wrong>"
	Header        http.Header // as received, Authorization reduced the same way
	Body          []byte      // API POST payloads
}

// Hub is a fake private Hugging Face hub replaying the recorded behaviour of huggingface.co for one repository.
type Hub struct {
	URL string

	opts Options
	mux  *http.ServeMux

	mu       sync.Mutex
	head     string
	commits  []string
	files    map[string]*hubFile
	requests []Request
}

type hubFile struct {
	lfs     bool
	size    int64
	sha256  string       // lfs content digest, hex
	hash    xet.FileHash // lfs xet hash
	content []byte       // regular content
	etag    string       // regular git blob sha1
}

const (
	errInvalidCredentials = "Invalid username or password."
	errEntryNotFound      = "Entry not found"
	errDanglingLFS        = "Your push was rejected because an LFS pointer pointed to a file that does not exist. For instance, this can happen if you used git push --no-verify to push your changes. Offending file: - %s"
)

// defaultUploadMode is the hub's choice when Options.UploadMode is nil.
func defaultUploadMode(_ string, sample []byte, size int64) string {
	if size == 0 {
		return "regular"
	}
	if size >= 10<<20 || bytes.IndexByte(sample, 0) >= 0 || !utf8.Valid(sample) {
		return "lfs"
	}
	return "regular"
}

// NewHub serves a hub for o on a real listener until the test ends.
func NewHub(t testing.TB, o Options) *Hub {
	t.Helper()
	if o.Repo == "" {
		o.Repo = "wzshiming/test"
	}
	if o.RepoType == "" {
		o.RepoType = "model"
	}
	if o.Revision == "" {
		o.Revision = "main"
	}
	if o.UploadMode == nil {
		o.UploadMode = defaultUploadMode
	}
	h := &Hub{opts: o, mux: http.NewServeMux(), head: sha1Hex([]byte("init " + o.Repo)), files: map[string]*hubFile{}}
	h.mux.HandleFunc("POST /api/{kind}/{owner}/{name}/preupload/{rev}", h.preupload)
	h.mux.HandleFunc("GET /api/{kind}/{owner}/{name}/xet-read-token/{rev}", h.token(auth.Read))
	h.mux.HandleFunc("GET /api/{kind}/{owner}/{name}/xet-write-token/{rev}", h.token(auth.Write))
	h.mux.HandleFunc("POST /api/{kind}/{owner}/{name}/commit/{rev}", h.commit)
	h.mux.HandleFunc("GET /api/{kind}/{owner}/{name}/revision/{rev}", h.revision)
	h.mux.HandleFunc("GET /api/{kind}/{owner}/{name}/tree/{rev}", h.tree)
	h.mux.HandleFunc("POST /api/repos/create", h.createRepo)
	h.mux.HandleFunc("GET /cdn/{sha256}", h.cdn)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	h.URL = srv.URL
	return h
}

// Requests returns every request received so far in arrival order.
func (h *Hub) Requests() []Request {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.requests)
}

// Head returns the current commit oid of the served revision.
func (h *Hub) Head() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.head
}

// Commits returns the oids of the commits created through the API, oldest first.
func (h *Hub) Commits() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.commits)
}

func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.record(r)
	if !h.authorized(w, r) {
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/cdn/") {
		h.mux.ServeHTTP(w, r)
		return
	}
	h.resolve(w, r)
}

func (h *Hub) record(r *http.Request) {
	req := Request{Method: r.Method, Path: r.URL.RequestURI(), Header: r.Header.Clone()}
	switch a := r.Header.Get("Authorization"); {
	case a == "":
	case h.opts.Token != "" && a == "Bearer "+h.opts.Token:
		req.Authorization = "Bearer <redacted>"
	default:
		req.Authorization = "Bearer <wrong>"
	}
	if req.Authorization != "" {
		req.Header.Set("Authorization", req.Authorization)
	}
	if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/") {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		req.Body = body
	}
	h.mu.Lock()
	h.requests = append(h.requests, req)
	h.mu.Unlock()
}

// authorized passes requests carrying the hub token and CDN fetches, which the presigned Location needs no credential for; everything else gets the recorded 401.
func (h *Hub) authorized(w http.ResponseWriter, r *http.Request) bool {
	if h.opts.Token == "" || strings.HasPrefix(r.URL.Path, "/cdn/") || r.Header.Get("Authorization") == "Bearer "+h.opts.Token {
		return true
	}
	w.Header().Set("Www-Authenticate", `Bearer realm="Authentication required", charset="UTF-8"`)
	w.Header().Set("X-Error-Message", errInvalidCredentials)
	if strings.HasPrefix(r.URL.Path, "/api/") {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": errInvalidCredentials})
	} else {
		writeText(w, http.StatusUnauthorized, errInvalidCredentials)
	}
	return false
}

// located checks the repository and revision a request names, answering 404 like the hub when they are not the served ones.
func (h *Hub) located(w http.ResponseWriter, kind, repo, rev string) bool {
	if (kind != "" && kind != h.opts.RepoType+"s") || repo != h.opts.Repo {
		w.Header().Set("X-Error-Code", "RepoNotFound")
		w.Header().Set("X-Error-Message", "Repository not found")
		writeText(w, http.StatusNotFound, "Repository not found")
		return false
	}
	if rev != h.opts.Revision && rev != h.Head() {
		w.Header().Set("X-Error-Code", "RevisionNotFound")
		w.Header().Set("X-Error-Message", "Revision not found")
		writeText(w, http.StatusNotFound, "Revision not found")
		return false
	}
	return true
}

// api checks an API request's repository and revision path values.
func (h *Hub) api(w http.ResponseWriter, r *http.Request) bool {
	return h.located(w, r.PathValue("kind"), r.PathValue("owner")+"/"+r.PathValue("name"), r.PathValue("rev"))
}

func (h *Hub) preupload(w http.ResponseWriter, r *http.Request) {
	if !h.api(w, r) {
		return
	}
	var req struct {
		Files []struct {
			Path   string `json:"path"`
			Sample []byte `json:"sample"`
			Size   int64  `json:"size"`
		} `json:"files"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid preupload payload"})
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	files := make([]map[string]any, 0, len(req.Files))
	for _, f := range req.Files {
		entry := map[string]any{"path": f.Path, "shouldIgnore": false, "uploadMode": h.opts.UploadMode(f.Path, f.Sample, f.Size)}
		switch cur := h.files[f.Path]; {
		case cur == nil:
		case cur.lfs:
			entry["oid"] = cur.sha256
		default:
			entry["oid"] = cur.etag
		}
		files = append(files, entry)
	}
	writeJSON(w, http.StatusOK, map[string]any{"commitOid": h.head, "files": files})
}

// token answers a xet-read-token or xet-write-token request with a CAS token for perm in the body and the X-Xet-* headers.
func (h *Hub) token(perm auth.Permission) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !h.api(w, r) {
			return
		}
		token, exp := "opaque-"+string(perm)+"-token", time.Now().Add(time.Hour).Unix()
		if h.opts.Issuer != nil {
			var err error
			if token, exp, err = h.opts.Issuer.Sign(auth.Grant{Permission: perm}); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
				return
			}
		}
		w.Header().Set("X-Xet-Access-Token", token)
		w.Header().Set("X-Xet-Cas-Url", h.opts.CAS)
		w.Header().Set("X-Xet-Token-Expiration", strconv.FormatInt(exp, 10))
		writeJSON(w, http.StatusOK, map[string]any{"accessToken": token, "casUrl": h.opts.CAS, "exp": exp})
	}
}

func (h *Hub) commit(w http.ResponseWriter, r *http.Request) {
	if !h.api(w, r) {
		return
	}
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "unreadable commit payload"})
		return
	}
	var lines []struct {
		Key   string          `json:"key"`
		Value json.RawMessage `json:"value"`
	}
	for line := range strings.SplitSeq(strings.TrimSpace(string(payload)), "\n") {
		var l struct {
			Key   string          `json:"key"`
			Value json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal([]byte(line), &l); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid commit payload: " + err.Error()})
			return
		}
		lines = append(lines, l)
	}
	if len(lines) == 0 || lines[0].Key != "header" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid commit payload: header line missing"})
		return
	}

	staged := map[string]*hubFile{}
	for _, l := range lines[1:] {
		switch l.Key {
		case "lfsFile":
			var v struct {
				Algo string `json:"algo"`
				OID  string `json:"oid"`
				Path string `json:"path"`
				Size int64  `json:"size"`
			}
			if err := json.Unmarshal(l.Value, &v); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid lfsFile line"})
				return
			}
			hash, ok := h.lookup(r, v.OID)
			if !ok {
				msg := fmt.Sprintf(errDanglingLFS, v.Path)
				w.Header().Set("X-Error-Message", msg)
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": msg})
				return
			}
			staged[v.Path] = &hubFile{lfs: true, size: v.Size, sha256: v.OID, hash: hash}
		case "file":
			var v struct {
				Content  string `json:"content"`
				Encoding string `json:"encoding"`
				Path     string `json:"path"`
			}
			if err := json.Unmarshal(l.Value, &v); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid file line"})
				return
			}
			content := []byte(v.Content)
			if v.Encoding == "base64" {
				if content, err = base64.StdEncoding.DecodeString(v.Content); err != nil {
					writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid base64 content for " + v.Path})
					return
				}
			}
			staged[v.Path] = &hubFile{size: int64(len(content)), content: content, etag: blobSHA1(content)}
		default:
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "unsupported commit operation " + l.Key})
			return
		}
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	maps.Copy(h.files, staged)
	h.head = sha1Hex(append([]byte(h.head), payload...))
	h.commits = append(h.commits, h.head)
	writeJSON(w, http.StatusOK, map[string]any{
		"commitOid":  h.head,
		"commitUrl":  "https://huggingface.co/" + h.opts.Repo + "/commit/" + h.head,
		"hookOutput": "",
		"success":    true,
	})
}

// lookup resolves an lfs oid to the xet hash the CAS store recorded for it.
func (h *Hub) lookup(r *http.Request, oid string) (xet.FileHash, bool) {
	digest, err := hex.DecodeString(oid)
	if err != nil || len(digest) != sha256.Size || h.opts.Storage == nil {
		return xet.FileHash{}, false
	}
	hash, err := h.opts.Storage.GetFileHashBySHA256(r.Context(), "default", [sha256.Size]byte(digest))
	return hash, err == nil
}

// resolve serves /{owner}/{name}/resolve/{rev}/{path}: a redirect with the xet links for lfs files, the content for regular ones.
func (h *Hub) resolve(w http.ResponseWriter, r *http.Request) {
	seg := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 5)
	if len(seg) < 5 || seg[2] != "resolve" || (r.Method != http.MethodGet && r.Method != http.MethodHead) {
		http.NotFound(w, r)
		return
	}
	if !h.located(w, "", seg[0]+"/"+seg[1], seg[3]) {
		return
	}
	h.mu.Lock()
	f, head := h.files[seg[4]], h.head
	h.mu.Unlock()
	w.Header().Set("X-Repo-Commit", head)
	if f == nil {
		w.Header().Set("X-Error-Code", "EntryNotFound")
		w.Header().Set("X-Error-Message", errEntryNotFound)
		writeText(w, http.StatusNotFound, errEntryNotFound)
		return
	}
	w.Header().Set("Accept-Ranges", "bytes")
	if !f.lfs {
		w.Header().Set("ETag", `"`+f.etag+`"`)
		respond(w, http.StatusOK, "text/plain; charset=utf-8", f.content)
		return
	}
	location := h.URL + "/cdn/" + f.sha256
	w.Header().Set("Location", location)
	w.Header().Set("Link", fmt.Sprintf(`<%s/api/%ss/%s/xet-read-token/%s>; rel="xet-auth", <%s/v1/reconstructions/%s>; rel="xet-reconstruction-info"`, h.URL, h.opts.RepoType, h.opts.Repo, head, h.opts.CAS, f.hash))
	w.Header().Set("X-Linked-Etag", `"`+f.sha256+`"`)
	w.Header().Set("X-Linked-Size", strconv.FormatInt(f.size, 10))
	w.Header().Set("X-Xet-Hash", f.hash.String())
	writeText(w, http.StatusFound, "Found. Redirecting to "+location)
}

// cdn stands in for the presigned CDN the resolve redirect names, serving the file reconstructed from the CAS store.
func (h *Hub) cdn(w http.ResponseWriter, r *http.Request) {
	digest, err := hex.DecodeString(r.PathValue("sha256"))
	if err != nil || len(digest) != sha256.Size || h.opts.Storage == nil {
		http.NotFound(w, r)
		return
	}
	content, err := h.opts.Storage.GetReconstructedFile(r.Context(), "default", [sha256.Size]byte(digest))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer content.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", `"`+r.PathValue("sha256")+`"`)
	http.ServeContent(w, r, "", time.Time{}, content)
}

func (h *Hub) revision(w http.ResponseWriter, r *http.Request) {
	if !h.api(w, r) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": h.opts.Repo, "private": h.opts.Token != "", "sha": h.Head()})
}

func (h *Hub) tree(w http.ResponseWriter, r *http.Request) {
	if !h.api(w, r) {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	entries := make([]map[string]any, 0, len(h.files))
	for _, path := range slices.Sorted(maps.Keys(h.files)) {
		f := h.files[path]
		if !f.lfs {
			entries = append(entries, map[string]any{"oid": f.etag, "path": path, "size": f.size, "type": "file"})
			continue
		}
		pointer := fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", f.sha256, f.size)
		entries = append(entries, map[string]any{
			"lfs":     map[string]any{"oid": f.sha256, "pointerSize": len(pointer), "size": f.size},
			"oid":     blobSHA1([]byte(pointer)),
			"path":    path,
			"size":    f.size,
			"type":    "file",
			"xetHash": f.hash.String(),
		})
	}
	writeJSON(w, http.StatusOK, entries)
}

func (h *Hub) createRepo(w http.ResponseWriter, _ *http.Request) {
	msg := fmt.Sprintf("You already created this %s repo: %s", h.opts.RepoType, h.opts.Repo)
	w.Header().Set("X-Error-Message", msg)
	writeJSON(w, http.StatusConflict, map[string]any{"error": msg, "url": "https://huggingface.co/" + h.opts.Repo})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	body, _ := json.Marshal(v)
	respond(w, status, "application/json; charset=utf-8", body)
}

func writeText(w http.ResponseWriter, status int, text string) {
	respond(w, status, "text/plain; charset=utf-8", []byte(text))
}

// respond writes body with the hub's framing: an explicit Content-Length (kept on HEAD) and, off redirects, the weak ETag its web framework adds to every body.
func respond(w http.ResponseWriter, status int, contentType string, body []byte) {
	h := w.Header()
	h.Set("Content-Type", contentType)
	h.Set("Content-Length", strconv.Itoa(len(body)))
	if status/100 != 3 && h.Get("Etag") == "" {
		sum := sha1.Sum(body)
		h.Set("Etag", fmt.Sprintf(`W/"%x-%s"`, len(body), base64.RawStdEncoding.EncodeToString(sum[:])))
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// blobSHA1 is the git object id of a blob holding content.
func blobSHA1(content []byte) string {
	return sha1Hex(append([]byte(fmt.Sprintf("blob %d\x00", len(content))), content...))
}

func sha1Hex(data []byte) string {
	sum := sha1.Sum(data)
	return hex.EncodeToString(sum[:])
}
