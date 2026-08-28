// Package hf serves the Hugging Face hub front end of a mirror: hub-style
// resolve requests are answered from the local cache (ingesting on miss
// through the mirror engine), the token endpoints hand downstream clients
// their CAS credential, and tree listings are rewritten so xet clients stay
// on the mirror. Once a file is ready, the resolve response carries both the
// xet Link headers (pointing at the CAS server's reconstruction and token
// endpoints) and the normal plain redirect, so xet clients follow the links
// while plain clients ignore them. Everything else falls through to next;
// wire NewUpstreamProxy there to forward the control plane to the upstream
// hub. The handler is composed behind the server package's CAS routes via
// server.WithNext.
package hf

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/wzshiming/xet/mirror"
)

// Handler serves the hub front end routes.
type Handler struct {
	mirror    *mirror.Mirror
	root      *mux.Router
	next      http.Handler
	external  string
	mintToken func(now time.Time) (token string, exp int64)
}

// Option defines a functional option for configuring the Handler.
type Option func(*Handler)

// WithMirror sets the mirror engine backing the hub front end. Required.
func WithMirror(m *mirror.Mirror) Option {
	return func(h *Handler) {
		h.mirror = m
	}
}

// WithNext sets the next http.Handler to call if a request does not match
// any hub route.
func WithNext(next http.Handler) Option {
	return func(h *Handler) {
		h.next = next
	}
}

// WithExternalURL sets the externally visible base URL used in the hub front
// end's Link headers and minted tokens. When empty it is derived from each
// request.
func WithExternalURL(external string) Option {
	return func(h *Handler) {
		h.external = strings.TrimRight(external, "/")
	}
}

// WithMintToken sets the function the hub token endpoint uses to mint
// downstream CAS tokens, typically the Mint of a token.Issuer shared with
// the CAS server's AuthFunc. When unset the endpoint returns an empty
// anonymous token, suitable for an unauthenticated CAS.
func WithMintToken(mint func(now time.Time) (token string, exp int64)) Option {
	return func(h *Handler) {
		h.mintToken = mint
	}
}

// NewHandler creates a handler for the hub front end.
func NewHandler(opts ...Option) *Handler {
	h := &Handler{
		root: mux.NewRouter(),
	}

	for _, opt := range opts {
		opt(h)
	}

	h.registerRoutes()
	return h
}

// ServeHTTP implements http.Handler
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.root.ServeHTTP(w, r)
}

const tokenEndpointPath = "/xet-token"

// registerRoutes sets up the hub routes; requests that match no route — or a
// route's path with the wrong method — fall through to next (404 when
// unset). Routing matches the encoded path, so escaped segments (e.g.
// revisions like refs%2Fpr%2F1) stay single path elements, and the matched
// variables are handed to the mirror engine in the same escaped form.
func (h *Handler) registerRoutes() {
	next := h.next
	if next == nil {
		next = http.NotFoundHandler()
	}

	h.root.UseEncodedPath()
	// Path cleaning belongs to the CAS router this handler is composed
	// behind; cleaning again here would answer redirects for encoded paths.
	h.root.SkipClean(true)
	h.root.NotFoundHandler = next
	h.root.MethodNotAllowedHandler = next

	h.root.HandleFunc(tokenEndpointPath, h.handleToken).Methods(http.MethodGet)
	// The hub token refresh route clients construct themselves
	// ({endpoint}/api/{type}s/{repo}/xet-read-token/{revision}) when the
	// resolve response carries no xet-auth Link header: clients that skipped
	// the resolve HEAD (hub tree caches) refresh their CAS credential here;
	// answer locally so they stay on the mirror instead of the upstream CAS.
	h.root.HandleFunc("/api/{type:models|datasets|spaces}/{repo:.+?}/xet-read-token/{rev}", h.handleToken).Methods(http.MethodGet)
	// Hub tree listing API paths, with or without a subpath. Their entries
	// carry per-file xet hashes that would steer downstream clients straight
	// to the upstream CAS.
	h.root.HandleFunc("/api/{type:models|datasets|spaces}/{repo:.+?}/tree/{rev}", h.handleTree).Methods(http.MethodGet)
	h.root.HandleFunc("/api/{type:models|datasets|spaces}/{repo:.+?}/tree/{rev}/{path:.*}", h.handleTree).Methods(http.MethodGet)
	// Hub-style download paths. The prefix before /resolve/ is treated as an
	// opaque repo identity, so no platform-specific routing exists.
	h.root.HandleFunc("/{repo:.+?}/resolve/{rev}/{path:.+}", h.handleResolve).Methods(http.MethodGet, http.MethodHead)
}

// handleResolve serves GET/HEAD for hub-style download paths through the
// mirror engine: ready files answer metadata plus links, in-flight ingests
// stream from the growing spool.
func (h *Handler) handleResolve(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	repo, rev, path := vars["repo"], vars["rev"], vars["path"]
	// Bounded retry: a resolution can hand back a stream whose ingest
	// finished and whose spool was drained before this request attached;
	// resolving again returns the published entry.
	for range 2 {
		res, err := h.mirror.Resolve(r.Context(), repo, rev, path)
		if err != nil {
			serveFetchError(w, errors.Is(err, mirror.ErrUpstreamNotFound), err)
			return
		}
		if res.Entry != nil {
			h.serveReady(w, r, res.Entry)
			return
		}
		if h.serveFromStream(w, r, res.Stream) {
			return
		}
	}
	http.Error(w, "File not found", http.StatusNotFound)
}

// serveReady answers a resolve request for a fully cached file: metadata plus
// xet Link headers for capable clients, and a redirect to the sha256 bridge
// for everyone else.
func (h *Handler) serveReady(w http.ResponseWriter, r *http.Request, e *mirror.Entry) {
	base := h.hubExternalBase(r)
	writeMetadataHeaders(w, e.ETag, e.Size, e.Commit)
	if e.FileHash != "" {
		w.Header().Add("Link", fmt.Sprintf("<%s%s>; rel=\"xet-auth\", <%s/v1/reconstructions/%s>; rel=\"xet-reconstruction-info\"", base, tokenEndpointPath, base, e.FileHash))
		w.Header().Set("X-Xet-Hash", e.FileHash)
	}
	if e.Size == 0 {
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
		return
	}
	// The Location must be absolute: hub clients follow relative redirects
	// before reading metadata, which would strip the xet headers off the
	// response they end up looking at.
	http.Redirect(w, r, base+"/xet-bridge/"+e.SHA256, http.StatusFound)
}

// serveFetchError maps a failed ingest to its HTTP response: 404 when the
// upstream lacks the file, 502 with the underlying error otherwise.
func serveFetchError(w http.ResponseWriter, notFound bool, err error) {
	if notFound {
		http.Error(w, "File not found upstream", http.StatusNotFound)
		return
	}
	msg := "upstream fetch failed"
	if err != nil {
		msg = err.Error()
	}
	http.Error(w, msg, http.StatusBadGateway)
}

// serveFromStream streams a file that is still being ingested. Bytes are
// served from the growing spool; range requests for regions not yet spooled
// block until the data lands. It reports false when the ingest finished and
// the spool was already drained before this request could attach.
func (h *Handler) serveFromStream(w http.ResponseWriter, r *http.Request, st *mirror.Stream) bool {
	etag, commit, err := st.WaitMeta(r.Context())
	if err != nil {
		if r.Context().Err() != nil {
			return true
		}
		serveFetchError(w, errors.Is(err, mirror.ErrUpstreamNotFound), err)
		return true
	}

	// The size may only be learned from the ingest download's first response
	// headers (e.g. modelscope.cn sends none on HEAD); downstream hub clients
	// refuse metadata without it, so wait rather than answer size-less.
	size, ok := st.WaitSize(r.Context())
	if !ok {
		return true
	}
	if r.Method == http.MethodHead {
		writeMetadataHeaders(w, etag, size, commit)
		w.Header().Set("Content-Type", "application/octet-stream")
		if size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		}
		w.WriteHeader(http.StatusOK)
		return true
	}

	if size < 0 {
		// Total size unknown until the ingest completes (xet upstream whose
		// probe carried no size): ServeContent cannot handle that, so stream
		// the whole body as it lands, ignoring Range.
		rc := st.NewReader(r.Context(), 0)
		if rc == nil {
			return false
		}
		defer func() {
			_ = rc.Close()
		}()
		writeMetadataHeaders(w, etag, size, commit)
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = io.Copy(w, rc)
		return true
	}

	rs := st.NewSeekReader(r.Context(), size)
	if rs == nil {
		return false
	}
	defer func() {
		_ = rs.Close()
	}()
	writeMetadataHeaders(w, etag, size, commit)
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, "", time.Time{}, rs)
	return true
}

// writeMetadataHeaders emits the header set downstream tooling relies on,
// mirroring what the upstream hub advertised.
func writeMetadataHeaders(w http.ResponseWriter, etag string, size int64, commit string) {
	if etag != "" {
		quoted := `"` + etag + `"`
		w.Header().Set("ETag", quoted)
		w.Header().Set("X-Linked-Etag", quoted)
	}
	if size >= 0 {
		w.Header().Set("X-Linked-Size", strconv.FormatInt(size, 10))
	}
	if commit != "" {
		w.Header().Set("X-Repo-Commit", commit)
	}
	w.Header().Set("Accept-Ranges", "bytes")
}

// handleToken hands out a short-lived CAS token pointing downstream clients
// at the server itself. One mint serves a dual contract: the X-Xet-Cas-Url /
// X-Xet-Access-Token / X-Xet-Token-Expiration response headers carry it for
// huggingface_hub >= 1.29, which reads only these, while the JSON body keeps
// the hub token endpoint shape older clients read.
func (h *Handler) handleToken(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	var tok string
	exp := now.Add(15 * time.Minute).Unix()
	if h.mintToken != nil {
		tok, exp = h.mintToken(now)
	}
	casURL := h.hubExternalBase(r)
	w.Header().Set("X-Xet-Cas-Url", casURL)
	w.Header().Set("X-Xet-Access-Token", tok)
	w.Header().Set("X-Xet-Token-Expiration", strconv.FormatInt(exp, 10))
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"casUrl":      casURL,
		"accessToken": tok,
		"exp":         exp,
	})
}

// hubExternalBase returns the externally visible base URL used in hub
// responses: the configured external URL when set, otherwise derived from
// the request.
func (h *Handler) hubExternalBase(r *http.Request) string {
	if h.external != "" {
		return h.external
	}
	return externalBase(r)
}

// externalBase returns the server's externally visible base URL derived
// from the request, honoring X-Forwarded-Proto when behind a proxy.
func externalBase(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		scheme = proto
	}
	return scheme + "://" + r.Host
}

// treeLFS is the lfs pointer block of a hub tree entry; OID is the sha256
// naming the file bytes. The entry itself stays a raw map because expand
// requests add fields (lastCommit, securityFileStatus, ...) that must pass
// through untouched.
type treeLFS struct {
	OID         string `json:"oid"`
	PointerSize int64  `json:"pointerSize"`
	Size        int64  `json:"size"`
}

// handleTree proxies a tree listing API request, rewriting each entry's
// xetHash: entries whose lfs sha256 oid resolves in local storage advertise
// the mirror's own hash so xet clients reconstruct them from the mirror CAS;
// all other entries lose the hash so clients fall back to the resolve flow,
// which ingests through the mirror. Left untouched, upstream hashes would
// send clients directly to the upstream CAS, bypassing the mirror entirely.
// Matching by content makes revision and repo irrelevant. The array is
// rewritten in a streaming pass, so a listing is never buffered whole; no
// upstream header is forwarded, only the body is relayed.
func (h *Handler) handleTree(w http.ResponseWriter, r *http.Request) {
	pathAndQuery := r.URL.EscapedPath()
	if r.URL.RawQuery != "" {
		pathAndQuery += "?" + r.URL.RawQuery
	}
	resp, err := h.mirror.FetchUpstream(r.Context(), pathAndQuery)
	if err != nil {
		http.Error(w, "upstream tree fetch failed", http.StatusBadGateway)
		return
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body := bufio.NewReader(resp.Body)
	if resp.StatusCode != http.StatusOK || !startsJSONArray(body) {
		// Error status or unexpected shape: relay the bytes untouched.
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, body)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	dec := json.NewDecoder(body)
	if _, err := dec.Token(); err != nil {
		return
	}
	_, _ = io.WriteString(w, "[")
	for first := true; dec.More(); first = false {
		// Raw values keep every untouched field byte-exact through the re-encode.
		var item map[string]json.RawMessage
		if err := dec.Decode(&item); err != nil {
			// Mid-stream failure: stop without the closing bracket so the
			// client sees invalid JSON rather than a truncated listing.
			return
		}
		if _, ok := item["xetHash"]; ok {
			var lfs treeLFS
			if raw, ok := item["lfs"]; ok {
				_ = json.Unmarshal(raw, &lfs)
			}
			if hash, ok := h.mirror.LookupXetHash(r.Context(), lfs.OID); ok {
				quoted, _ := json.Marshal(hash)
				item["xetHash"] = quoted
			} else {
				delete(item, "xetHash")
			}
		}
		out, err := json.Marshal(item)
		if err != nil {
			return
		}
		if !first {
			_, _ = io.WriteString(w, ",")
		}
		if _, err := w.Write(out); err != nil {
			return
		}
	}
	_, _ = io.WriteString(w, "]")
}

// startsJSONArray peeks past leading whitespace and reports whether the next
// byte opens a JSON array, consuming nothing.
func startsJSONArray(br *bufio.Reader) bool {
	for i := 1; ; i++ {
		buf, _ := br.Peek(i)
		if len(buf) < i {
			return false
		}
		switch buf[i-1] {
		case ' ', '\t', '\r', '\n':
		default:
			return buf[i-1] == '['
		}
	}
}
