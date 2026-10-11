// Package internalapi serves non-CAS management endpoints with Read or Write permission.
package internalapi

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/mirror/spool"
	"github.com/wzshiming/xet/storage"
)

// Handler serves the internal management endpoints.
type Handler struct {
	authorizer auth.Authorizer
	storage    storage.Storage
	gc         *storage.GC
	gcGrace    time.Duration
	gcAnchor   storage.SweepAnchor
	mirror     *mirror.Mirror
	spool      *spool.Spool
	root       *mux.Router
	next       http.Handler
}

// Option defines a functional option for configuring the Handler.
type Option func(*Handler)

// WithAuthorizer sets the route authorizer; unset allows all requests.
func WithAuthorizer(a auth.Authorizer) Option {
	return func(h *Handler) {
		h.authorizer = a
	}
}

// WithStorage sets the storage backend for the internal endpoints.
func WithStorage(storage storage.Storage) Option {
	return func(h *Handler) {
		h.storage = storage
	}
}

// WithGCGrace sets the sweep grace used when a request omits the grace
// parameter, following the storage.SweepOptions.Grace conventions: zero
// means the default window, negative disables it.
func WithGCGrace(grace time.Duration) Option {
	return func(h *Handler) {
		h.gcGrace = grace
	}
}

// WithGCAnchor sets the sweep anchor used when a request omits the anchor
// parameter; the zero value is storage.AnchorBoth.
func WithGCAnchor(anchor storage.SweepAnchor) Option {
	return func(h *Handler) {
		h.gcAnchor = anchor
	}
}

// WithMirror adds the mirror's dead index entries to the GC sweep; nil sweeps none.
func WithMirror(m *mirror.Mirror) Option {
	return func(h *Handler) {
		h.mirror = m
	}
}

// WithSpool adds the spool's idle files to the GC sweep; nil sweeps none.
func WithSpool(q *spool.Spool) Option {
	return func(h *Handler) {
		h.spool = q
	}
}

// WithNext sets the next http.Handler to call if a request does not match any internal route.
func WithNext(next http.Handler) Option {
	return func(h *Handler) {
		h.next = next
	}
}

// NewHandler creates a handler for the internal management endpoints.
func NewHandler(opts ...Option) *Handler {
	h := &Handler{
		root: mux.NewRouter(),
	}

	for _, opt := range opts {
		opt(h)
	}

	h.gc = storage.NewGC(h.storage)

	h.registerRoutes()
	return h
}

// registerRoutes sets up all internal HTTP routes.
func (h *Handler) registerRoutes() {
	h.root.HandleFunc("/internal/files", h.handleListFiles).Methods(http.MethodGet)
	h.root.HandleFunc("/internal/files/xet/{hash}", h.handleUnlinkFile).Methods(http.MethodDelete)
	h.root.HandleFunc("/internal/files/sha256/{hash}", h.handleUnlinkSHA256).Methods(http.MethodDelete)
	h.root.HandleFunc("/internal/gc", h.handleGC).Methods(http.MethodPost)

	h.root.NotFoundHandler = h.next
}

// ServeHTTP implements http.Handler
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.root.ServeHTTP(w, r)
}

func (h *Handler) authorize(w http.ResponseWriter, r *http.Request, g auth.Grant) bool {
	if h.authorizer == nil {
		return true
	}
	if err := h.authorizer.Authorize(r, g); err != nil {
		auth.Deny(w, err)
		return false
	}
	return true
}

// handleListFiles handles GET /internal/files: all stored files grouped by
// content SHA-256, each carrying its xet file hashes and original size.
func (h *Handler) handleListFiles(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r, auth.Grant{Permission: auth.Read}) {
		return
	}
	entries, err := storage.ListFiles(r.Context(), h.storage)
	if err != nil {
		http.Error(w, "Failed to list files: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(entries)
}

// handleUnlinkFile handles DELETE /internal/files/xet/{hash}: it drops the
// file-index entry only; the shard and its data are collected by the next
// sweep once, per its anchor, nothing references them. Under the default
// "both" anchor a non-empty file's sha256 entry still anchors the shard,
// so full removal also takes DELETE /internal/files/sha256/{hash}, while a
// "files" sweep reclaims after this unlink alone; empty files need this
// unlink alone under every anchor.
func (h *Handler) handleUnlinkFile(w http.ResponseWriter, r *http.Request) {
	fileHash, err := xet.ParseFileHash(mux.Vars(r)["hash"])
	if err != nil {
		http.Error(w, "Invalid file hash", http.StatusBadRequest)
		return
	}
	if !h.authorize(w, r, auth.Grant{Permission: auth.Write, File: &fileHash}) {
		return
	}
	removed, err := h.gc.Unlink(r.Context(), fileHash)
	if err != nil {
		http.Error(w, "Failed to unlink file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if !removed {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"file_hash": fileHash.String(),
		"removed":   true,
	})
}

// handleUnlinkSHA256 handles DELETE /internal/files/sha256/{hash}: it drops
// the index/sha256 entry only: SHA-256 lookups stop resolving at once while
// the content stays reachable by file hash. Space is reclaimed by a later
// sweep once, per its anchor, nothing references the shard — a "sha256"
// sweep reclaims after this unlink alone.
func (h *Handler) handleUnlinkSHA256(w http.ResponseWriter, r *http.Request) {
	raw, err := hex.DecodeString(mux.Vars(r)["hash"])
	if err != nil || len(raw) != 32 {
		http.Error(w, "Invalid SHA-256 digest", http.StatusBadRequest)
		return
	}
	digest := [32]byte(raw)
	if digest == [32]byte{} {
		// Mirrors the storage rule: the all-zero digest is the shared
		// empty-file marker, never a deletable entry.
		http.Error(w, "Invalid SHA-256 digest: all-zero empty-file marker", http.StatusBadRequest)
		return
	}
	if !h.authorize(w, r, auth.Grant{Permission: auth.Write, SHA256: hex.EncodeToString(digest[:])}) {
		return
	}
	removed, err := h.gc.UnlinkSHA256(r.Context(), digest)
	if err != nil {
		http.Error(w, "Failed to unlink SHA-256: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if !removed {
		http.Error(w, "SHA-256 not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"sha256":  hex.EncodeToString(digest[:]),
		"removed": true,
	})
}

// handleGC handles POST /internal/gc?dry_run=&grace=&max=&budget=&anchor=:
// it removes (or, with dry_run, reports) shards and xorbs nothing keeps
// alive under the chosen anchor: "both" (default; any file or non-zero
// sha256 entry anchors — reclaiming takes the file and sha256 unlinks),
// "files" (file entries only; the file unlink alone reclaims), or "sha256"
// (non-zero sha256 entries only; the sha256 unlink alone reclaims — for
// stores managed exclusively by SHA-256, such as LFS backends). An omitted
// anchor uses the server-configured default. An omitted grace uses the
// server-configured window (the default when none was configured); an
// explicit zero disables it; negative values are rejected. The response
// reports the storage pass under "storage" and, with a spool and a mirror,
// the idle-spool pass the same grace and dry_run drive under "spools" and
// the mirror's index pass under "mirror". Every request runs independent
// bounded passes that re-mark from scratch: max bounds the storage pass
// only, while budget is one wall-clock budget for the whole request, spent
// storage → spool → mirror, each later pass getting what is left and being
// skipped once the request has swept anything and nothing is left (a
// request that swept nothing still hands the next pass a token budget, so
// progress never stalls). The spool and mirror passes run only once the
// storage pass has ended, done or failed, and each later pass only as the
// budget allows, so stepped responses may carry "storage" alone, or
// "storage" and "spools" without "mirror"; repeat until the top-level done
// is true, and without max or budget one request already sweeps everything.
// A failed pass appends "<name>: <message>" to "errors" and leaves done
// false but does not stop the passes after it; the status stays 200 — only
// a busy GC (409) or a bad parameter (400) fails the request. done and
// remaining_* describe one request only. dry_run reports every pass's full
// upper bound (for storage the mark-time bound: no per-shard re-checks, no
// entry counts), ignoring max and budget.
func (h *Handler) handleGC(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r, auth.Grant{Permission: auth.Write}) {
		return
	}

	var dryRun bool
	grace := h.gcGrace
	var err error
	if v := r.URL.Query().Get("dry_run"); v != "" {
		dryRun, err = strconv.ParseBool(v)
		if err != nil {
			http.Error(w, "Invalid dry_run value", http.StatusBadRequest)
			return
		}
	}
	if v := r.URL.Query().Get("grace"); v != "" {
		grace, err = time.ParseDuration(v)
		if err != nil || grace < 0 {
			http.Error(w, "Invalid grace value", http.StatusBadRequest)
			return
		}
		if grace == 0 {
			grace = -1 // explicit zero disables the window; zero means default in Sweep
		}
	}
	var maxDeletes int
	if v := r.URL.Query().Get("max"); v != "" {
		maxDeletes, err = strconv.Atoi(v)
		if err != nil || maxDeletes < 0 {
			http.Error(w, "Invalid max value", http.StatusBadRequest)
			return
		}
	}
	var budget time.Duration
	if v := r.URL.Query().Get("budget"); v != "" {
		budget, err = time.ParseDuration(v)
		if err != nil || budget < 0 {
			http.Error(w, "Invalid budget value", http.StatusBadRequest)
			return
		}
	}
	anchor := h.gcAnchor
	if v := r.URL.Query().Get("anchor"); v != "" {
		switch v {
		case "both":
			anchor = storage.AnchorBoth
		case "files":
			anchor = storage.AnchorFiles
		case "sha256":
			anchor = storage.AnchorSHA256
		default:
			http.Error(w, "Invalid anchor value", http.StatusBadRequest)
			return
		}
	}
	var deadline time.Time
	if budget > 0 && !dryRun {
		deadline = time.Now().Add(budget)
	}
	opts := storage.SweepOptions{
		Grace:      grace,
		DryRun:     dryRun,
		MaxDeletes: maxDeletes,
		Budget:     budget,
		Anchor:     anchor,
	}
	result, err := h.gc.SweepStep(r.Context(), opts)
	if errors.Is(err, storage.ErrGCBusy) {
		http.Error(w, "GC already running", http.StatusConflict)
		return
	}
	var resp sweepResponse
	swept, done := 0, true
	if err != nil {
		resp.Errors = append(resp.Errors, "storage: "+err.Error())
		done = false
	} else {
		resp.Storage = result
		swept = len(result.SweptShards) + len(result.SweptXorbs)
		done = result.Done
	}
	// The later passes wait for the storage pass to finish, not for one that failed.
	ended := err != nil || result.Done
	// handDown returns the budget left for the next pass; false defers it
	// because this request already swept something and spent its budget.
	handDown := func() (time.Duration, bool) {
		if deadline.IsZero() {
			return 0, true
		}
		if left := time.Until(deadline); left > 0 {
			return left, true
		}
		if swept > 0 {
			return 0, false
		}
		return time.Nanosecond, true
	}
	if ended && h.spool != nil {
		if b, ok := handDown(); !ok {
			done = false
		} else if spools, err := h.spool.Sweep(r.Context(), spool.SweepOptions{Grace: grace, DryRun: dryRun, Budget: b}); err != nil {
			resp.Errors = append(resp.Errors, "spools: "+err.Error())
			done = false
		} else {
			resp.Spools = &spools
			swept += spools.SweptSpools
			done = done && spools.Done
		}
	}
	if ended && h.mirror != nil {
		if b, ok := handDown(); !ok {
			done = false
		} else if index, err := h.mirror.Sweep(r.Context(), mirror.SweepOptions{Grace: grace, DryRun: dryRun, Budget: b}); err != nil {
			resp.Errors = append(resp.Errors, "mirror: "+err.Error())
			done = false
		} else {
			resp.Mirror = &index
			done = done && index.Done
		}
	}
	resp.Done = done
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// sweepResponse is the GC sweep report: the storage pass and, with a spool and a mirror, the spool and index passes;
// Errors lists the failed passes as "<name>: <message>" in pass order;
// Done once every configured pass finished in this request.
type sweepResponse struct {
	Done    bool                 `json:"done"`
	Storage *storage.SweepResult `json:"storage,omitempty"`
	Spools  *spool.SweepResult   `json:"spools,omitempty"`
	Mirror  *mirror.SweepResult  `json:"mirror,omitempty"`
	Errors  []string             `json:"errors,omitempty"`
}
