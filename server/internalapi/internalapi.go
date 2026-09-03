// Package internalapi serves internal management endpoints that are not
// part of the CAS protocol surface.
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
	"github.com/wzshiming/xet/storage"
)

// Handler serves the internal management endpoints.
type Handler struct {
	storage  storage.Storage
	gc       *storage.GC
	gcGrace  time.Duration
	gcAnchor storage.SweepAnchor
	root     *mux.Router
	next     http.Handler
}

// Option defines a functional option for configuring the Handler.
type Option func(*Handler)

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

	if gcs, ok := h.storage.(storage.GCStore); ok {
		h.gc = storage.NewGC(gcs)
	}

	h.registerRoutes()
	return h
}

// registerRoutes sets up all internal HTTP routes.
func (h *Handler) registerRoutes() {
	h.root.HandleFunc("/internal/files", h.handleListFiles).Methods(http.MethodGet)
	h.root.HandleFunc("/internal/files/xet/{hash}", h.handleUnlinkFile).Methods(http.MethodDelete)
	h.root.HandleFunc("/internal/files/sha256/{hash}", h.handleUnlinkSHA256).Methods(http.MethodDelete)
	h.root.HandleFunc("/internal/gc/sweep", h.handleGCSweep).Methods(http.MethodPost)

	h.root.NotFoundHandler = h.next
}

// ServeHTTP implements http.Handler
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.root.ServeHTTP(w, r)
}

// handleListFiles handles GET /internal/files: all stored files grouped by
// content SHA-256, each carrying its xet file hashes and original size.
func (h *Handler) handleListFiles(w http.ResponseWriter, r *http.Request) {
	ls, ok := h.storage.(storage.ListStore)
	if !ok {
		http.Error(w, "Storage does not support file listing", http.StatusNotImplemented)
		return
	}
	entries, err := storage.ListFiles(r.Context(), ls)
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
	if h.gc == nil {
		http.Error(w, "Storage does not support garbage collection", http.StatusNotImplemented)
		return
	}
	fileHash, err := xet.ParseFileHash(mux.Vars(r)["hash"])
	if err != nil {
		http.Error(w, "Invalid file hash", http.StatusBadRequest)
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
	if h.gc == nil {
		http.Error(w, "Storage does not support garbage collection", http.StatusNotImplemented)
		return
	}
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

// handleGCSweep handles POST /internal/gc/sweep?dry_run=&grace=&max=&budget=&anchor=:
// it removes (or, with dry_run, reports) shards and xorbs nothing keeps
// alive under the chosen anchor: "both" (default; any file or non-zero
// sha256 entry anchors — reclaiming takes the file and sha256 unlinks),
// "files" (file entries only; the file unlink alone reclaims), or "sha256"
// (non-zero sha256 entries only; the sha256 unlink alone reclaims — for
// stores managed exclusively by SHA-256, such as LFS backends). An omitted
// anchor uses the server-configured default. An omitted grace uses the
// server-configured window (the default when none was configured); an
// explicit zero disables it; negative values are rejected. Every request
// runs one independent bounded pass that re-marks from scratch — repeat
// until the response reports done=true; done and remaining_* describe that
// pass only, and without max or budget one pass already sweeps everything.
// dry_run reports a full stateless pass's mark-time upper bound (no
// per-shard re-checks, no entry counts), ignoring max and budget.
func (h *Handler) handleGCSweep(w http.ResponseWriter, r *http.Request) {
	if h.gc == nil {
		http.Error(w, "Storage does not support garbage collection", http.StatusNotImplemented)
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
	result, err := h.gc.SweepStep(r.Context(), storage.SweepOptions{
		Grace:      grace,
		DryRun:     dryRun,
		MaxDeletes: maxDeletes,
		Budget:     budget,
		Anchor:     anchor,
	})
	if err != nil {
		if errors.Is(err, storage.ErrGCBusy) {
			http.Error(w, "GC already running", http.StatusConflict)
			return
		}
		http.Error(w, "Sweep failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
