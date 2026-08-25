// Package internalapi serves internal management endpoints that are not
// part of the CAS protocol surface.
package internalapi

import (
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
	storage storage.Storage
	gc      *storage.GC
	root    *mux.Router
	next    http.Handler
}

// Option defines a functional option for configuring the Handler.
type Option func(*Handler)

// WithStorage sets the storage backend for the internal endpoints.
func WithStorage(storage storage.Storage) Option {
	return func(h *Handler) {
		h.storage = storage
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
	h.root.HandleFunc("/internal/gc/sweep", h.handleGCSweep).Methods(http.MethodPost)
	h.root.HandleFunc("/internal/gc/compact", h.handleGCCompact).Methods(http.MethodPost)

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
// sweep once nothing references them.
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

// handleGCSweep handles POST /internal/gc/sweep?dry_run=&grace=: it removes
// (or, with dry_run, reports) shards and xorbs no file-index entry keeps
// alive. An omitted grace uses the default window; an explicit zero disables
// it; negative values are rejected.
func (h *Handler) handleGCSweep(w http.ResponseWriter, r *http.Request) {
	if h.gc == nil {
		http.Error(w, "Storage does not support garbage collection", http.StatusNotImplemented)
		return
	}

	var dryRun bool
	var grace time.Duration
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
	result, err := h.gc.Sweep(r.Context(), grace, dryRun)
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

// handleGCCompact handles POST /internal/gc/compact?dry_run=: it repacks (or, with dry_run, plans) all live chunks into dense xorbs; superseded objects are left for the next sweep.
func (h *Handler) handleGCCompact(w http.ResponseWriter, r *http.Request) {
	if h.gc == nil {
		http.Error(w, "Storage does not support garbage collection", http.StatusNotImplemented)
		return
	}

	var dryRun bool
	var err error
	if v := r.URL.Query().Get("dry_run"); v != "" {
		dryRun, err = strconv.ParseBool(v)
		if err != nil {
			http.Error(w, "Invalid dry_run value", http.StatusBadRequest)
			return
		}
	}
	result, err := h.gc.Compact(r.Context(), dryRun)
	if err != nil {
		if errors.Is(err, storage.ErrGCBusy) {
			http.Error(w, "GC already running", http.StatusConflict)
			return
		}
		http.Error(w, "Compact failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
