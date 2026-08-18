// Package admin serves the internal storage administration endpoints, such as
// garbage collection. They are not part of the xet protocol and live in their
// own handler so a deployment registers them only where destructive
// operations are allowed.
package admin

import (
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/wzshiming/xet/storage"
)

// AuthFunc validates an authentication token, returning true when it is valid.
type AuthFunc func(token string) bool

// Handler serves the storage administration endpoints as a thin HTTP adapter
// over a storage.GC coordinator.
type Handler struct {
	gc      *storage.GC
	storage storage.Storage
	root    *mux.Router
	next    http.Handler
	authFn  AuthFunc
}

// Option defines a functional option for configuring the Handler.
type Option func(*Handler)

// WithGC shares an existing coordinator with the endpoints, so its
// single-flight guard spans in-process callers and these HTTP requests.
// Without it, NewHandler builds a coordinator from the WithStorage backend.
func WithGC(g *storage.GC) Option {
	return func(h *Handler) {
		h.gc = g
	}
}

// WithStorage sets the storage backend to administer. It must implement
// storage.SweepStore for the endpoints to be available.
func WithStorage(st storage.Storage) Option {
	return func(h *Handler) {
		h.storage = st
	}
}

// WithAuthFunc sets the authentication function guarding the endpoints.
// Without one they stay disabled.
func WithAuthFunc(authFn AuthFunc) Option {
	return func(h *Handler) {
		h.authFn = authFn
	}
}

// WithNext sets the http.Handler to call for requests that match no admin route.
func WithNext(next http.Handler) Option {
	return func(h *Handler) {
		h.next = next
	}
}

// NewHandler creates a handler for the storage administration endpoints.
func NewHandler(opts ...Option) *Handler {
	h := &Handler{
		root: mux.NewRouter(),
	}

	for _, opt := range opts {
		opt(h)
	}

	if h.gc == nil {
		if sweeper, ok := h.storage.(storage.SweepStore); ok {
			h.gc = storage.NewGC(sweeper)
		}
	}

	h.root.HandleFunc("/internal/files", h.handleListFiles).Methods(http.MethodGet)
	// Deletion is by xet file hash only: one SHA-256 can map to several xet
	// hashes, so a SHA-256 delete could unlink the wrong file. The SHA-256
	// index entry falls with its shard during a sweep instead.
	h.root.HandleFunc("/internal/files/xet/{hash}", h.handleDeleteFile).Methods(http.MethodDelete)
	h.root.HandleFunc("/internal/gc/sweep", h.handleSweep).Methods(http.MethodPost)

	if h.next != nil {
		h.root.NotFoundHandler = h.next
	}
	return h
}

// ServeHTTP implements http.Handler
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.root.ServeHTTP(w, r)
}

// authorize gates the destructive endpoints: they stay disabled until an
// AuthFunc is configured, and need a Collector-capable backend.
func (h *Handler) authorize(w http.ResponseWriter, r *http.Request) (*storage.GC, bool) {
	if h.authFn == nil {
		http.Error(w, "Admin endpoints are disabled without authentication", http.StatusForbidden)
		return nil, false
	}
	if !h.authenticate(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return nil, false
	}
	if h.gc == nil {
		http.Error(w, "storage does not support GC", http.StatusNotImplemented)
		return nil, false
	}
	return h.gc, true
}

// authenticate validates the request's bearer token.
func (h *Handler) authenticate(r *http.Request) bool {
	parts := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
	if len(parts) != 2 || parts[0] != "Bearer" {
		return false
	}
	return h.authFn(parts[1])
}
