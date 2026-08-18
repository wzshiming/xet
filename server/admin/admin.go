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

// FileRemovedHook is called after a file's index entries are removed via the
// delete endpoint, e.g. to drop matching mirror index entries.
type FileRemovedHook = storage.FileRemovedHook

// Handler serves the storage administration endpoints as a thin HTTP adapter
// over a storage.GC coordinator.
type Handler struct {
	storage storage.Storage
	gc      *storage.GC
	root    *mux.Router
	next    http.Handler
	authFn  AuthFunc

	fileRemovedHook storage.FileRemovedHook
}

// Option defines a functional option for configuring the Handler.
type Option func(*Handler)

// WithStorage sets the storage backend to administer. It must implement
// storage.Collector for the GC endpoints to be available.
func WithStorage(st storage.Storage) Option {
	return func(h *Handler) {
		h.storage = st
	}
}

// WithGC shares an existing coordinator with the endpoints, so its
// single-flight guard spans in-process callers and these HTTP requests.
// Without it, NewHandler builds a coordinator from the WithStorage backend.
func WithGC(g *storage.GC) Option {
	return func(h *Handler) {
		h.gc = g
	}
}

// WithAuthFunc sets the authentication function guarding the endpoints.
// Without one they stay disabled.
func WithAuthFunc(authFn AuthFunc) Option {
	return func(h *Handler) {
		h.authFn = authFn
	}
}

// WithFileRemovedHook sets a callback invoked after a successful file delete.
// It applies to the handler-built coordinator; one injected with WithGC
// carries its own hook.
func WithFileRemovedHook(fn FileRemovedHook) Option {
	return func(h *Handler) {
		h.fileRemovedHook = fn
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
		if collector, ok := h.storage.(storage.Collector); ok {
			h.gc = storage.NewGC(collector, storage.WithFileRemovedHook(h.fileRemovedHook))
		}
	}

	h.root.HandleFunc("/internal/files", h.handleListFiles).Methods(http.MethodGet)
	h.root.HandleFunc("/internal/files/sha256/{hash}", h.deleteFileHandler(storage.HashKindSHA256)).Methods(http.MethodDelete)
	h.root.HandleFunc("/internal/files/xet/{hash}", h.deleteFileHandler(storage.HashKindFile)).Methods(http.MethodDelete)
	h.root.HandleFunc("/internal/gc/sweep", h.handleSweep).Methods(http.MethodPost)
	h.root.HandleFunc("/internal/compact", h.handleCompact).Methods(http.MethodPost)

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
