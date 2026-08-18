package admin

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/wzshiming/xet/storage"
)

type deleteFileResponse struct {
	*storage.UnlinkResult
	HookError string `json:"hook_error,omitempty"`
}

// deleteFileHandler serves DELETE /internal/files/sha256/{hash} and
// /internal/files/xet/{hash}; the path names the hash kind since both are 64
// hex characters. It unlinks the file's index entries; space is reclaimed by
// a later sweep.
func (h *Handler) deleteFileHandler(kind storage.HashKind) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collector, ok := h.authorize(w, r)
		if !ok {
			return
		}

		res, err := storage.Unlink(r.Context(), collector, mux.Vars(r)["hash"], kind)
		if err != nil {
			if errors.Is(err, storage.ErrFileNotFound) {
				http.Error(w, "File not found", http.StatusNotFound)
				return
			}
			http.Error(w, "Failed to delete file: "+err.Error(), http.StatusInternalServerError)
			return
		}

		resp := deleteFileResponse{UnlinkResult: res}
		if h.fileRemovedHook != nil {
			if err := h.fileRemovedHook(r.Context(), res.SHA256, res.FileHash); err != nil {
				resp.HookError = err.Error()
			}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// handleSweep handles POST /internal/gc/sweep?dry_run=&grace=. Only one GC
// operation (sweep or compaction) runs at a time; concurrent requests get
// 409. grace=0 disables the grace window; see SweepOptions.Grace for why
// that is unsafe during uploads.
func (h *Handler) handleSweep(w http.ResponseWriter, r *http.Request) {
	collector, ok := h.authorize(w, r)
	if !ok {
		return
	}

	var opts storage.SweepOptions
	query := r.URL.Query()
	if v := query.Get("dry_run"); v != "" {
		dryRun, err := strconv.ParseBool(v)
		if err != nil {
			http.Error(w, "Invalid dry_run", http.StatusBadRequest)
			return
		}
		opts.DryRun = dryRun
	}
	if v := query.Get("grace"); v != "" {
		grace, err := time.ParseDuration(v)
		if err != nil {
			http.Error(w, "Invalid grace", http.StatusBadRequest)
			return
		}
		if grace <= 0 {
			grace = -1 // explicit zero disables the grace window
		}
		opts.Grace = grace
	}

	if !h.gcActive.CompareAndSwap(false, true) {
		http.Error(w, "Another GC operation is in progress", http.StatusConflict)
		return
	}
	defer h.gcActive.Store(false)

	report, err := storage.Sweep(r.Context(), collector, opts)
	if err != nil {
		http.Error(w, "Sweep failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(report)
}
