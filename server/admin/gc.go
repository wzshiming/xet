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

// deleteFileHandler serves DELETE /internal/files/sha256/{hash} and
// /internal/files/xet/{hash}; the path names the hash kind since both are 64
// hex characters. It unlinks the file's index entries; space is reclaimed by
// a later sweep. A hook failure is reported in the response's hook_error.
func (h *Handler) deleteFileHandler(kind storage.HashKind) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		gc, ok := h.authorize(w, r)
		if !ok {
			return
		}

		res, err := gc.Unlink(r.Context(), mux.Vars(r)["hash"], kind)
		if err != nil {
			if errors.Is(err, storage.ErrFileNotFound) {
				http.Error(w, "File not found", http.StatusNotFound)
				return
			}
			http.Error(w, "Failed to delete file: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(res)
	}
}

// handleSweep handles POST /internal/gc/sweep?dry_run=&grace=. Only one GC
// operation (sweep or compaction) runs at a time; concurrent requests get
// 409. grace=0 disables the grace window; see SweepOptions.Grace for why
// that is unsafe during uploads.
func (h *Handler) handleSweep(w http.ResponseWriter, r *http.Request) {
	gc, ok := h.authorize(w, r)
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

	report, err := gc.Sweep(r.Context(), opts)
	if err != nil {
		if errors.Is(err, storage.ErrGCBusy) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, "Sweep failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(report)
}
