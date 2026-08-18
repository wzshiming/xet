package admin

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/wzshiming/xet/storage"
)

// handleCompact handles POST /internal/compact?dry_run=&grace=&min_utilization=.
// Only one GC operation (sweep or compaction) runs at a time; concurrent
// requests get 409. Repacking changes xorb and shard hashes but not file
// hashes, so clients keep resolving the same file ids.
func (h *Handler) handleCompact(w http.ResponseWriter, r *http.Request) {
	collector, ok := h.authorize(w, r)
	if !ok {
		return
	}

	var opts storage.CompactOptions
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
	if v := query.Get("min_utilization"); v != "" {
		ratio, err := strconv.ParseFloat(v, 64)
		if err != nil || ratio <= 0 || ratio > 1 {
			http.Error(w, "Invalid min_utilization", http.StatusBadRequest)
			return
		}
		opts.MinUtilization = ratio
	}
	if v := query.Get("max_xorbs"); v != "" {
		maxXorbs, err := strconv.Atoi(v)
		if err != nil || maxXorbs < 0 {
			http.Error(w, "Invalid max_xorbs", http.StatusBadRequest)
			return
		}
		opts.MaxXorbs = maxXorbs
	}

	if !h.gcActive.CompareAndSwap(false, true) {
		http.Error(w, "Another GC operation is in progress", http.StatusConflict)
		return
	}
	defer h.gcActive.Store(false)

	report, err := storage.Compact(r.Context(), collector, opts)
	if err != nil {
		http.Error(w, "Compaction failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(report)
}
