package admin

import (
	"encoding/json"
	"net/http"
)

// handleListFiles handles GET /internal/files. It lists every file reachable
// through the file index with its SHA-256 mapping and unpacked size.
func (h *Handler) handleListFiles(w http.ResponseWriter, r *http.Request) {
	gc, ok := h.authorize(w, r)
	if !ok {
		return
	}

	list, err := gc.ListFiles(r.Context())
	if err != nil {
		http.Error(w, "Failed to list files: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(list)
}
