package mirror

import (
	"encoding/json"
	"net/http"
	"time"
)

// handleToken hands out a short-lived CAS token in the same JSON shape as the
// hub token endpoint, pointing casUrl at the mirror itself.
func (h *Handler) handleToken(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	var tok string
	exp := now.Add(15 * time.Minute).Unix()
	if h.mintToken != nil {
		tok, exp = h.mintToken(now)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"casUrl":      h.externalBase(r),
		"accessToken": tok,
		"exp":         exp,
	})
}

// externalBase returns the mirror's externally visible base URL.
func (h *Handler) externalBase(r *http.Request) string {
	if h.external != "" {
		return h.external
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		scheme = proto
	}
	return scheme + "://" + r.Host
}
