package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wzshiming/xet/storage"
)

func TestCompactEndpoint(t *testing.T) {
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	putGCTestFile(t, stor, []byte("compact endpoint test data"))
	handler := newTestHandler(stor)

	do := func(target string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, adminRequest(http.MethodPost, target))
		return rec
	}

	// grace=0s disables the grace window; the single dense xorb stays put.
	rec := do("/internal/compact?grace=0s&dry_run=true")
	if rec.Code != http.StatusOK {
		t.Fatalf("compact status = %d", rec.Code)
	}
	var report storage.CompactReport
	if err := json.NewDecoder(rec.Body).Decode(&report); err != nil {
		t.Fatal(err)
	}
	if !report.DryRun || report.ScannedXorbs != 1 || report.SparseXorbs != 0 {
		t.Fatalf("compact report = %+v", report)
	}

	if rec := do("/internal/compact?min_utilization=2"); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid min_utilization = %d, want 400", rec.Code)
	}

	handler.compactActive.Store(true)
	if rec := do("/internal/compact"); rec.Code != http.StatusConflict {
		t.Fatalf("concurrent compaction = %d, want 409", rec.Code)
	}
}
