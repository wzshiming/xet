package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wzshiming/xet/storage"
)

func TestListFilesEndpoint(t *testing.T) {
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("list endpoint test data")
	sha256Hex := putGCTestFile(t, stor, data)
	handler := newTestHandler(stor)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, adminRequest(http.MethodGet, "/internal/files"))
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d", rec.Code)
	}
	var list []storage.FileListEntry
	if err := json.NewDecoder(rec.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("list = %+v", list)
	}
	f := list[0]
	if f.SHA256 != sha256Hex || f.Size != int64(len(data)) || f.Missing || len(f.FileHashes) != 1 || f.FileHashes[0] == "" {
		t.Fatalf("entry = %+v", f)
	}

	// The unlink routes resolve the same file both ways the listing reports.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, adminRequest(http.MethodDelete, "/internal/files/xet/"+f.FileHashes[0]))
	if rec.Code != http.StatusOK {
		t.Fatalf("delete by listed file hash = %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, adminRequest(http.MethodGet, "/internal/files"))
	if rec.Code != http.StatusOK {
		t.Fatalf("list after delete status = %d", rec.Code)
	}
	list = nil
	if err := json.NewDecoder(rec.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("list after delete = %+v", list)
	}
}
