package common

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage/local"
)

// newTestServer serves a real CAS handler over local storage and records
// every request as "path|range-header".
func newTestServer(t *testing.T) (client.AuthProvider, func() string) {
	t.Helper()
	stor, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := server.NewHandler(server.WithStorage(stor))
	var mut sync.Mutex
	var requests []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mut.Lock()
		requests = append(requests, r.URL.Path+"|"+r.Header.Get("Range"))
		mut.Unlock()
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return client.StaticAuthProvider(srv.URL, ""), func() string {
		mut.Lock()
		defer mut.Unlock()
		return strings.Join(requests, "\n")
	}
}

func TestExecuteUploadMissingInputFile(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "missing")
	var out bytes.Buffer
	err := ExecuteUpload(context.Background(), filename, nil, "default", 1, "", &out)
	if !errors.Is(err, fs.ErrNotExist) || !strings.HasPrefix(err.Error(), "upload failed: open input file: ") {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := out.String(), filename+" Uploading file\n"; got != want {
		t.Fatalf("output %q, want %q", got, want)
	}
}

func TestExecuteDownloadOutputFileErrors(t *testing.T) {
	for _, tc := range []struct {
		resume bool
		prefix string
	}{
		{resume: false, prefix: "create output file: "},
		{resume: true, prefix: "open output file: "},
	} {
		t.Run(tc.prefix, func(t *testing.T) {
			output := filepath.Join(t.TempDir(), "missing-dir", "out")
			var out bytes.Buffer
			err := ExecuteDownload(context.Background(), xet.FileHash{}, output, nil, "default", 1, "", tc.resume, &out)
			if !errors.Is(err, fs.ErrNotExist) || !strings.HasPrefix(err.Error(), tc.prefix) {
				t.Fatalf("unexpected error: %v", err)
			}
			if out.Len() != 0 {
				t.Fatalf("unexpected output %q", out.String())
			}
		})
	}
}

func TestExecuteUploadThenDownload(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	provider, requests := newTestServer(t)
	dir := t.TempDir()
	data := bytes.Repeat([]byte("xet inline cas characterization\n"), 256)
	input := filepath.Join(dir, "input.bin")
	if err := os.WriteFile(input, data, 0o644); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := ExecuteUpload(context.Background(), input, provider, "ns-a", 2, t.TempDir(), &out); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	if !strings.HasPrefix(got, input+" Uploading file\n") || !strings.Contains(got, ")\r") {
		t.Fatalf("unexpected upload output %q", got)
	}
	_, hashLine, ok := strings.Cut(got, "Upload complete!\nFile hash: ")
	if !ok {
		t.Fatalf("unexpected upload output %q", got)
	}
	fileHash, err := xet.ParseFileHash(strings.TrimSuffix(hashLine, "\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(requests(), "/v1/xorbs/ns-a/") {
		t.Fatalf("namespace not propagated, requests:\n%s", requests())
	}

	for _, tc := range []struct {
		name     string
		resume   bool
		existing []byte
		ranged   bool
	}{
		{name: "truncate", resume: false, existing: []byte("stale content"), ranged: false},
		{name: "resume", resume: true, existing: data[:len(data)/2], ranged: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output := filepath.Join(dir, tc.name)
			if err := os.WriteFile(output, tc.existing, 0o644); err != nil {
				t.Fatal(err)
			}
			before := requests()
			var out bytes.Buffer
			if err := ExecuteDownload(context.Background(), fileHash, output, provider, "ns-a", 2, t.TempDir(), tc.resume, &out); err != nil {
				t.Fatal(err)
			}
			if want := fmt.Sprintf("Download complete! (%d bytes)\r", len(data)); !strings.HasSuffix(out.String(), want) {
				t.Fatalf("output %q, want suffix %q", out.String(), want)
			}
			content, err := os.ReadFile(output)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(content, data) {
				t.Fatalf("downloaded %d bytes, want %d", len(content), len(data))
			}
			since := strings.TrimPrefix(requests(), before)
			if ranged := strings.Contains(since, fileHash.String()+"|bytes="); ranged != tc.ranged {
				t.Fatalf("ranged reconstruction = %v, want %v; requests:\n%s", ranged, tc.ranged, since)
			}
		})
	}
}
