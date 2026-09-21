package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage/local"
)

// TestClientDownloadResume seeds the destination with a prefix that ends inside
// a later term and verifies DownloadFile continues from there with a single
// ranged reconstruction query, fetching only the missing xorb bytes.
func TestClientDownloadResume(t *testing.T) {
	stor, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := server.NewHandler(server.WithStorage(stor))

	var (
		mu              sync.Mutex
		v2Available     = true
		reconstructions []string
		xorbGets        []string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		serveV2 := v2Available
		switch {
		case strings.HasPrefix(r.URL.Path, "/v1/reconstructions/"), strings.HasPrefix(r.URL.Path, "/v2/reconstructions/"):
			reconstructions = append(reconstructions, r.URL.Path[:3]+" "+r.Header.Get("Range"))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/xorbs/"):
			xorbGets = append(xorbGets, r.URL.Path)
		}
		mu.Unlock()
		if !serveV2 && strings.HasPrefix(r.URL.Path, "/v2/reconstructions/") {
			http.NotFound(w, r)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer srv.Close()

	c, err := client.NewClient(client.WithBaseURL(srv.URL), client.WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// file2 shares file1's prefix, so its upload dedups the leading chunks
	// against file1's xorb and its reconstruction spans two terms.
	file1 := deterministicData(1024 * 1024)
	file2 := append(append([]byte{}, file1[:512*1024]...), bytes.Repeat([]byte{0xC7}, 512*1024)...)
	if _, err := c.UploadFile(ctx, bytes.NewReader(file1)); err != nil {
		t.Fatal(err)
	}
	hash2, err := c.UploadFile(ctx, bytes.NewReader(file2))
	if err != nil {
		t.Fatal(err)
	}

	layout, err := c.GetReconstructionV1(ctx, hash2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(layout.Terms) < 2 {
		t.Fatalf("fixture produced %d terms, want at least 2", len(layout.Terms))
	}
	firstXorb := layout.Terms[0].Hash
	prefix := int64(layout.Terms[0].UnpackedLength + layout.Terms[1].UnpackedLength/2)

	for _, tc := range []struct {
		name                string
		v2                  bool
		wantReconstructions []string
	}{
		{name: "v2", v2: true, wantReconstructions: []string{fmt.Sprintf("/v2 bytes=%d-", prefix)}},
		{name: "v1 fallback", v2: false, wantReconstructions: []string{fmt.Sprintf("/v2 bytes=%d-", prefix), fmt.Sprintf("/v1 bytes=%d-", prefix)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, err := os.CreateTemp(t.TempDir(), "out")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if _, err := f.Write(file2[:prefix]); err != nil {
				t.Fatal(err)
			}

			mu.Lock()
			v2Available = tc.v2
			reconstructions, xorbGets = nil, nil
			mu.Unlock()

			// A fresh cache dir keeps the suffix xorb fetch observable.
			dc, err := client.NewClient(client.WithBaseURL(srv.URL), client.WithCacheDir(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			dst := &trackedWriteSeeker{f: f}
			if err := dc.DownloadFile(ctx, hash2, dst); err != nil {
				t.Fatal(err)
			}

			mu.Lock()
			gotReconstructions, gotXorbGets := reconstructions, xorbGets
			mu.Unlock()
			if strings.Join(gotReconstructions, ",") != strings.Join(tc.wantReconstructions, ",") {
				t.Fatalf("reconstruction requests = %q, want %q", gotReconstructions, tc.wantReconstructions)
			}
			for _, p := range gotXorbGets {
				if strings.Contains(p, firstXorb) {
					t.Fatalf("fetched xorb %s that only backs the seeded prefix", firstXorb)
				}
			}
			if len(gotXorbGets) == 0 {
				t.Fatal("no xorb fetched for the missing suffix")
			}
			if dst.rewound || dst.written != int64(len(file2))-prefix {
				t.Fatalf("wrote %d bytes (rewound=%v), want %d after the seeded prefix", dst.written, dst.rewound, int64(len(file2))-prefix)
			}
			got, err := os.ReadFile(f.Name())
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, file2) {
				t.Fatalf("resumed file has %d bytes and differs from the original %d bytes", len(got), len(file2))
			}
		})
	}
}

// trackedWriteSeeker deliberately hides os.File's ReadFrom so every byte
// passes through Write.
type trackedWriteSeeker struct {
	f       *os.File
	written int64
	rewound bool
}

func (w *trackedWriteSeeker) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	w.written += int64(n)
	return n, err
}

func (w *trackedWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekStart && offset == 0 {
		w.rewound = true
	}
	return w.f.Seek(offset, whence)
}
