package matrix_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/hf"
	"github.com/wzshiming/xet/test/conformance/rustref"
	"github.com/wzshiming/xet/test/conformance/utils"
)

// redirectingHub is a fake Hugging Face hub whose xet token routes answer with
// a redirect before handing out the CAS endpoint and token, mimicking the
// hub's behavior for moved or renamed repos.
type redirectingHub struct {
	url       string
	entryHits atomic.Int64 // requests that were answered with a redirect
	tokenHits atomic.Int64 // requests that reached the final token route
}

// startRedirectingHub starts a hub whose token routes redirect with the given
// status code. With crossHost the redirect target lives on a second host,
// where the hub credential must have been dropped, matching xet-core's
// reqwest policy; a same-host hop must keep it.
func startRedirectingHub(t *testing.T, casEndpoint string, code int, crossHost bool) *redirectingHub {
	t.Helper()

	hub := &redirectingHub{}
	exp := time.Now().Add(time.Hour).Unix()
	serveToken := func(w http.ResponseWriter, r *http.Request, wantAuth string) {
		hub.tokenHits.Add(1)
		if auth := r.Header.Get("Authorization"); auth != wantAuth {
			t.Errorf("token route auth = %q, want %q", auth, wantAuth)
		}
		var mode string
		switch {
		case strings.Contains(r.URL.Path, "xet-write-token"):
			mode = "write"
		case strings.Contains(r.URL.Path, "xet-read-token"):
			mode = "read"
		default:
			t.Errorf("unexpected token path: %s", r.URL.Path)
		}
		fmt.Fprintf(w, `{"casUrl":%q,"accessToken":"cas-%s-token","exp":%d}`, casEndpoint, mode, exp)
	}
	redirect := func(w http.ResponseWriter, r *http.Request, location string) {
		hub.entryHits.Add(1)
		if auth := r.Header.Get("Authorization"); auth != "Bearer hf-token" {
			t.Errorf("redirecting route auth = %q, want %q", auth, "Bearer hf-token")
		}
		http.Redirect(w, r, location, code)
	}

	if crossHost {
		final := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			serveToken(w, r, "") // the chain left the hub host, credential dropped
		}))
		t.Cleanup(final.Close)
		entry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			redirect(w, r, final.URL+r.URL.Path)
		}))
		t.Cleanup(entry.Close)
		hub.url = entry.URL
		return hub
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/moved/", func(w http.ResponseWriter, r *http.Request) {
		serveToken(w, r, "Bearer hf-token") // same-host hop keeps the credential
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		redirect(w, r, "/moved"+r.URL.Path)
	})
	entry := httptest.NewServer(mux)
	t.Cleanup(entry.Close)
	hub.url = entry.URL
	return hub
}

// newHFClient returns a client without a static base URL, so the CAS endpoint
// can only come from the token response handed out after the redirect.
func newHFClient(t *testing.T) *client.Client {
	t.Helper()
	c, err := client.NewClient(client.WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatalf("create Go client: %v", err)
	}
	return c
}

func uploadFileWithProvider(ctx context.Context, c *client.Client, protocol rustref.ProtocolVersion, provider client.AuthProvider, r io.ReadSeeker) (xet.FileHash, error) {
	if protocol == rustref.ProtocolV2 {
		return c.UploadFileV2WithAuthProvider(ctx, provider, r)
	}
	return c.UploadFileV1WithAuthProvider(ctx, provider, r)
}

func downloadFileWithProvider(ctx context.Context, c *client.Client, protocol rustref.ProtocolVersion, provider client.AuthProvider, hash xet.FileHash, w io.WriteSeeker) error {
	if protocol == rustref.ProtocolV2 {
		return c.DownloadFileV2WithAuthProvider(ctx, provider, hash, w)
	}
	return c.DownloadFileV1WithAuthProvider(ctx, provider, hash, w)
}

// TestHFUploadRedirectMatrix runs the full Hugging Face flow — write-token
// upload followed by read-token download — with both the xet-core client and
// the Go client, against every server kind and protocol version, for every
// redirect status code the hub may answer the token routes with, on both
// same-host and cross-host topologies. xet-core resolves the CAS endpoint and
// token through its real hub token refresher, so the Go client's redirect
// handling is verified against the reference behavior, not assumptions.
func TestHFUploadRedirectMatrix(t *testing.T) {
	redirectCodes := []int{
		http.StatusMovedPermanently,  // 301
		http.StatusFound,             // 302
		http.StatusSeeOther,          // 303
		http.StatusTemporaryRedirect, // 307
		http.StatusPermanentRedirect, // 308
	}
	topologies := []struct {
		name      string
		crossHost bool
	}{
		{name: "same_host", crossHost: false},
		{name: "cross_host", crossHost: true},
	}
	clients := []clientKind{xetCoreClient, goClient}

	runServerProtocolMatrix(t, func(t *testing.T, server serverKind, protocol rustref.ProtocolVersion) {
		running := server.start(t)
		for _, code := range redirectCodes {
			for _, topo := range topologies {
				t.Run(fmt.Sprintf("redirect_%d_%s", code, topo.name), func(t *testing.T) {
					for _, kind := range clients {
						t.Run(string(kind), func(t *testing.T) {
							hub := startRedirectingHub(t, running.endpoint, code, topo.crossHost)

							// Unique content per cell so a shared server cannot
							// satisfy a cell from another cell's upload.
							label := fmt.Sprintf("%s/%s/%d/%s/%s", server.name, protocol, code, topo.name, kind)
							data := append([]byte(label+": "), utils.MakeRandData(256*1024)...)

							if kind == xetCoreClient {
								xetCoreHFRoundTrip(t, hub, protocol, label, data)
							} else {
								goHFRoundTrip(t, hub, protocol, label, data)
							}

							// Both token modes must have traversed the redirect.
							if hub.entryHits.Load() < 2 {
								t.Errorf("redirecting route hit %d times, want >= 2 (write and read token)", hub.entryHits.Load())
							}
							if hub.tokenHits.Load() < 2 {
								t.Errorf("token route hit %d times, want >= 2 (write and read token)", hub.tokenHits.Load())
							}
						})
					}
				})
			}
		}
	})
}

const (
	hubWriteTokenPath = "/api/models/org/repo/xet-write-token/main"
	hubReadTokenPath  = "/api/models/org/repo/xet-read-token/main"
)

// xetCoreHFRoundTrip uploads and downloads with the real xet-core client,
// resolving CAS endpoint and tokens through the redirecting hub routes.
func xetCoreHFRoundTrip(t *testing.T, hub *redirectingHub, protocol rustref.ProtocolVersion, label string, data []byte) {
	t.Helper()
	dir := t.TempDir()
	src := filepath.Join(dir, "upload")
	if err := os.WriteFile(src, data, 0o600); err != nil {
		t.Fatalf("write upload fixture: %v", err)
	}

	results, err := rustref.UploadFilesViaTokenRefresh([]string{src}, hub.url+hubWriteTokenPath, "hf-token", protocol)
	if err != nil {
		t.Fatalf("hf upload with xet-core client: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("uploaded %d files, want 1", len(results))
	}
	if results[0].FileSize != uint64(len(data)) {
		t.Errorf("reported size = %d, want %d", results[0].FileSize, len(data))
	}

	dest := filepath.Join(dir, "download")
	if _, err := rustref.DownloadFilesViaTokenRefresh([]rustref.DownloadRequest{{
		DestinationPath: dest,
		Hash:            results[0].Hash,
		FileSize:        int64(len(data)),
	}}, hub.url+hubReadTokenPath, "hf-token", protocol); err != nil {
		t.Fatalf("hf download with xet-core client: %v", err)
	}
	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read download file: %v", err)
	}
	checkContent(t, label, got, data)
}

// goHFRoundTrip uploads and downloads with the Go client through the hf
// token providers pointed at the redirecting hub.
func goHFRoundTrip(t *testing.T, hub *redirectingHub, protocol rustref.ProtocolVersion, label string, data []byte) {
	t.Helper()
	target := hf.Target{Endpoint: hub.url, RepoType: "model", RepoID: "org/repo", Revision: "main"}

	writeProvider := hf.NewWriteTokenProvider(nil, target, "hf-token")
	hash, err := uploadFileWithProvider(context.Background(), newHFClient(t), protocol, writeProvider, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("hf upload with Go client: %v", err)
	}

	// Fresh client and cache so the download cannot be served from
	// client-local state; the read token route is redirected the same way.
	readProvider := hf.NewReadTokenProvider(nil, target, "hf-token")
	path := filepath.Join(t.TempDir(), "download")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create download file: %v", err)
	}
	err = downloadFileWithProvider(context.Background(), newHFClient(t), protocol, readProvider, hash, file)
	closeErr := file.Close()
	if err != nil {
		t.Fatalf("hf download with Go client: %v", err)
	}
	if closeErr != nil {
		t.Fatalf("close download file: %v", closeErr)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read download file: %v", err)
	}
	checkContent(t, label, got, data)
}
