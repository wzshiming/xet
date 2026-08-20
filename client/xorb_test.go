package client

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/upload"
)

func TestUploadXorbFollowsDirectUploadRedirect(t *testing.T) {
	xorbData := []byte("raw xorb bytes")

	var putMethod, putAuth, putCond atomic.Value
	var putBody atomic.Pointer[[]byte]
	objectStore := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		putMethod.Store(r.Method)
		putAuth.Store(r.Header.Get("Authorization"))
		putCond.Store(r.Header.Get("If-None-Match"))
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read direct upload body: %v", err)
		}
		putBody.Store(&body)
		w.WriteHeader(http.StatusOK)
	}))
	defer objectStore.Close()

	uploadURL := objectStore.URL + "/bucket/xorbs/ab/cdef?X-Amz-Signature=sig"
	var postExpect, postAuth, postOptIn atomic.Value
	casServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		postExpect.Store(r.Header.Get("Expect"))
		postAuth.Store(r.Header.Get("Authorization"))
		postOptIn.Store(r.Header.Get(upload.HeaderDirectUpload))
		w.Header().Set("Location", uploadURL)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer casServer.Close()

	c, err := NewClient(WithBaseURL(casServer.URL), WithToken("secret"))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.UploadXorb(t.Context(), xet.XorbHash{1}, bytes.NewReader(xorbData))
	if err != nil {
		t.Fatal(err)
	}
	if !resp.WasInserted {
		t.Fatal("WasInserted = false, want true after direct upload")
	}

	if got := postExpect.Load(); got != "100-continue" {
		t.Errorf("xorb POST Expect header = %q, want 100-continue", got)
	}
	if got := postOptIn.Load(); got != upload.DirectUploadAccept {
		t.Errorf("xorb POST %s header = %q, want %q", upload.HeaderDirectUpload, got, upload.DirectUploadAccept)
	}
	if got := postAuth.Load(); got != "Bearer secret" {
		t.Errorf("xorb POST Authorization = %q, want Bearer secret", got)
	}
	if got := putMethod.Load(); got != http.MethodPut {
		t.Errorf("direct upload method = %q, want PUT", got)
	}
	if got := putCond.Load(); got != "*" {
		t.Errorf("direct upload If-None-Match = %q, want *", got)
	}
	if got := putAuth.Load(); got != "" {
		t.Errorf("Authorization forwarded to direct upload URL: %q", got)
	}
	if got := putBody.Load(); got == nil || !bytes.Equal(*got, xorbData) {
		t.Errorf("direct upload body does not match xorb bytes")
	}
}

func TestUploadXorbReportsDirectUploadFailure(t *testing.T) {
	objectStore := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "access denied", http.StatusForbidden)
	}))
	defer objectStore.Close()

	casServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", objectStore.URL+"/bucket/xorbs/ab/cdef?X-Amz-Signature=topsecret")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer casServer.Close()

	c, err := NewClient(WithBaseURL(casServer.URL))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.UploadXorb(t.Context(), xet.XorbHash{1}, bytes.NewReader([]byte("data")))
	if err == nil || !strings.Contains(err.Error(), "direct xorb upload error") {
		t.Fatalf("expected direct upload error, got %v", err)
	}
	if strings.Contains(err.Error(), "topsecret") {
		t.Fatalf("error leaks the presigned query: %v", err)
	}
}

// A 412 means another uploader stored the same content-addressed xorb
// between the redirect and the PUT; that is success, not an error.
func TestUploadXorbDirectTreatsPreconditionFailedAsExisting(t *testing.T) {
	objectStore := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusPreconditionFailed)
	}))
	defer objectStore.Close()

	casServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", objectStore.URL+"/bucket/xorbs/ab/cdef")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer casServer.Close()

	c, err := NewClient(WithBaseURL(casServer.URL))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.UploadXorb(t.Context(), xet.XorbHash{1}, bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("UploadXorb: %v", err)
	}
	if resp.WasInserted {
		t.Fatal("WasInserted = true for a 412 (already stored) direct upload")
	}
}

func TestDownloadXorbWithURLAcceptsExactRangeAsOK(t *testing.T) {
	const body = "term"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got != "bytes=0-3" {
			t.Errorf("Range = %q, want bytes=0-3", got)
		}
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()

	c, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	r, err := c.DownloadXorbWithURL(t.Context(), srv.URL, http.Header{"Range": {"bytes=0-3"}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != body {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

func TestDownloadXorbWithURLAcceptsPartialWithoutContentRange(t *testing.T) {
	const body = "term"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()

	c, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	r, err := c.DownloadXorbWithURL(t.Context(), srv.URL, http.Header{"Range": {"bytes=0-3"}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != body {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

func TestDownloadXorbWithURLRejectsWrongLengthOKResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "whole xorb")
	}))
	defer srv.Close()

	c, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.DownloadXorbWithURL(t.Context(), srv.URL, http.Header{"Range": {"bytes=0-3"}})
	if err == nil || !strings.Contains(err.Error(), "status 200 OK") {
		t.Fatalf("expected range response error, got %v", err)
	}
}
