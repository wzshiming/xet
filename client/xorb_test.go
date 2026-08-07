package client

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
