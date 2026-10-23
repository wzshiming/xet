package chaos_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSelfPassThrough(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "hello")
	}))
	t.Cleanup(backend.Close)
	p := newFaultProxy()
	t.Cleanup(p.Close)
	p.setTarget(backend.URL)
	p.arm(nil)

	resp, err := http.Get(p.URL() + "/x")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	got, err := io.ReadAll(resp.Body)
	if err != nil || string(got) != "hello" {
		t.Fatalf("body %q, err %v; want %q", got, err, "hello")
	}
}
