package mirror

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet/client/hf"
	"github.com/wzshiming/xet/download"
)

// xorbGate passes requests to http.DefaultTransport, holding each xorb GET
// until release is closed and signaling it on requested.
type xorbGate struct {
	requested chan struct{}
	release   chan struct{}
}

func (g *xorbGate) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodGet && strings.HasPrefix(req.URL.Path, "/v1/xorbs/") {
		select {
		case g.requested <- struct{}{}:
		default:
		}
		select {
		case <-g.release:
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	}
	return http.DefaultTransport.RoundTrip(req)
}

// TestMirrorClientsShareOneBudget pins that the mirror's per-download xet
// clients hold their terms in the one memory budget the mirror owns: while
// downloads through two clients wait for their xorbs, that budget cannot admit
// its largest target, and once both are done it can.
func TestMirrorClientsShareOneBudget(t *testing.T) {
	const path = "/org/a/resolve/main/f.bin"
	up := newXetUpstream(t, path, []byte("xet bytes held in the mirror's budget"), "cas-token")
	gate := &xorbGate{requested: make(chan struct{}, 2), release: make(chan struct{})}
	m, _ := newTestMirror(t, up.hubURL, t.TempDir(), t.TempDir(), WithTransport(gate))
	repo, file, err := hf.ParseResolveURL(up.hubURL + path)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 2)
	for range 2 {
		c, err := m.newClient(repo, "")
		if err != nil {
			t.Fatal(err)
		}
		resolved, err := c.Resolve(t.Context(), file)
		if err != nil {
			t.Fatal(err)
		}
		out, err := os.CreateTemp(t.TempDir(), "out")
		if err != nil {
			t.Fatal(err)
		}
		defer out.Close()
		go func() { done <- c.DownloadFile(t.Context(), resolved.Hash, out) }()
	}
	for range 2 {
		select {
		case <-gate.requested:
		case <-time.After(10 * time.Second):
			close(gate.release)
			t.Fatal("the downloads requested no xorb")
		}
	}

	limit := download.DefaultMemorySizes().Limit
	waiting, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if err := m.budget.Acquire(waiting, limit); !errors.Is(err, context.DeadlineExceeded) {
		close(gate.release)
		t.Fatalf("acquiring the mirror's whole budget during two downloads = %v, want it to wait for their terms", err)
	}
	close(gate.release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	admitted, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := m.budget.Acquire(admitted, limit); err != nil {
		t.Fatalf("acquiring the mirror's whole budget after the downloads = %v, want it admitted", err)
	}
}
