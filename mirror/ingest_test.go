package mirror

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http/httptest"
	"testing"
)

func TestIngest(t *testing.T) {
	upstream := newPlainUpstream()
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	data := make([]byte, 128*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	const resolvePath = "/org/repo/resolve/main/model.bin"
	upstream.set(resolvePath, data)

	m, stor := newTestMirror(t, upstreamSrv.URL, t.TempDir(), t.TempDir())

	t.Run("rejects invalid components", func(t *testing.T) {
		if _, err := m.Ingest("org/repo", "main/extra", "model.bin"); err == nil {
			t.Fatal("expected an error for a rev containing a slash")
		}
		if _, err := m.Ingest("org/repo", "main", ""); err == nil {
			t.Fatal("expected an error for an empty path")
		}
	})

	t.Run("resolves once done", func(t *testing.T) {
		in, err := m.Ingest("org/repo", "main", "model.bin")
		if err != nil {
			t.Fatalf("Ingest: %v", err)
		}
		<-in.Done()
		entry, err := in.Entry()
		if err != nil {
			t.Fatalf("Entry: %v", err)
		}
		sum := sha256.Sum256(data)
		if entry.SHA256 != hex.EncodeToString(sum[:]) {
			t.Fatalf("SHA256 = %q, want %q", entry.SHA256, hex.EncodeToString(sum[:]))
		}
		if entry.Size != int64(len(data)) {
			t.Fatalf("Size = %d, want %d", entry.Size, len(data))
		}
		if entry.FileHash == "" {
			t.Fatal("FileHash is empty")
		}
		if entry.Commit != "commit-1" {
			t.Fatalf("Commit = %q, want %q", entry.Commit, "commit-1")
		}
		if entry.ETag == "" {
			t.Fatal("ETag is empty")
		}

		// Readiness must hold the moment Done closes: Resolve answers with
		// the ready entry and storage serves the bytes.
		res, err := m.Resolve(context.Background(), "org/repo", "main", "model.bin")
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if res.Entry == nil {
			t.Fatal("Resolve did not return the ready entry immediately after Done")
		}
		if res.Entry.FileHash != entry.FileHash {
			t.Fatalf("Resolve FileHash = %q, want %q", res.Entry.FileHash, entry.FileHash)
		}
		if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, data) {
			t.Fatalf("stored bytes mismatch: got %d bytes, want %d", len(got), len(data))
		}
	})

	t.Run("ready entry resolves without new downloads", func(t *testing.T) {
		before := upstream.dataGETs.Load()
		in, err := m.Ingest("org/repo", "main", "model.bin")
		if err != nil {
			t.Fatalf("Ingest: %v", err)
		}
		<-in.Done()
		entry, err := in.Entry()
		if err != nil {
			t.Fatalf("Entry: %v", err)
		}
		if entry.Size != int64(len(data)) {
			t.Fatalf("Size = %d, want %d", entry.Size, len(data))
		}
		if got := upstream.dataGETs.Load(); got != before {
			t.Fatalf("upstream GETs went %d -> %d, want no new downloads", before, got)
		}
	})

	t.Run("not found matches ErrUpstreamNotFound", func(t *testing.T) {
		in, err := m.Ingest("org/repo", "main", "missing.bin")
		if err != nil {
			t.Fatalf("Ingest: %v", err)
		}
		<-in.Done()
		if _, err := in.Entry(); !errors.Is(err, ErrUpstreamNotFound) {
			t.Fatalf("err = %v, want ErrUpstreamNotFound", err)
		}
		// The failure is cached with backoff and resolves without re-probing.
		in, err = m.Ingest("org/repo", "main", "missing.bin")
		if err != nil {
			t.Fatalf("Ingest: %v", err)
		}
		<-in.Done()
		if _, err := in.Entry(); !errors.Is(err, ErrUpstreamNotFound) {
			t.Fatalf("cached err = %v, want ErrUpstreamNotFound", err)
		}
	})
}

// TestIngestInFlight verifies Ingest returns while the download runs, a
// second Ingest joins the same task, and abandoning an Ingestion never
// cancels the ingest.
func TestIngestInFlight(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.gate = make(chan struct{})
	upstream.gateHit = make(chan struct{})
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	data := make([]byte, 128*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	const resolvePath = "/org/repo/resolve/main/model.bin"
	upstream.set(resolvePath, data)

	m, _ := newTestMirror(t, upstreamSrv.URL, t.TempDir(), t.TempDir())

	in, err := m.Ingest("org/repo", "main", "model.bin")
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	<-upstream.gateHit // Ingest already returned while the transfer is held open
	select {
	case <-in.Done():
		t.Fatal("Done closed while the upstream transfer is still gated")
	default:
	}
	if e, err := in.Entry(); e != nil || err != nil {
		t.Fatalf("Entry before Done = %v, %v; want nil, nil", e, err)
	}

	joined, err := m.Ingest("org/repo", "main", "model.bin")
	if err != nil {
		t.Fatalf("joining Ingest: %v", err)
	}
	// The first handle is abandoned from here on; the ingest must not care.

	close(upstream.gate) // let the transfer finish
	<-joined.Done()
	entry, err := joined.Entry()
	if err != nil {
		t.Fatalf("Entry: %v", err)
	}
	sum := sha256.Sum256(data)
	if entry.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("SHA256 = %q, want %q", entry.SHA256, hex.EncodeToString(sum[:]))
	}
	if got := upstream.dataGETs.Load(); got != 1 {
		t.Fatalf("upstream GETs = %d, want 1 (joins must share one download)", got)
	}
}
