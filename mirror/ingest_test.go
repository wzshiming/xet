package mirror

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
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
		const wantCommit = "4dddf896b78f84b58402ab0da2c89514b0e17d1f"
		if entry.Commit != wantCommit {
			t.Fatalf("Commit = %q, want %q", entry.Commit, wantCommit)
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

func awaitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: timed out", what)
	}
}

func TestIngestInFlight(t *testing.T) {
	commit := strings.Repeat("ab", 20)
	for _, tc := range []struct{ name, rev string }{
		{"same alias", "main"},
		{"branch alias", "dev"},
		{"direct commit", commit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := newPlainUpstream()
			upstream.gate = make(chan struct{})
			upstream.gateHit = make(chan struct{})
			upstream.commit = commit
			data := make([]byte, 128*1024)
			if _, err := rand.Read(data); err != nil {
				t.Fatal(err)
			}
			upstream.set("/org/repo/resolve/main/model.bin", data)
			upstream.set("/org/repo/resolve/dev/model.bin", data)
			upstreamSrv := httptest.NewServer(upstream)
			defer upstreamSrv.Close()
			var release sync.Once
			defer release.Do(func() { close(upstream.gate) }) // Close blocks on the gated handler

			m, stor := newTestMirror(t, upstreamSrv.URL, t.TempDir(), t.TempDir())
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			in, err := m.Ingest("org/repo", "main", "model.bin")
			if err != nil {
				t.Fatalf("Ingest: %v", err)
			}
			awaitClosed(t, upstream.gateHit, "upstream transfer")
			select {
			case <-in.Done():
				t.Fatal("Done closed while the upstream transfer is still gated")
			default:
			}
			if e, err := in.Entry(); e != nil || err != nil {
				t.Fatalf("Entry before Done = %v, %v; want nil, nil", e, err)
			}

			res, err := m.Resolve(ctx, "org/repo", tc.rev, "model.bin")
			if err != nil || res.Stream == nil {
				t.Fatalf("Resolve %s = %+v, %v; want the in-flight stream", tc.rev, res, err)
			}
			m.mu.Lock()
			pinned, tasks := m.tasks[resolveKey{repo: "org/repo", rev: commit, path: "model.bin"}], len(m.tasks)
			m.mu.Unlock()
			if pinned == nil || res.Stream.t != pinned || tasks != 1 {
				t.Fatalf("%s attached to task %p, pinned task %p, %d tasks; want one shared task", tc.rev, res.Stream.t, pinned, tasks)
			}
			if _, c, err := res.Stream.WaitMeta(ctx); err != nil || c != commit {
				t.Fatalf("WaitMeta = %q, %v; want commit %s", c, err, commit)
			}
			if got := upstream.dataGETs.Load(); got != 1 {
				t.Fatalf("upstream GETs while gated = %d, want 1", got)
			}

			joined, err := m.Ingest("org/repo", tc.rev, "model.bin")
			if err != nil {
				t.Fatalf("joining Ingest: %v", err)
			}
			select {
			case <-joined.Done():
				t.Fatal("joined Done closed while the upstream transfer is still gated")
			default:
			}

			release.Do(func() { close(upstream.gate) })
			sum := sha256.Sum256(data)
			for name, h := range map[string]*Ingestion{"first": in, "joined": joined} {
				awaitClosed(t, h.Done(), name+" Done")
				entry, err := h.Entry()
				if err != nil {
					t.Fatalf("%s Entry: %v", name, err)
				}
				if entry.Commit != commit || entry.SHA256 != hex.EncodeToString(sum[:]) || entry.Size != int64(len(data)) {
					t.Fatalf("%s entry = %+v, want commit %s, sha256 %x, size %d", name, entry, commit, sum, len(data))
				}
			}
			if got := readStored(t, stor, hex.EncodeToString(sum[:])); !bytes.Equal(got, data) {
				t.Fatalf("stored bytes mismatch: got %d bytes, want %d", len(got), len(data))
			}
			if got := upstream.dataGETs.Load(); got != 1 {
				t.Fatalf("upstream GETs = %d, want 1 (joins must share one download)", got)
			}
		})
	}
}
