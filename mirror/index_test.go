package mirror

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/wzshiming/xet/storage"
)

// A ready entry is only served while its file is still in storage: after a GC
// unlink the next resolve must drop the entry and re-ingest from the upstream
// instead of redirecting to a dead bridge.
func TestMirrorReingestsAfterStorageUnlink(t *testing.T) {
	upstream := newPlainUpstream()
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	data := []byte("reingest-after-unlink data")
	const resolvePath = "/org/repo/resolve/main/f.bin"
	upstream.set(resolvePath, data)

	fx := newMirrorFixture(t, upstreamSrv.URL, t.TempDir(), t.TempDir())
	resolveURL := fx.srv.URL + resolvePath
	waitReady(t, resolveURL)

	ctx := context.Background()
	k, _ := parseResolveKey(resolvePath)
	e := fx.handler.entryForKey(ctx, k)
	if e == nil || e.FileHash == "" {
		t.Fatalf("ready entry = %+v", e)
	}

	if _, err := storage.Unlink(ctx, fx.stor.(storage.SweepStore), e.FileHash); err != nil {
		t.Fatalf("Unlink: %v", err)
	}

	// Storage is the source of truth: the entry no longer resolves and its
	// index file is gone.
	if got := fx.handler.entryForKey(ctx, k); got != nil {
		t.Fatalf("entry still resolves after unlink: %+v", got)
	}
	if _, err := os.Stat(indexEntryPath(fx.handler.indexDir, k.String())); !os.IsNotExist(err) {
		t.Fatalf("dead entry file still persisted: %v", err)
	}

	// The next resolve re-ingests from the upstream and serves again.
	before := upstream.dataGETs.Load()
	resp := waitReady(t, resolveURL)
	if upstream.dataGETs.Load() == before {
		t.Fatal("no upstream refetch after unlink")
	}
	bridge, err := http.Get(resp.Header.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(bridge.Body)
	bridge.Body.Close()
	if err != nil || bridge.StatusCode != http.StatusOK || string(body) != string(data) {
		t.Fatalf("bridge after re-ingest = %d, %v", bridge.StatusCode, err)
	}
}
