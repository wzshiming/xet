package local

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/crc64nvme"
	"github.com/wzshiming/xet/internal/xorbmap"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/storagetest"
)

// TestCommitStagedXorbDeletedMidCommit: a staged file deleted after the commit
// opened it, as a sweep past the grace window may, commits as not found.
func TestCommitStagedXorbDeletedMidCommit(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	fs, err := NewStorage(WithBasePath(basePath))
	if err != nil {
		t.Fatal(err)
	}
	data, xorbHash := storagetest.EncodeXorb(t, true, []byte("swept while committing"))
	grant, err := fs.GrantXorbUpload(ctx, xorbHash, int64(len(data)), crc64nvme.Checksum(data))
	if err != nil {
		t.Fatal(err)
	}
	if err := fs.PutStagedXorb(ctx, grant.ID, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	// Remove runs the last release hook under the cache's lock, so a blocking one parks the commit in HasXorb, after its open.
	held, release := make(chan struct{}), make(chan struct{})
	fs.xorbCache = xorbmap.New(0, func(xet.XorbHash) (*os.File, error) {
		return os.Open(os.DevNull)
	}, func(xet.XorbHash) {
		close(held)
		<-release
	})
	m, err := fs.xorbCache.Acquire(xet.XorbHash{})
	if err != nil {
		t.Fatal(err)
	}
	m.Release()
	go fs.xorbCache.Remove(xet.XorbHash{})
	<-held
	type result struct {
		published bool
		err       error
	}
	done := inBackground(func() result {
		published, err := fs.CommitStagedXorb(ctx, grant.ID, xorbHash)
		return result{published, err}
	})
	buf := make([]byte, 1<<20)
	for deadline := time.Now().Add(10 * time.Second); !bytes.Contains(buf[:runtime.Stack(buf, true)], []byte("(*Storage).HasXorb(")); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("CommitStagedXorb never reached HasXorb")
		}
	}
	if err := os.Remove(fs.objectPath("staging", grant.ID)); err != nil {
		t.Fatal(err)
	}
	close(release)

	if got := await(t, done); got.published || !errors.Is(got.err, storage.ErrStagedXorbNotFound) {
		t.Fatalf("CommitStagedXorb of a staged file deleted mid-commit = %v, %v; want false, ErrStagedXorbNotFound", got.published, got.err)
	}
	if ok, err := fs.HasXorb(ctx, xorbHash); err != nil || ok {
		t.Fatalf("HasXorb = %v, %v; want absent", ok, err)
	}
	for _, kind := range []string{"staging", "xorbs"} {
		if left, err := fanoutEntries(filepath.Join(basePath, kind)); err != nil || len(left) != 0 {
			t.Fatalf("%s/ holds %v, %v; want nothing", kind, left, err)
		}
	}
}
