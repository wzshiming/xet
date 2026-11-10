package memory

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet/internal/crc64nvme"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/storagetest"
)

// commitParked reports whether a CommitStagedXorb call waits on a lock.
func commitParked() bool {
	buf := make([]byte, 64<<10)
	n := runtime.Stack(buf, true)
	for n == len(buf) {
		buf = make([]byte, 2*len(buf))
		n = runtime.Stack(buf, true)
	}
	for stack := range strings.SplitSeq(string(buf[:n]), "\n\n") {
		if strings.Contains(stack, "sync.runtime_Semacquire") && strings.Contains(stack, "memory.(*Storage).CommitStagedXorb(") {
			return true
		}
	}
	return false
}

// A sweep that deletes the staged upload after the commit read it but before
// it published leaves the commit nothing to publish: not found, and no xorb.
func TestCommitStagedXorbSweptBeforePublish(t *testing.T) {
	ctx := context.Background()
	data, xorbHash := storagetest.EncodeXorb(t, true, []byte("swept between its read and its publish"))
	type result struct {
		published bool
		err       error
	}
	for range 100 {
		st := NewStorage()
		grant, err := st.GrantXorbUpload(ctx, xorbHash, int64(len(data)), crc64nvme.Checksum(data))
		if err != nil {
			t.Fatalf("GrantXorbUpload: %v", err)
		}
		if err := st.PutStagedXorb(ctx, grant.ID, bytes.NewReader(data)); err != nil {
			t.Fatalf("PutStagedXorb: %v", err)
		}

		// Holding the store parks the commit at its first read.
		// This relies on sync.RWMutex's reader preference: Unlock lets the parked read in ahead of the sweep's Lock.
		st.mu.Lock()
		done := make(chan result, 1)
		go func() {
			published, err := st.CommitStagedXorb(ctx, grant.ID, xorbHash)
			done <- result{published, err}
		}()
		for deadline := time.Now().Add(10 * time.Second); !commitParked(); time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				st.mu.Unlock()
				t.Fatal("CommitStagedXorb never waited on the store's lock")
			}
		}
		// Unlocking admits the parked read, which the sweep's delete waits out
		// before it takes the lock ahead of the commit's publish.
		st.mu.Unlock()
		swept := st.delete("staging", grant.ID)
		got := <-done
		if !swept {
			// The commit took the lock first and published; nothing vanished, so try again.
			if !got.published || got.err != nil {
				t.Fatalf("CommitStagedXorb ahead of the sweep = %v, %v; want true, nil", got.published, got.err)
			}
			continue
		}
		if got.published || !errors.Is(got.err, storage.ErrStagedXorbNotFound) {
			t.Fatalf("CommitStagedXorb(upload swept between its read and its publish) = %v, %v; want false, ErrStagedXorbNotFound", got.published, got.err)
		}
		if ok, _ := st.HasXorb(ctx, xorbHash); ok {
			t.Fatal("HasXorb = true after the swept commit, want nothing published")
		}
		return
	}
	t.Fatal("the commit took the lock ahead of the sweep in every attempt")
}
