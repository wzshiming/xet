package e2e_test

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage/memory"
)

type shardProgressCall struct {
	name            string
	verified, total int64
}

func TestUploadFilesV2ReportsShardProgress(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(server.NewHandler(server.WithStorage(memory.NewStorage())))
	defer srv.Close()

	var calls []shardProgressCall
	c, err := client.NewClient(
		client.WithCache(client.NewCache(t.TempDir(), 0, 0)),
		client.WithUpstreamProvider(client.StaticUpstreamProvider(srv.URL, "")),
		client.WithShardProgressFunc(func(name string, verified, total int64) {
			calls = append(calls, shardProgressCall{name, verified, total})
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	files := [][]byte{[]byte("first file of a two-file shard"), []byte("second file of a two-file shard")}
	hashes, err := c.UploadFilesV2(ctx, []io.ReadSeeker{bytes.NewReader(files[0]), bytes.NewReader(files[1])})
	if err != nil {
		t.Fatal(err)
	}

	n := 0
	for n < len(calls) && calls[n].name == "validating" {
		n++
	}
	if n == 0 || len(calls) != n+2 || calls[n].name != "uploading" || calls[n+1].name != "syncing" {
		t.Fatalf("shard progress = %+v, want validating calls, then one uploading, then one syncing", calls)
	}
	for i := 1; i < n; i++ {
		if calls[i].verified < calls[i-1].verified || calls[i].total < calls[i-1].total {
			t.Fatalf("shard progress = %+v, validating call %d goes backwards", calls, i)
		}
	}
	if last := calls[n-1]; last.verified != last.total || last.total < int64(1+len(files)) {
		t.Fatalf("shard progress = %+v, want the last validating call to verify all of at least %d units", calls, 1+len(files))
	}

	for i, data := range files {
		assertDownload(t, ctx, srv.URL, hashes[i], data)
	}
}
