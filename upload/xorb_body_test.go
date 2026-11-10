package upload

import (
	"bytes"
	"context"
	"io"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"
)

// sentXorb is one UploadXorb call: the hash it named and the bytes it read.
type sentXorb struct {
	hash xet.XorbHash
	body []byte
}

// recordingClient is a stubClient that keeps every xorb it is sent.
type recordingClient struct {
	stubClient
	mu    sync.Mutex
	xorbs []sentXorb
}

func (c *recordingClient) UploadXorb(_ context.Context, xorbHash xet.XorbHash, r io.ReadSeeker) (*XorbUploadResponse, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.xorbs = append(c.xorbs, sentXorb{xorbHash, body})
	return &XorbUploadResponse{WasInserted: true}, nil
}

// footerlessXorb decodes body as a footer-less chunk sequence, failing t
// unless the last chunk ends the body.
func footerlessXorb(t *testing.T, body []byte) *xorb.Decoder {
	t.Helper()
	r := bytes.NewReader(body)
	dec := xorb.NewDecoder(r, false)
	buf := make([]byte, xet.MaxChunkSize)
	end := 0
	for {
		_, err := dec.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("decode the %d-byte body as chunks: %v", len(body), err)
		}
		end = len(body) - r.Len()
	}
	if rest := body[end:]; len(rest) != 0 {
		t.Fatalf("%d bytes %q... follow the last chunk of the %d-byte body, want the footer-less chunk sequence", len(rest), rest[:min(len(rest), 7)], len(body))
	}
	return dec
}

// UploadFiles hands UploadXorb every xorb as the bare chunk sequence xet-core
// 1.8.0 servers require: the file's chunks under the xorb's hash, then nothing.
func TestUploadFilesSendsFooterlessXorbs(t *testing.T) {
	data := make([]byte, 1<<20)
	_, _ = rand.NewChaCha8([32]byte{1}).Read(data)
	var chunkHashes []xet.ChunkHash
	if err := xet.ChunkData(bytes.NewReader(data), func(_ int64, chunk []byte) error {
		chunkHashes = append(chunkHashes, xet.ComputeChunkHash(chunk))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	client := &recordingClient{stubClient: stubClient{&fakeClient{}}}
	if _, err := UploadFiles(t.Context(), client, []io.ReadSeeker{bytes.NewReader(data)}, WithCacheManager(NewCacheManager(t.TempDir(), 0))); err != nil {
		t.Fatal(err)
	}
	var sent []xet.ChunkHash
	for _, x := range client.xorbs {
		dec := footerlessXorb(t, x.body)
		if got := dec.SummoryHash(); got != x.hash {
			t.Errorf("body sent as xorb %s hashes to %s", x.hash.String(), got.String())
		}
		hashes, _ := dec.Chunks()
		sent = append(sent, hashes...)
	}
	if !slices.Equal(sent, chunkHashes) {
		t.Errorf("%d xorbs carry %d chunks, want the file's %d in order", len(client.xorbs), len(sent), len(chunkHashes))
	}
}
