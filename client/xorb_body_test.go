package client

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/rand/v2"
	"net/http"
	"reflect"
	"strconv"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/crc64nvme"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/upload"
	"github.com/wzshiming/xet/xorb"
)

// xorbsOnly is a Client for xorb uploads alone: no xorb or chunk is known and
// shards land without a request, so only xorb routes reach the CAS.
type xorbsOnly struct{ *Client }

func (xorbsOnly) HasXorb(context.Context, xet.XorbHash) (bool, error) { return false, nil }

func (xorbsOnly) QueryDedupShards(context.Context, []xet.ChunkHash, ...xet.ChunkHash) (map[xet.ChunkHash]shard.ChunkLocation, error) {
	return nil, nil
}

func (xorbsOnly) UploadShard(context.Context, *shard.Shard) (*upload.ShardUploadResponse, error) {
	return &upload.ShardUploadResponse{}, nil
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

// The xorb UploadFiles stages goes out as the bare chunk sequence xet-core
// 1.8.0 servers require, on the granted PUT and the legacy POST alike, and the
// grant declares the length and CRC-64/NVME of exactly those bytes.
func TestUploadFilesSendsFooterlessXorbBodies(t *testing.T) {
	data := make([]byte, 1<<20)
	_, _ = rand.NewChaCha8([32]byte{2}).Read(data)
	var chunkHashes []xet.ChunkHash
	var chunkSizes []uint64
	if err := xet.ChunkData(bytes.NewReader(data), func(_ int64, chunk []byte) error {
		chunkHashes = append(chunkHashes, xet.ComputeChunkHash(chunk))
		chunkSizes = append(chunkSizes, uint64(len(chunk)))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	xorbHash := xet.ComputeXorbHash(chunkHashes, chunkSizes)
	for _, tc := range []struct {
		name  string
		grant []scriptedReply
		route string // the route carrying the xorb
		want  []string
	}{
		{"granted PUT", nil, "upload", []string{"grant", "upload g1", "commit g1"}},
		{"legacy POST after a 404 grant", []scriptedReply{{status: http.StatusNotFound}}, "legacy", []string{"grant", "legacy"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cas := newScriptedCASFor(t, xorbHash)
			cas.script("grant", tc.grant...)
			c, _ := cas.client(t)
			_, err := upload.UploadFiles(t.Context(), xorbsOnly{c}, []io.ReadSeeker{bytes.NewReader(data)}, upload.WithCacheManager(upload.NewCacheManager(t.TempDir(), 0)))
			cas.expectRoutes(t, tc.want...)
			if err != nil {
				t.Fatal(err)
			}
			body := cas.requests(tc.route)[0].body
			if got := footerlessXorb(t, body).SummoryHash(); got != xorbHash {
				t.Errorf("%s body hashes to %s, want the path's %s", tc.route, got.String(), xorbHash.String())
			}
			want := map[string]any{
				"length":   json.Number(strconv.Itoa(len(body))),
				"checksum": map[string]any{"algo": "crc64nvme", "value": strconv.FormatUint(crc64nvme.Checksum(body), 10)},
			}
			if got := cas.requests("grant")[0].decoded(t); !reflect.DeepEqual(got, want) {
				t.Errorf("grant body = %v, want %v for the %d-byte %s body", got, want, len(body), tc.route)
			}
		})
	}
}
