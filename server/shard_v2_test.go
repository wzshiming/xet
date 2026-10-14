package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/handlers"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/memory"
	"github.com/wzshiming/xet/storage/storagetest"
)

// shardV2TestStorage holds the first call to its gate method until unblock.
type shardV2TestStorage struct {
	storage.Storage
	gate    string
	gateErr error
	held    atomic.Bool
	started chan struct{}
	release chan struct{}
}

// hold gives up after five seconds so a failed test cannot hang the handler.
func (s *shardV2TestStorage) hold(method string) error {
	if method != s.gate || !s.held.CompareAndSwap(false, true) {
		return nil
	}
	close(s.started)
	select {
	case <-s.release:
		return s.gateErr
	case <-time.After(5 * time.Second):
		return fmt.Errorf("%s gate was never released", method)
	}
}

func (s *shardV2TestStorage) unblock() {
	select {
	case <-s.release:
	default:
		close(s.release)
	}
}

func (s *shardV2TestStorage) GetFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (string, error) {
	if err := s.hold("GetFileIndexEntry"); err != nil {
		return "", err
	}
	return s.Storage.GetFileIndexEntry(ctx, fileHash)
}

func (s *shardV2TestStorage) GetXorbRangeReadCloser(ctx context.Context, xorbHash xet.XorbHash, start, end int64) (io.ReadCloser, error) {
	if err := s.hold("GetXorbRangeReadCloser"); err != nil {
		return nil, err
	}
	return s.Storage.GetXorbRangeReadCloser(ctx, xorbHash, start, end)
}

func (s *shardV2TestStorage) PutShardObject(ctx context.Context, shardHash string, encoded []byte) (bool, error) {
	if err := s.hold("PutShardObject"); err != nil {
		return false, err
	}
	return s.Storage.PutShardObject(ctx, shardHash, encoded)
}

func (s *shardV2TestStorage) PutIndexEntry(ctx context.Context, kind, name, shardHash string) error {
	if err := s.hold("PutIndexEntry"); err != nil {
		return err
	}
	return s.Storage.PutIndexEntry(ctx, kind, name, shardHash)
}

// newShardV2TestStorage stores one single-chunk xorb per term of the returned one-file shard.
func newShardV2TestStorage(t *testing.T, gate string, terms int) (*shardV2TestStorage, []byte) {
	t.Helper()
	backend := memory.NewStorage()
	parts := make([][]byte, terms)
	for i := range parts {
		parts[i] = fmt.Appendf(nil, "shard upload term %d", i)
	}
	shardObj := shard.NewShard()
	storagetest.AddFileBlock(t, context.Background(), backend, shardObj, parts)
	// AddFileBlock leaves NumBytesInCAS zero, which the handler's Validate rejects.
	for i := range shardObj.CASInfos {
		shardObj.CASInfos[i].NumBytesInCAS = shardObj.CASInfos[i].Chunks[0].UnpackedSegBytes
	}
	encoded, err := shardObj.Encode(false)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return &shardV2TestStorage{Storage: backend, gate: gate, started: make(chan struct{}), release: make(chan struct{})}, body
}

type shardUploadWireEvent struct {
	Type      string `json:"type"`
	Verified  uint64 `json:"verified"`
	Total     uint64 `json:"total"`
	Stage     string `json:"stage"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

func readShardUploadWireEvent(t *testing.T, r *bufio.Reader) shardUploadWireEvent {
	t.Helper()
	line, err := r.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read shard upload event: %v", err)
	}
	var event shardUploadWireEvent
	if err := json.Unmarshal(line, &event); err != nil {
		t.Fatalf("decode shard upload event %q: %v", line, err)
	}
	return event
}

// readShardUploadWireEvents reads n events, or all of them when n < 0, stopping early where the stream ends.
func readShardUploadWireEvents(t *testing.T, r *bufio.Reader, n int) []shardUploadWireEvent {
	t.Helper()
	var events []shardUploadWireEvent
	for ; n != 0; n-- {
		line, err := r.ReadBytes('\n')
		if err != nil {
			break
		}
		var event shardUploadWireEvent
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("decode shard upload event %q after %+v: %v", line, events, err)
		}
		events = append(events, event)
	}
	return events
}

func TestUploadShardV2StreamsWhileRequestBodyIsArriving(t *testing.T) {
	stor, body := newShardV2TestStorage(t, "GetFileIndexEntry", 2)
	stor.unblock()
	// Withhold the 48-byte CAS-section bookend so both CAS blocks arrive but Decode cannot finish.
	partialSize := len(body) - 48

	// Wrap as cmd/xetd does: ResponseController must unwrap the logging writer for HTTP/1 full duplex.
	httpSrv := httptest.NewServer(handlers.CombinedLoggingHandler(io.Discard, NewHandler(WithStorage(stor))))
	defer httpSrv.Close()

	conn, err := net.Dial("tcp", httpSrv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}

	if _, err := fmt.Fprintf(conn,
		"POST /v2/shards HTTP/1.1\r\nHost: %s\r\nContent-Type: application/octet-stream\r\nContent-Length: %d\r\nConnection: close\r\n\r\n",
		httpSrv.Listener.Addr().String(), len(body)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(body[:partialSize]); err != nil {
		t.Fatal(err)
	}

	connReader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(connReader, &http.Request{Method: http.MethodPost})
	if err != nil {
		t.Fatalf("read response before request completed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); got != "application/x-ndjson" {
		t.Fatalf("Content-Type = %q", got)
	}

	events := bufio.NewReader(resp.Body)
	if got, want := readShardUploadWireEvents(t, events, 3), []shardUploadWireEvent{
		{Type: "validating", Verified: 0, Total: 0},
		{Type: "validating", Verified: 0, Total: 3},
		{Type: "validating", Verified: 0, Total: 4},
	}; !slices.Equal(got, want) {
		t.Fatalf("events before request completed = %+v, want %+v", got, want)
	}
	select {
	case <-stor.started:
		t.Fatal("GetFileIndexEntry called before the complete request body arrived")
	default:
	}

	if _, err := conn.Write(body[partialSize:]); err != nil {
		t.Fatal(err)
	}
	if got, want := readShardUploadWireEvents(t, events, -1), []shardUploadWireEvent{
		{Type: "validating", Verified: 0, Total: 4},
		{Type: "validating", Verified: 1, Total: 4},
		{Type: "validating", Verified: 2, Total: 4},
		{Type: "validating", Verified: 3, Total: 4},
		{Type: "validating", Verified: 4, Total: 4},
		{Type: "committing", Stage: "uploading"},
		{Type: "committing", Stage: "syncing"},
		{Type: "result"},
	}; !slices.Equal(got, want) {
		t.Fatalf("events after request completed = %+v, want %+v", got, want)
	}
}

func TestUploadShardV2FlushesProgressBeforeStorageCalls(t *testing.T) {
	stream := []shardUploadWireEvent{
		{Type: "validating", Verified: 0, Total: 0},
		{Type: "validating", Verified: 0, Total: 2},
		{Type: "validating", Verified: 0, Total: 2},
		{Type: "validating", Verified: 1, Total: 2},
		{Type: "validating", Verified: 2, Total: 2},
		{Type: "committing", Stage: "uploading"},
		{Type: "committing", Stage: "syncing"},
		{Type: "result"},
	}
	for _, test := range []struct {
		gate    string
		flushed int
	}{
		{"GetXorbRangeReadCloser", 4},
		{"PutShardObject", 6},
		{"PutIndexEntry", 7},
	} {
		t.Run(test.gate, func(t *testing.T) {
			stor, body := newShardV2TestStorage(t, test.gate, 1)
			srv := httptest.NewServer(NewHandler(WithStorage(stor)))
			defer srv.Close()
			defer stor.unblock()

			resp, err := srv.Client().Post(srv.URL+"/v2/shards", "application/octet-stream", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			events := bufio.NewReader(resp.Body)
			select {
			case <-stor.started:
			case <-time.After(5 * time.Second):
				t.Fatalf("%s was not called", test.gate)
			}
			if got, want := readShardUploadWireEvents(t, events, test.flushed), stream[:test.flushed]; !slices.Equal(got, want) {
				t.Fatalf("events while %s is held = %+v, want %+v", test.gate, got, want)
			}
			stor.unblock()
			if got, want := readShardUploadWireEvents(t, events, -1), stream[test.flushed:]; !slices.Equal(got, want) {
				t.Fatalf("events after %s = %+v, want %+v", test.gate, got, want)
			}
		})
	}
}

func TestUploadShardV2ReportsStoredShardWithoutCommitting(t *testing.T) {
	stor, _, _, _, _, _, body := authorizerFixture(t)
	rec := httptest.NewRecorder()
	NewHandler(WithStorage(stor)).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v2/shards", bytes.NewReader(body)))
	if got, want := readShardUploadWireEvents(t, bufio.NewReader(rec.Body), -1), []shardUploadWireEvent{
		{Type: "validating", Verified: 0, Total: 0},
		{Type: "validating", Verified: 0, Total: 2},
		{Type: "result"},
	}; !slices.Equal(got, want) {
		t.Fatalf("events = %+v, want %+v", got, want)
	}
}

func TestUploadShardStorageFailureRemainsRetryable(t *testing.T) {
	for _, endpoint := range []string{"/shards", "/v1/shards", "/v2/shards"} {
		t.Run(endpoint, func(t *testing.T) {
			stor, body := newShardV2TestStorage(t, "GetFileIndexEntry", 1)
			stor.gateErr = fmt.Errorf("backend unavailable")
			stor.unblock()
			response := httptest.NewRecorder()
			NewHandler(WithStorage(stor)).ServeHTTP(response, httptest.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body)))
			if endpoint != "/v2/shards" {
				if response.Code != http.StatusInternalServerError || response.Body.String() != "failed to store shard\n" {
					t.Fatalf("response = %d %q", response.Code, response.Body.String())
				}
				return
			}
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", response.Code)
			}
			events := bufio.NewReader(response.Body)
			var last shardUploadWireEvent
			for _, err := events.Peek(1); err == nil; _, err = events.Peek(1) {
				last = readShardUploadWireEvent(t, events)
			}
			if last.Type != "error" || !last.Retryable || last.Message != "failed to store shard" {
				t.Fatalf("terminal event = %+v, want retryable storage error", last)
			}
		})
	}
}

func TestUploadShardV2ReportsDecodeFailureInStream(t *testing.T) {
	handler := NewHandler(WithStorage(&shardV2TestStorage{}))
	req := httptest.NewRequest(http.MethodPost, "/v2/shards", strings.NewReader("x"))
	req.ContentLength = 1
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	resp := rec.Result()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	events := bufio.NewReader(resp.Body)
	if got, want := readShardUploadWireEvent(t, events), (shardUploadWireEvent{Type: "validating"}); got != want {
		t.Fatalf("initial event = %+v, want %+v", got, want)
	}
	errorEvent := readShardUploadWireEvent(t, events)
	if errorEvent.Type != "error" || !strings.Contains(errorEvent.Message, "invalid shard format") || errorEvent.Retryable {
		t.Fatalf("error event = %+v", errorEvent)
	}
}
