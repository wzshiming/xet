package server

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/handlers"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/memory"
	"github.com/wzshiming/xet/storage/storagetest"
)

// shardV2TestStorage holds the nth gate call (default: the first PutShard) until putContinue closes.
type shardV2TestStorage struct {
	storage.Storage
	gate        string
	nth         int
	seen        int
	putStarted  chan struct{}
	putContinue chan struct{}
	putErr      error
}

func (s *shardV2TestStorage) hold(method string) error {
	if method != cmp.Or(s.gate, "PutShard") {
		return nil
	}
	if s.seen++; s.seen != max(s.nth, 1) {
		return nil
	}
	close(s.putStarted)
	<-s.putContinue
	return s.putErr
}

func (s *shardV2TestStorage) release() {
	select {
	case <-s.putContinue:
	default:
		close(s.putContinue)
	}
}

func (s *shardV2TestStorage) HasXorb(context.Context, xet.XorbHash) (bool, error) {
	if err := s.hold("HasXorb"); err != nil {
		return false, err
	}
	return true, nil
}

func (s *shardV2TestStorage) GetFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (string, error) {
	if err := s.hold("GetFileIndexEntry"); err != nil {
		return "", err
	}
	return s.Storage.GetFileIndexEntry(ctx, fileHash)
}

func (s *shardV2TestStorage) GetXorbReadSeekCloser(ctx context.Context, xorbHash xet.XorbHash) (io.ReadSeekCloser, error) {
	if err := s.hold("GetXorbReadSeekCloser"); err != nil {
		return nil, err
	}
	return s.Storage.GetXorbReadSeekCloser(ctx, xorbHash)
}

func (s *shardV2TestStorage) PutShard(ctx context.Context, sh *shard.Shard) (string, bool, error) {
	if err := s.hold("PutShard"); err != nil {
		return "", false, err
	}
	return s.Storage.PutShard(ctx, sh)
}

func (s *shardV2TestStorage) PutChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash, shardHash string) (bool, error) {
	if err := s.hold("PutChunkIndexEntry"); err != nil {
		return false, err
	}
	return s.Storage.PutChunkIndexEntry(ctx, chunkHash, shardHash)
}

// uncommittedFixture returns a memory store of two xorbs and the uncommitted shard of a file with a term in each.
func uncommittedFixture(t *testing.T) (storage.Storage, []byte) {
	t.Helper()
	stor := memory.NewStorage()
	shardObj := shard.NewShard()
	storagetest.AddFileBlock(t, t.Context(), stor, shardObj, [][]byte{[]byte("first term of the upload"), []byte("second term of the upload")})
	// AddFileBlock leaves NumBytesInCAS zero, which Validate rejects.
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
	return stor, body
}

// uncommittedFixtureEvents is the stream of a first upload of uncommittedFixture's shard.
var uncommittedFixtureEvents = []shardUploadWireEvent{
	{Type: "validating", Verified: 0, Total: 0},
	{Type: "validating", Verified: 0, Total: 3},
	{Type: "validating", Verified: 0, Total: 4},
	{Type: "validating", Verified: 0, Total: 4},
	{Type: "validating", Verified: 1, Total: 4},
	{Type: "validating", Verified: 2, Total: 4},
	{Type: "validating", Verified: 3, Total: 4},
	{Type: "validating", Verified: 4, Total: 4},
	{Type: "committing", Stage: "uploading"},
	{Type: "committing", Stage: "syncing"},
	{Type: "result"},
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

func readShardUploadWireEvents(t *testing.T, r *bufio.Reader) []shardUploadWireEvent {
	t.Helper()
	var events []shardUploadWireEvent
	for _, err := r.Peek(1); err == nil; _, err = r.Peek(1) {
		events = append(events, readShardUploadWireEvent(t, r))
	}
	return events
}

func TestUploadShardV2StreamsWhileRequestBodyIsArriving(t *testing.T) {
	target, body := uncommittedFixture(t)
	// Withhold the 48-byte CAS-section bookend: both CAS blocks decode, the shard cannot.
	partialSize := len(body) - 48

	stor := &shardV2TestStorage{
		Storage:     target,
		gate:        "GetFileIndexEntry",
		putStarted:  make(chan struct{}),
		putContinue: make(chan struct{}),
	}
	stor.release()
	// Exercise the same logging wrapper used by cmd/xetd; ResponseController
	// must unwrap it to enable full duplex on the underlying HTTP/1 writer.
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
	for i, want := range uncommittedFixtureEvents[:3] {
		if got := readShardUploadWireEvent(t, events); got != want {
			t.Fatalf("event %d before request completed = %+v, want %+v", i, got, want)
		}
	}
	select {
	case <-stor.putStarted:
		t.Fatal("PutShard started before the complete request body arrived")
	default:
	}

	if _, err := conn.Write(body[partialSize:]); err != nil {
		t.Fatal(err)
	}
	if got, want := readShardUploadWireEvents(t, events), uncommittedFixtureEvents[3:]; !slices.Equal(got, want) {
		t.Fatalf("events after request completed = %+v, want %+v", got, want)
	}
}

func TestUploadShardV2FlushesEachPhaseBeforeItsStorageCall(t *testing.T) {
	for _, test := range []struct {
		gate    string
		nth     int
		flushed int
	}{
		{"HasXorb", 1, 2},               // the first CAS block's xorb probe
		{"GetXorbReadSeekCloser", 2, 5}, // the second verification unit
		{"PutShard", 1, 9},
		{"PutChunkIndexEntry", 1, 10},
	} {
		t.Run(test.gate, func(t *testing.T) {
			target, body := uncommittedFixture(t)
			stor := &shardV2TestStorage{Storage: target, gate: test.gate, nth: test.nth, putStarted: make(chan struct{}), putContinue: make(chan struct{})}
			rec := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				defer close(done)
				NewHandler(WithStorage(stor)).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v2/shards", bytes.NewReader(body)))
			}()
			defer func() {
				stor.release()
				<-done
			}()
			select {
			case <-stor.putStarted:
			case <-time.After(5 * time.Second):
				t.Fatalf("call %d of %s was not made", test.nth, test.gate)
			}

			// The handler waits inside the held call, so rec has exactly the frames flushed before it.
			if got, want := readShardUploadWireEvents(t, bufio.NewReader(bytes.NewReader(rec.Body.Bytes()))), uncommittedFixtureEvents[:test.flushed]; !slices.Equal(got, want) {
				t.Fatalf("events while call %d of %s is held = %+v, want %+v", test.nth, test.gate, got, want)
			}
			stor.release()
			<-done
			if got := readShardUploadWireEvents(t, bufio.NewReader(rec.Body)); !slices.Equal(got, uncommittedFixtureEvents) {
				t.Fatalf("events = %+v, want %+v", got, uncommittedFixtureEvents)
			}
		})
	}
}

func TestUploadShardV2ReportsKnownShardWithoutCommitting(t *testing.T) {
	stor, body := uncommittedFixture(t)
	upload := func() []shardUploadWireEvent {
		rec := httptest.NewRecorder()
		NewHandler(WithStorage(stor)).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v2/shards", bytes.NewReader(body)))
		return readShardUploadWireEvents(t, bufio.NewReader(rec.Body))
	}
	if first := upload(); len(first) == 0 || first[len(first)-1].Type != "result" {
		t.Fatalf("first upload events = %+v, want a final result", first)
	}

	events := upload()
	if slices.ContainsFunc(events, func(e shardUploadWireEvent) bool { return e.Type == "committing" }) {
		t.Fatalf("repeated upload events = %+v, want no committing", events)
	}
	if n := len(events); n == 0 || events[n-1].Type != "result" {
		t.Fatalf("repeated upload events = %+v, want a final result", events)
	}
}

func TestUploadShardStorageFailureRemainsRetryable(t *testing.T) {
	for _, endpoint := range []string{"/shards", "/v1/shards", "/v2/shards"} {
		t.Run(endpoint, func(t *testing.T) {
			target, body := uncommittedFixture(t)
			stor := &shardV2TestStorage{
				Storage:     target,
				putStarted:  make(chan struct{}),
				putContinue: make(chan struct{}),
				putErr:      fmt.Errorf("backend unavailable"),
			}
			close(stor.putContinue)
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
