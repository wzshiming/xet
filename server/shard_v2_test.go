package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/handlers"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
)

type shardV2TestStorage struct {
	storage.Storage
	putStarted  chan struct{}
	putContinue chan struct{}
}

func (s *shardV2TestStorage) HasXorb(context.Context, string, xet.XorbHash) (bool, error) {
	return true, nil
}

func (s *shardV2TestStorage) PutShard(context.Context, *shard.Shard) (bool, error) {
	close(s.putStarted)
	<-s.putContinue
	return true, nil
}

func (s *shardV2TestStorage) PinFile(context.Context, string, xet.FileHash) error {
	return nil
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

func TestUploadShardV2StreamsWhileRequestBodyIsArriving(t *testing.T) {
	shardObj := shard.NewShard()
	shardObj.AddCASBlock(shard.CASBlock{})
	encoded, err := shardObj.Encode(false)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(encoded)
	if err != nil {
		t.Fatal(err)
	}
	// Leave the final CAS-section bookend unsent. The first CAS block is
	// complete, but Decode cannot finish until these last 48 bytes arrive.
	partialSize := len(body) - 48

	stor := &shardV2TestStorage{
		putStarted:  make(chan struct{}),
		putContinue: make(chan struct{}),
	}
	// Exercise the same logging wrapper used by cmd/xetd; ResponseController
	// must unwrap it to enable full duplex on the underlying HTTP/1 writer.
	httpSrv := httptest.NewServer(handlers.CombinedLoggingHandler(io.Discard, NewHandler(WithStorage(stor))))
	defer httpSrv.Close()
	defer func() {
		select {
		case <-stor.putContinue:
		default:
			close(stor.putContinue)
		}
	}()

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
	for i, want := range []shardUploadWireEvent{
		{Type: "validating", Verified: 0, Total: 0},
		{Type: "validating", Verified: 0, Total: 1},
		{Type: "validating", Verified: 1, Total: 1},
	} {
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
	if got, want := readShardUploadWireEvent(t, events), (shardUploadWireEvent{Type: "committing", Stage: "uploading"}); got != want {
		t.Fatalf("commit event = %+v, want %+v", got, want)
	}
	select {
	case <-stor.putStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("PutShard did not start")
	}
	close(stor.putContinue)

	for i, want := range []shardUploadWireEvent{
		{Type: "committing", Stage: "syncing"},
		{Type: "result"},
	} {
		if got := readShardUploadWireEvent(t, events); got != want {
			t.Fatalf("terminal event %d = %+v, want %+v", i, got, want)
		}
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
