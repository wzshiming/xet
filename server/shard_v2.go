package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/wzshiming/xet/shard"
)

const shardUploadHeartbeatInterval = 20 * time.Second

type shardUploadValidatingEvent struct {
	Type     string `json:"type"`
	Verified uint64 `json:"verified"`
	Total    uint64 `json:"total"`
}

type shardUploadCommittingEvent struct {
	Type  string `json:"type"`
	Stage string `json:"stage"`
}

type shardUploadResultEvent struct {
	Type string `json:"type"`
}

type shardUploadErrorEvent struct {
	Type      string `json:"type"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

// shardUploadStream serializes response writes because heartbeat frames may be
// emitted while the handler is blocked reading, validating, or committing.
type shardUploadStream struct {
	w          http.ResponseWriter
	controller *http.ResponseController

	mu   sync.Mutex
	last []byte
}

func newShardUploadStream(w http.ResponseWriter) *shardUploadStream {
	return &shardUploadStream{
		w:          w,
		controller: http.NewResponseController(w),
	}
}

func (s *shardUploadStream) send(event any) error {
	frame, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode shard upload event: %w", err)
	}
	frame = append(frame, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.w.Write(frame); err != nil {
		return err
	}
	s.last = append(s.last[:0], frame...)
	return s.controller.Flush()
}

func (s *shardUploadStream) validating(verified, total uint64) error {
	return s.send(shardUploadValidatingEvent{
		Type:     "validating",
		Verified: verified,
		Total:    total,
	})
}

func (s *shardUploadStream) committing(stage string) error {
	return s.send(shardUploadCommittingEvent{Type: "committing", Stage: stage})
}

func (s *shardUploadStream) result() error {
	return s.send(shardUploadResultEvent{Type: "result"})
}

func (s *shardUploadStream) uploadError(message string, retryable bool) error {
	return s.send(shardUploadErrorEvent{
		Type:      "error",
		Message:   message,
		Retryable: retryable,
	})
}

// startHeartbeat repeats the latest progress frame while a processing stage is
// quiet. This keeps the normal xet-core read timeout from expiring during long
// validation and storage operations.
func (s *shardUploadStream) startHeartbeat(interval time.Duration) func() {
	done := make(chan struct{})
	finished := make(chan struct{})
	var stopOnce sync.Once

	go func() {
		defer close(finished)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.mu.Lock()
				if len(s.last) != 0 {
					if _, err := s.w.Write(s.last); err == nil {
						_ = s.controller.Flush()
					}
				}
				s.mu.Unlock()
			case <-done:
				return
			}
		}
	}()

	return func() {
		stopOnce.Do(func() { close(done) })
		<-finished
	}
}

// handleUploadShardV2 handles POST /v2/shards as a full-duplex NDJSON stream.
// Progress starts before the request body has finished arriving, CAS references
// are validated as their blocks are decoded, and every post-stream failure is
// reported by a terminal error frame.
func (s *Handler) handleUploadShardV2(w http.ResponseWriter, r *http.Request) {
	if !s.authenticate(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if r.ContentLength <= 0 {
		http.Error(w, "Content-Length header required", http.StatusLengthRequired)
		return
	}

	controller := http.NewResponseController(w)
	// HTTP/2 is full duplex already. On HTTP/1 this opts out of net/http's
	// default behavior of consuming the full request before flushing a response.
	// Some synthetic ResponseWriters do not implement it but do not need it.
	_ = controller.EnableFullDuplex()

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	stream := newShardUploadStream(w)
	if err := stream.validating(0, 0); err != nil {
		return
	}
	stopHeartbeat := stream.startHeartbeat(shardUploadHeartbeatInterval)
	defer stopHeartbeat()

	finishWithError := func(message string, retryable bool) {
		stopHeartbeat()
		_ = stream.uploadError(message, retryable)
	}

	body := io.LimitReader(r.Body, r.ContentLength)
	shardObj := shard.NewShard()
	var verified, total uint64
	var callbackMessage string
	var callbackRetryable bool
	if err := shardObj.DecodeWithCASBlockCallback(body, false, func(casBlock shard.CASBlock) error {
		total++
		if err := stream.validating(verified, total); err != nil {
			return err
		}

		exists, err := s.storage.HasXorb(r.Context(), "default", casBlock.CASHash)
		if err != nil {
			callbackMessage = "failed to check referenced xorb"
			callbackRetryable = true
			return errors.New(callbackMessage)
		}
		if !exists {
			callbackMessage = "invalid shard: referenced xorb not uploaded"
			return errors.New(callbackMessage)
		}

		verified++
		if err := stream.validating(verified, total); err != nil {
			return err
		}
		return nil
	}); err != nil {
		if callbackMessage != "" {
			finishWithError(callbackMessage, callbackRetryable)
			return
		}
		finishWithError(fmt.Sprintf("invalid shard format: %v", err), false)
		return
	}

	if err := shardObj.Validate(); err != nil {
		finishWithError(fmt.Sprintf("invalid shard: %v", err), false)
		return
	}

	if err := stream.committing("uploading"); err != nil {
		return
	}
	if _, err := s.storage.PutShard(r.Context(), shardObj); err != nil {
		finishWithError("failed to store shard", true)
		return
	}
	// Direct uploads are pinned as permanent GC roots; pinning also when the
	// shard already existed covers re-uploads of content the mirror ingested.
	for _, fileBlock := range shardObj.Files {
		if err := s.storage.PinFile(r.Context(), "default", fileBlock.FileHash); err != nil {
			finishWithError("failed to pin uploaded file", true)
			return
		}
	}
	if err := stream.committing("syncing"); err != nil {
		return
	}

	stopHeartbeat()
	_ = stream.result()
}
