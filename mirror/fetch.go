package mirror

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/wzshiming/xet/client/hf"
)

// fetchIdleTimeout cuts a fetch attempt that receives no upstream bytes for this long.
const fetchIdleTimeout = 60 * time.Second

// errFetchStalled reports an attempt cut by the idle watchdog.
var errFetchStalled = errors.New("upstream stalled")

// fetchXet downloads the file through the upstream xet CAS into the spool,
// resuming from the current spool offset on retries. Resolve and token
// handling reuse the hf package: the returned provider refreshes short-lived
// CAS tokens from the upstream's xet-auth endpoint, and term fetches resume
// dropped bodies via the client's built-in httpseek transport. The resolve
// itself runs inside the retry loop so a transient failure there does not
// fail the whole task.
func (m *Mirror) fetchXet(ctx context.Context, t *task, key string) error {
	return m.fetchWithRetries(ctx, "xet download", func(ctx context.Context, progress io.Writer) error {
		fileHash, provider, err := hf.ResolveDownload(ctx, m.probeClient, m.upstreamURL(key))
		if err != nil {
			return fmt.Errorf("resolve upstream xet download: %w", err)
		}
		return m.xetClient.DownloadFileWithAuthProvider(ctx, provider, fileHash, &progressSpool{spool: t.spool, progress: progress})
	})
}

// fetchPlain downloads the file bytes over plain HTTP into the spool, resuming
// from the current spool offset with Range requests on retries.
func (m *Mirror) fetchPlain(ctx context.Context, t *task, key string) error {
	return m.fetchWithRetries(ctx, "plain download", func(ctx context.Context, progress io.Writer) error {
		return m.fetchPlainOnce(ctx, t, key, progress)
	})
}

func (m *Mirror) fetchWithRetries(ctx context.Context, operation string, fetch func(context.Context, io.Writer) error) error {
	var lastErr error
	for attempt := range maxFetchAttempts {
		if err := sleepBackoff(ctx, attempt); err != nil {
			return err
		}
		lastErr = fetchAttempt(ctx, m.idleTimeout, fetch)
		if lastErr == nil {
			return nil
		}
	}
	return fmt.Errorf("%s failed after %d attempts: %w", operation, maxFetchAttempts, lastErr)
}

// fetchAttempt cuts the attempt after idle without a progress write; flow of any speed re-arms it.
func fetchAttempt(ctx context.Context, idle time.Duration, fetch func(context.Context, io.Writer) error) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	w := &idleTimer{idle: idle}
	w.timer = time.AfterFunc(idle, func() { cancel(errFetchStalled) })
	defer w.timer.Stop()
	err := fetch(ctx, w)
	if err != nil && context.Cause(ctx) == errFetchStalled {
		return fmt.Errorf("%w: no data for %s", errFetchStalled, idle)
	}
	return err
}

// idleTimer is the attempt's progress writer: each write re-arms the stall timer.
type idleTimer struct {
	timer *time.Timer
	idle  time.Duration
}

func (w *idleTimer) Write(p []byte) (int, error) {
	w.timer.Reset(w.idle)
	return len(p), nil
}

// progressSpool reports the bytes landing in the spool to the attempt's progress writer.
type progressSpool struct {
	*spool
	progress io.Writer
}

func (s *progressSpool) Write(p []byte) (int, error) {
	n, err := s.spool.Write(p)
	if n > 0 {
		_, _ = s.progress.Write(p[:n])
	}
	return n, err
}

func (m *Mirror) fetchPlainOnce(ctx context.Context, t *task, key string, progress io.Writer) error {
	offset := t.spool.size()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.upstreamURL(key), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept-Encoding", "identity")
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}

	resp, err := m.fetchClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body := io.TeeReader(resp.Body, progress)
	switch {
	case offset == 0 && resp.StatusCode == http.StatusOK:
		if resp.ContentLength >= 0 {
			t.setSize(resp.ContentLength)
		}
	case offset > 0 && resp.StatusCode == http.StatusPartialContent:
		if total := parseContentRangeTotal(resp.Header.Get("Content-Range")); total >= 0 {
			t.setSize(total)
		}
	case offset > 0 && resp.StatusCode == http.StatusOK:
		// Upstream ignored the Range; skip what the spool already holds.
		if _, err := io.CopyN(io.Discard, body, offset); err != nil {
			return fmt.Errorf("skip resumed bytes: %w", err)
		}
	default:
		return fmt.Errorf("upstream fetch status %d", resp.StatusCode)
	}

	_, err = io.Copy(t.spool, body)
	return err
}

func parseContentRangeTotal(value string) int64 {
	// Format: bytes <start>-<end>/<total>
	idx := strings.LastIndexByte(value, '/')
	if idx < 0 {
		return -1
	}
	total, err := strconv.ParseInt(value[idx+1:], 10, 64)
	if err != nil {
		return -1
	}
	return total
}

func sleepBackoff(ctx context.Context, attempt int) error {
	if attempt == 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
		return nil
	}
}
