package mirror

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// fetchXet spools the file over the upstream xet CAS; every attempt re-resolves with the task's pinned hub token.
func (m *Mirror) fetchXet(ctx context.Context, t *task, src resolveKey) error {
	return fetchWithRetries(ctx, "xet download", func() error {
		f, err := m.xetClient.Resolve(ctx, t.origin+src.String(), t.token)
		if err != nil {
			return fmt.Errorf("resolve upstream xet download: %w", err)
		}
		return m.xetClient.DownloadResolved(ctx, f, t.spool)
	})
}

// fetchPlain downloads the file bytes over plain HTTP into the spool, resuming
// from the current spool offset with Range requests on retries.
func (m *Mirror) fetchPlain(ctx context.Context, t *task, src resolveKey) error {
	return fetchWithRetries(ctx, "plain download", func() error {
		return m.fetchPlainOnce(ctx, t, src)
	})
}

func fetchWithRetries(ctx context.Context, operation string, fetch func() error) error {
	var lastErr error
	for attempt := range maxFetchAttempts {
		if err := sleepBackoff(ctx, attempt); err != nil {
			return err
		}
		lastErr = fetch()
		if lastErr == nil {
			return nil
		}
	}
	return fmt.Errorf("%s failed after %d attempts: %w", operation, maxFetchAttempts, lastErr)
}

func (m *Mirror) fetchPlainOnce(ctx context.Context, t *task, src resolveKey) error {
	offset := t.spool.size()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.origin+src.String(), nil)
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

	body := io.Reader(resp.Body)
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
