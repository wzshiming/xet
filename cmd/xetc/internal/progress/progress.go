package progress

import (
	"fmt"
	"io"
	"sync"
	"time"
)

const defaultRateWindow = time.Second

type speedSample struct {
	at    time.Time
	bytes int64
}

type speedTracker struct {
	window  time.Duration
	samples []speedSample
}

func newSpeedTracker(window time.Duration) *speedTracker {
	if window <= 0 {
		window = time.Second
	}
	return &speedTracker{window: window}
}

func (s *speedTracker) Update(totalBytes int64, now time.Time) int64 {
	if totalBytes < 0 {
		totalBytes = 0
	}

	s.samples = append(s.samples, speedSample{at: now, bytes: totalBytes})
	cutoff := now.Add(-s.window)
	trim := 0
	for trim < len(s.samples)-1 && s.samples[trim].at.Before(cutoff) {
		trim++
	}
	if trim > 0 {
		s.samples = append([]speedSample(nil), s.samples[trim:]...)
	}

	if len(s.samples) < 2 {
		return 0
	}

	first := s.samples[0]
	last := s.samples[len(s.samples)-1]
	duration := last.at.Sub(first.at)
	if duration <= 0 || last.bytes <= first.bytes {
		return 0
	}

	return int64(float64(last.bytes-first.bytes) / duration.Seconds())
}

type Formatter func(current, total int64, bytesPerSecond int64) string

type Writer struct {
	out       io.Writer
	format    Formatter
	tracker   *speedTracker
	mu        sync.Mutex
	current   int64
	total     int64
	hasSample bool
}

func NewWriter(out io.Writer, format Formatter) *Writer {
	if out == nil || format == nil {
		return nil
	}
	return &Writer{
		out:     out,
		format:  format,
		tracker: newSpeedTracker(defaultRateWindow),
	}
}

func (w *Writer) Callback(current, total int64) {
	if w == nil {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.current = current
	w.total = total
	w.hasSample = true
	rate := w.tracker.Update(current, time.Now())
	_, _ = fmt.Fprintf(w.out, "\r%s", w.format(current, total, rate))
}

func (w *Writer) Finish() error {
	if w == nil {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := fmt.Fprint(w.out, "\r"); err != nil {
		return err
	}

	if !w.hasSample {
		if _, err := fmt.Fprintln(w.out, w.format(0, 0, 0)); err != nil {
			return err
		}
		return nil
	}

	rate := w.tracker.Update(w.current, time.Now())
	_, err := fmt.Fprintf(w.out, "%s\n", w.format(w.current, w.total, rate))
	return err
}

func FormatDownload(current, total int64, bytesPerSecond int64) string {
	rate := formatTransferRate(bytesPerSecond)
	if total > 0 {
		percent := float64(current) * 100 / float64(total)
		if percent > 100 {
			percent = 100
		}
		return fmt.Sprintf("Downloading... %.1f%% (%s / %s, %s)", percent, formatByteSize(current), formatByteSize(total), rate)
	}
	return fmt.Sprintf("Downloading... %s (%s)", formatByteSize(current), rate)
}

func FormatUpload(current, total int64, bytesPerSecond int64) string {
	rate := formatTransferRate(bytesPerSecond)
	if total > 0 {
		percent := float64(current) * 100 / float64(total)
		if percent > 100 {
			percent = 100
		}
		return fmt.Sprintf("Uploading... %.1f%% (%s / %s, %s)", percent, formatByteSize(current), formatByteSize(total), rate)
	}
	return fmt.Sprintf("Uploading... %s (%s)", formatByteSize(current), rate)
}

func formatTransferRate(bytesPerSecond int64) string {
	if bytesPerSecond <= 0 {
		return "0 B/s"
	}
	return formatByteSize(bytesPerSecond) + "/s"
}

func formatByteSize(size int64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}

	value := float64(size)
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	for _, name := range units {
		value /= unit
		if value < unit {
			return fmt.Sprintf("%.1f %s", value, name)
		}
	}

	return fmt.Sprintf("%.1f PiB", value/unit)
}
