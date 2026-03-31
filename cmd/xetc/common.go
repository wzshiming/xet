package main

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
)

const (
	defaultHFCASURL   = "https://cas-server.xethub.hf.co"
	defaultHFEndpoint = "https://huggingface.co"
	instantRateWindow = time.Second
)

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

func executeUpload(ctx context.Context, filename, baseURL, token, namespace string, concurrency int, out io.Writer) (err error) {
	if baseURL == "" {
		return fmt.Errorf("--url is required")
	}

	f, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("open input file: %w", err)
	}
	defer func() {
		closeErr := f.Close()
		if err == nil && closeErr != nil {
			err = fmt.Errorf("close input file: %w", closeErr)
		}
	}()

	cli := client.NewClient(
		client.WithBaseURL(baseURL),
		client.WithToken(token),
		client.WithNamespace(namespace),
	)

	session := cli.UploadSession().WithConcurrency(concurrency)
	speed := newSpeedTracker(instantRateWindow)
	var totalBytes int64
	if info, statErr := f.Stat(); statErr == nil {
		totalBytes = info.Size()
	}

	var progressMu sync.Mutex
	var latestProgress client.Progress
	if out != nil {
		session.WithProgress(func(progress client.Progress) {
			progressMu.Lock()
			progress.TotalBytes = totalBytes
			latestProgress = progress
			rate := speed.Update(progress.TransferredBytes, time.Now())
			_, _ = fmt.Fprintf(out, "\r%s", formatUploadProgress(progress, rate))
			progressMu.Unlock()
		})
	}

	if _, err := fmt.Fprintf(out, "%s Uploading file\n", filename); err != nil {
		return err
	}
	fileHashes, err := session.UploadFiles(ctx, f)
	if out != nil {
		progressMu.Lock()
		if _, writeErr := fmt.Fprint(out, "\r"); err == nil && writeErr != nil {
			err = writeErr
		}
		latestProgress.TotalBytes = totalBytes
		rate := speed.Update(latestProgress.TransferredBytes, time.Now())
		if _, writeErr := fmt.Fprintf(out, "%s\n", formatUploadProgress(latestProgress, rate)); err == nil && writeErr != nil {
			err = writeErr
		}
		progressMu.Unlock()
	}
	if err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}

	if _, err := fmt.Fprintln(out, "✓ Upload complete!"); err != nil {
		return err
	}
	for _, hash := range fileHashes {
		if _, err := fmt.Fprintf(out, "File hash: %s\n", hash.String()); err != nil {
			return err
		}
	}

	return nil
}

func executeDownload(ctx context.Context, fileHash xet.Hash, outputFile, baseURL, token, namespace string, concurrency int, resume bool, out io.Writer) (err error) {
	if baseURL == "" {
		return fmt.Errorf("--url is required")
	}

	// Detect partial download for resume
	var resumeOffset int64
	if resume {
		if stat, statErr := os.Stat(outputFile); statErr == nil && stat.Mode().IsRegular() {
			resumeOffset = stat.Size()
		}
	}

	cli := client.NewClient(
		client.WithBaseURL(baseURL),
		client.WithToken(token),
		client.WithNamespace(namespace),
	)
	session := cli.DownloadSession().WithConcurrency(concurrency)
	speed := newSpeedTracker(instantRateWindow)

	var progressMu sync.Mutex
	var latestProgress client.Progress
	if out != nil {
		session.WithProgress(func(progress client.Progress) {
			progressMu.Lock()
			// Adjust byte counts so progress reflects the full file, not just the remaining portion
			progress.BytesRead += resumeOffset
			progress.TotalBytes += resumeOffset
			latestProgress = progress
			rate := speed.Update(progress.TransferredBytes, time.Now())
			_, _ = fmt.Fprintf(out, "\r%s", formatDownloadProgress(progress, rate))
			progressMu.Unlock()
		})
	}

	var downloadOpts []client.ReqOpt
	if resumeOffset > 0 {
		downloadOpts = append(downloadOpts, client.WithRangeStart(resumeOffset))
	}

	reader, expectedLength, err := session.DownloadFile(ctx, fileHash, downloadOpts...)
	if err != nil {
		if resumeOffset > 0 {
			// Range request failed (server may not support it); fall back to full download
			resumeOffset = 0
			reader, expectedLength, err = session.DownloadFile(ctx, fileHash)
		}
		if err != nil {
			return fmt.Errorf("download failed: %w", err)
		}
	}

	// File is already fully downloaded
	if expectedLength == 0 && resumeOffset > 0 {
		if out != nil {
			if _, writeErr := fmt.Fprintf(out, "✓ File already complete! (%d bytes)\n", resumeOffset); writeErr != nil {
				return writeErr
			}
			if _, writeErr := fmt.Fprintf(out, "Saved to: %s\n", outputFile); writeErr != nil {
				return writeErr
			}
		}
		return nil
	}

	// Open output file: append if resuming, create otherwise
	var file *os.File
	if resumeOffset > 0 {
		file, err = os.OpenFile(outputFile, os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("open output file for append: %w", err)
		}
	} else {
		file, err = os.Create(outputFile)
		if err != nil {
			return fmt.Errorf("create output file: %w", err)
		}
	}
	defer func() {
		closeErr := file.Close()
		if err == nil && closeErr != nil {
			err = fmt.Errorf("close output file: %w", closeErr)
		}
	}()

	n, err := io.Copy(file, reader)
	if out != nil {
		progressMu.Lock()
		if _, writeErr := fmt.Fprint(out, "\r"); err == nil && writeErr != nil {
			err = writeErr
		}
		latestProgress.BytesRead = resumeOffset + n
		latestProgress.TotalBytes = resumeOffset + expectedLength
		rate := speed.Update(latestProgress.TransferredBytes, time.Now())
		if _, writeErr := fmt.Fprintf(out, "%s\n", formatDownloadProgress(latestProgress, rate)); err == nil && writeErr != nil {
			err = writeErr
		}
		progressMu.Unlock()
	}
	if err != nil {
		return fmt.Errorf("write output file: %w", err)
	}

	total := resumeOffset + n
	if _, err := fmt.Fprintf(out, "✓ Download complete! (%d bytes)\n", total); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "Saved to: %s\n", outputFile); err != nil {
		return err
	}
	return nil
}

func formatDownloadProgress(progress client.Progress, bytesPerSecond int64) string {
	current := progress.BytesRead
	total := progress.TotalBytes
	fetched := progress.TransferredBytes
	rate := formatTransferRate(bytesPerSecond)
	ratio := formatCompressionRatio(current, fetched)
	if total > 0 {
		percent := float64(current) * 100 / float64(total)
		if percent > 100 {
			percent = 100
		}
		return fmt.Sprintf("Downloading... %.1f%% (%s/%s, fetched %s, %s, ratio %s)", percent, formatByteSize(current), formatByteSize(total), formatByteSize(fetched), rate, ratio)
	}

	return fmt.Sprintf("Downloading... %s (fetched %s, %s, ratio %s)", formatByteSize(current), formatByteSize(fetched), rate, ratio)
}

func formatUploadProgress(progress client.Progress, bytesPerSecond int64) string {
	current := progress.BytesRead
	total := progress.TotalBytes
	fetched := progress.TransferredBytes
	rate := formatTransferRate(bytesPerSecond)
	ratio := formatCompressionRatio(current, fetched)
	if progress.TotalBytes > 0 {
		percent := float64(current) * 100 / float64(total)
		if percent > 100 {
			percent = 100
		}
		return fmt.Sprintf("Uploading... %.1f%% (%s/%s, sent %s, %s, ratio %s)", percent, formatByteSize(current), formatByteSize(total), formatByteSize(fetched), rate, ratio)
	}
	return fmt.Sprintf("Uploading... read %s, sent %s, %s, ratio %s", formatByteSize(current), formatByteSize(fetched), rate, ratio)
}

func formatTransferRate(bytesPerSecond int64) string {
	if bytesPerSecond <= 0 {
		return "0 B/s"
	}
	return formatByteSize(bytesPerSecond) + "/s"
}

func formatCompressionRatio(logicalBytes, transferBytes int64) string {
	if logicalBytes <= 0 || transferBytes <= 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.2fx", float64(logicalBytes)/float64(transferBytes))
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

func isURL(str string) bool {
	u, err := url.Parse(str)
	if err != nil {
		return false
	}
	return u.Scheme != "" && u.Host != ""
}

func normalizeArgs(args []string) []string {
	if len(args) < 2 {
		return args
	}

	switch args[0] {
	case "upload":
		if !isUploadMode(args[1]) {
			return append([]string{"upload", "cas"}, args[1:]...)
		}
	case "download":
		if !isDownloadMode(args[1]) {
			mode := "cas"
			if isURL(args[1]) {
				mode = "resolve"
			}
			return append([]string{"download", mode}, args[1:]...)
		}
	}

	return args
}

func isUploadMode(arg string) bool {
	switch arg {
	case "cas", "hf", "help", "--help", "-h":
		return true
	default:
		return false
	}
}

func isDownloadMode(arg string) bool {
	switch arg {
	case "cas", "hf", "resolve", "help", "--help", "-h":
		return true
	default:
		return false
	}
}
