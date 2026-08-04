package common

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/wzshiming/xet"
	xetcas "github.com/wzshiming/xet/cas"
	"github.com/wzshiming/xet/client"
)

const (
	DefaultHFCASURL   = "https://cas-server.xethub.hf.co"
	DefaultHFEndpoint = "https://huggingface.co"
)

func baseName(p string) string {
	idx := strings.Index(p, "?")
	if idx >= 0 {
		p = p[:idx]
	}
	idx = strings.LastIndex(p, "/")
	if idx >= 0 {
		return p[idx+1:]
	}
	return p
}

func ExecuteUpload(ctx context.Context, filename string, provider client.AuthProvider, namespace string, concurrency int, out io.Writer) (err error) {
	if _, err := fmt.Fprintf(out, "%s Uploading file\n", filename); err != nil {
		return err
	}

	progressSummary := newProgressSummary()
	fileHash, err := xetcas.Upload(ctx, filename, provider, namespace, concurrency, func(name string, current, total int64) {
		progressSummary.Update(baseName(name), current, total)
		progressSummary.Output(out)
	})
	if err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}

	if _, err := fmt.Fprintln(out, "Upload complete!"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "File hash: %s\n", fileHash.String()); err != nil {
		return err
	}

	return nil
}

func ExecuteDownload(ctx context.Context, fileHash xet.FileHash, outputFile string, provider client.AuthProvider, namespace string, concurrency int, resume bool, out io.Writer) (err error) {
	progressSummary := newProgressSummary()
	err = xetcas.Download(ctx, fileHash, outputFile, provider, namespace, concurrency, resume, func(name string, current, total int64) {
		progressSummary.Update(baseName(name), current, total)
		progressSummary.Output(out)
	})
	if err != nil {
		return err
	}

	var total int64
	if stat, statErr := os.Stat(outputFile); statErr == nil && stat.Mode().IsRegular() {
		total = stat.Size()
	}

	if _, err := fmt.Fprintf(out, "Download complete! (%d bytes)\r", total); err != nil {
		return err
	}
	return nil
}

type progressItem struct {
	current int64
	total   int64
}

type progressSummary struct {
	mut   sync.Mutex
	items map[string]progressItem

	previousCurrent int64
	latestUpdate    time.Time
}

func newProgressSummary() *progressSummary {
	return &progressSummary{}
}

func (s *progressSummary) Update(name string, current, total int64) {
	s.mut.Lock()
	defer s.mut.Unlock()
	if s.items == nil {
		s.items = make(map[string]progressItem)
	}
	s.items[name] = progressItem{current: current, total: total}
}

func (s *progressSummary) Output(out io.Writer) {

	s.mut.Lock()
	defer s.mut.Unlock()
	if s.items == nil {
		return
	}

	now := time.Now()
	if now.Sub(s.latestUpdate) < time.Second {
		return
	}

	var current int64
	var total int64
	for _, item := range s.items {
		current += item.current
		total += item.total
	}

	bytesSinceLast := current - s.previousCurrent
	elapsed := now.Sub(s.latestUpdate)
	s.previousCurrent = current
	s.latestUpdate = now

	rate := formatTransferRate(bytesSinceLast * int64(time.Second) / int64(elapsed))

	if _, err := fmt.Fprintf(out, "%s/%s (%s)\r", formatByteSize(current), formatByteSize(total), rate); err != nil {
		return
	}
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
