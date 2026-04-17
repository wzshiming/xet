package cas

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/progress"
)

// Download downloads a file from CAS and saves it to the specified output path. If resume is true and the output file already exists, it will attempt to resume the download from where it left off.
func Download(ctx context.Context, fileHash xet.Hash, outputFile string, provider client.AuthProvider, namespace string, concurrency int, resume bool, progressFunc progress.ProgressFunc) (err error) {
	var resumeOffset int64
	if resume {
		if stat, statErr := os.Stat(outputFile); statErr == nil && stat.Mode().IsRegular() {
			resumeOffset = stat.Size()
		}
	}

	cli := client.NewClient(
		client.WithAuthProvider(provider),
		client.WithNamespace(namespace),
		client.WithProgressFunc(progressFunc),
		client.WithConcurrency(concurrency),
	)

	var header http.Header
	if resumeOffset > 0 {
		header = http.Header{
			"Range": []string{fmt.Sprintf("bytes=%d-", resumeOffset)},
		}
	}

	reader, expectedLength, err := cli.DownloadFile(ctx, fileHash, header)
	if err != nil {
		if resumeOffset > 0 {
			header = nil
			resumeOffset = 0
			reader, expectedLength, err = cli.DownloadFile(ctx, fileHash, nil)
		}
		if err != nil {
			return fmt.Errorf("download failed: %w", err)
		}
	}

	var file *os.File
	if resumeOffset > 0 {
		file, err = os.OpenFile(outputFile, os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return fmt.Errorf("open output file for append: %w", err)
		}
	} else {
		file, err = os.Create(outputFile)
		if err != nil {
			return fmt.Errorf("create output file: %w", err)
		}
	}
	defer file.Close()

	n, err := io.Copy(file, reader)
	if err != nil {
		return fmt.Errorf("write output file: %w", err)
	}

	if n != expectedLength {
		return fmt.Errorf("downloaded file size mismatch: expected %d bytes, got %d bytes", expectedLength, n)
	}

	return nil
}
