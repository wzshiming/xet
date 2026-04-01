package cas

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
)

// Download downloads a file from CAS and saves it to the specified output path. If resume is true and the output file already exists, it will attempt to resume the download from where it left off.
func Download(ctx context.Context, fileHash xet.Hash, outputFile, baseURL, token, namespace string, concurrency int, resume bool, progressFunc client.ProgressFunc) (err error) {
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
	if progressFunc != nil {
		session = session.WithProgress(progressFunc)
	}

	var downloadOpts []client.ReqOpt
	if resumeOffset > 0 {
		downloadOpts = append(downloadOpts, client.WithRangeStart(resumeOffset))
	}

	reader, expectedLength, err := session.DownloadFile(ctx, fileHash, downloadOpts...)
	if err != nil {
		if resumeOffset > 0 {
			resumeOffset = 0
			reader, expectedLength, err = session.DownloadFile(ctx, fileHash)
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
