package common

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/wzshiming/xet"
	xetcas "github.com/wzshiming/xet/cas"
	"github.com/wzshiming/xet/cmd/xetc/internal/progress"
)

const (
	DefaultHFCASURL   = "https://cas-server.xethub.hf.co"
	DefaultHFEndpoint = "https://huggingface.co"
)

func ExecuteUpload(ctx context.Context, filename, baseURL, token, namespace string, concurrency int, out io.Writer) (err error) {
	if baseURL == "" {
		return fmt.Errorf("--url is required")
	}

	progressWriter := progress.NewWriter(out, progress.FormatUpload)

	if _, err := fmt.Fprintf(out, "%s Uploading file\n", filename); err != nil {
		return err
	}

	fileHash, err := xetcas.Upload(ctx, filename, baseURL, token, namespace, concurrency, func(current, total int64) {
		progressWriter.Callback(current, total)
	})
	if finishErr := progressWriter.Finish(); err == nil && finishErr != nil {
		err = finishErr
	}
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

func ExecuteDownload(ctx context.Context, fileHash xet.Hash, outputFile, baseURL, token, namespace string, concurrency int, resume bool, out io.Writer) (err error) {
	if baseURL == "" {
		return fmt.Errorf("--url is required")
	}

	progressWriter := progress.NewWriter(out, progress.FormatDownload)
	err = xetcas.Download(ctx, fileHash, outputFile, baseURL, token, namespace, concurrency, resume, func(current, total int64) {
		progressWriter.Callback(current, total)
	})
	if finishErr := progressWriter.Finish(); err == nil && finishErr != nil {
		err = finishErr
	}
	if err != nil {
		return err
	}

	var total int64
	if stat, statErr := os.Stat(outputFile); statErr == nil && stat.Mode().IsRegular() {
		total = stat.Size()
	}

	if _, err := fmt.Fprintf(out, "Download complete! (%d bytes)\n", total); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "Saved to: %s\n", outputFile); err != nil {
		return err
	}
	return nil
}
