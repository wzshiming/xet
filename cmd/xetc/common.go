package main

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
)

const (
	defaultHFCASURL   = "https://cas-server.xethub.hf.co"
	defaultHFEndpoint = "https://huggingface.co"
)

func executeUpload(ctx context.Context, filename, baseURL, token, namespace string, noDedup bool, out io.Writer) (err error) {
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

	session := cli.UploadSession(client.WithUploadGlobalDedup(!noDedup))

	if _, err := fmt.Fprintf(out, "%s Uploading file\n", filename); err != nil {
		return err
	}
	fileHashes, err := session.UploadFiles(ctx, f)
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

func executeDownload(ctx context.Context, fileHash xet.Hash, outputFile, baseURL, token, namespace string, out io.Writer) (err error) {
	if baseURL == "" {
		return fmt.Errorf("--url is required")
	}

	cli := client.NewClient(
		client.WithBaseURL(baseURL),
		client.WithToken(token),
		client.WithNamespace(namespace),
	)
	session := cli.DownloadSession()

	reader, _, err := session.DownloadFile(ctx, fileHash)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer func() {
		closeErr := file.Close()
		if err == nil && closeErr != nil {
			err = fmt.Errorf("close output file: %w", closeErr)
		}
	}()

	n, err := io.Copy(file, reader)
	if err != nil {
		return fmt.Errorf("write output file: %w", err)
	}

	if _, err := fmt.Fprintf(out, "✓ Download complete! (%d bytes)\n", n); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "Saved to: %s\n", outputFile); err != nil {
		return err
	}
	return nil
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
