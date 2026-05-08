package cas

import (
	"context"
	"fmt"
	"os"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/progress"
)

// Download downloads a file from CAS and saves it to the specified output path. If resume is true and the output file already exists, it will attempt to resume the download from where it left off.
func Download(ctx context.Context, fileHash xet.Hash, outputFile string, provider client.AuthProvider, namespace string, concurrency int, resume bool, progressFunc progress.ProgressFunc) (err error) {
	cli, err := client.NewClient(
		client.WithNamespace(namespace),
		client.WithProgressFunc(progressFunc),
		client.WithConcurrency(concurrency),
	)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}

	var file *os.File
	if resume {
		file, err = os.OpenFile(outputFile, os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			return fmt.Errorf("open output file: %w", err)
		}
	} else {
		file, err = os.Create(outputFile)
		if err != nil {
			return fmt.Errorf("create output file: %w", err)
		}
	}
	defer file.Close()

	return cli.DownloadFileWithAuthProvider(ctx, provider, fileHash, file)
}
