package cas

import (
	"context"
	"fmt"
	"os"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/progress"
)

// Upload uploads a file to the CAS server and returns the resulting file hash.
func Upload(ctx context.Context, filename string, provider client.AuthProvider, namespace string, concurrency int, progressFunc progress.ProgressFunc) (fileHash xet.Hash, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return xet.Hash{}, fmt.Errorf("open input file: %w", err)
	}
	defer f.Close()

	opts := []client.Options{
		client.WithAuthProvider(provider),
		client.WithNamespace(namespace),
		client.WithProgressFunc(progressFunc),
		client.WithConcurrency(concurrency),
	}

	cli, err := client.NewClient(opts...)
	if err != nil {
		return xet.Hash{}, fmt.Errorf("create client: %w", err)
	}

	return cli.UploadFile(ctx, f)
}
