package cas

import (
	"context"
	"fmt"
	"os"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
)

// Upload uploads a file to the CAS server and returns the resulting file hash.
func Upload(ctx context.Context, filename, baseURL, token, namespace string, concurrency int, progressFunc client.ProgressFunc) (fileHash xet.Hash, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return xet.Hash{}, fmt.Errorf("open input file: %w", err)
	}
	defer f.Close()

	opts := []client.Options{
		client.WithBaseURL(baseURL),
		client.WithToken(token),
		client.WithNamespace(namespace),
	}

	cli := client.NewClient(opts...)
	session := cli.UploadSession().
		WithConcurrency(concurrency).
		WithEnableSHA256(true)

	if progressFunc != nil {
		session = session.WithProgress(progressFunc)
	}

	return session.UploadFile(ctx, f)
}
