package cas

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/cmd/xetc/internal/common"
)

func NewCommand() *cobra.Command {
	var (
		baseURL     string
		token       string
		namespace   string
		concurrency int
		cacheDir    string
	)

	cmd := &cobra.Command{
		Use:   "cas <file>",
		Short: "Upload a file using the native CAS API",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cli, err := client.NewClient(append(common.Options(namespace, concurrency, cacheDir, os.Stderr), client.WithUpstreamProvider(client.StaticUpstreamProvider(baseURL, token)))...)
			if err != nil {
				return fmt.Errorf("upload failed: create client: %w", err)
			}
			return common.ExecuteUpload(cmd.Context(), args[0], cli, os.Stderr)
		},
	}

	cmd.Flags().StringVar(&baseURL, "url", common.DefaultHFCASURL, "CAS server URL")
	cmd.Flags().StringVar(&token, "token", "", "CAS token")
	cmd.Flags().StringVar(&namespace, "namespace", "default", "Storage namespace")
	cmd.Flags().IntVar(&concurrency, "concurrency", 4, "Number of upload tasks to run concurrently")
	cmd.Flags().StringVar(&cacheDir, "cache-dir", "", "Directory for temporary upload files (default: <os temp dir>)")
	return cmd
}
