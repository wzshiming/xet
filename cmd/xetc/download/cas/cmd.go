package cas

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/cmd/xetc/internal/common"
)

func NewCommand() *cobra.Command {
	var (
		baseURL     string
		token       string
		namespace   string
		concurrency int
		resume      bool
	)

	cmd := &cobra.Command{
		Use:   "cas <file> <hash>",
		Short: "Download a file using the native CAS API",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			hash, err := xet.ParseFileHash(args[1])
			if err != nil {
				return fmt.Errorf("invalid file hash: %w", err)
			}

			provider := client.StaticAuthProvider(baseURL, token)
			return common.ExecuteDownload(cmd.Context(), hash, args[0], provider, namespace, concurrency, resume, os.Stderr)
		},
	}

	cmd.Flags().StringVar(&baseURL, "url", common.DefaultHFCASURL, "CAS server URL")
	cmd.Flags().StringVar(&token, "token", "", "CAS token")
	cmd.Flags().StringVar(&namespace, "namespace", "default", "Storage namespace")
	cmd.Flags().IntVar(&concurrency, "concurrency", 4, "Number of download tasks to run concurrently")
	cmd.Flags().BoolVar(&resume, "resume", false, "Resume a partially downloaded file")
	return cmd
}
