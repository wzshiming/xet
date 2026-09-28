package resolve

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/wzshiming/xet/cmd/xetc/internal/common"
)

func NewCommand() *cobra.Command {
	var (
		token       string
		concurrency int
		cacheDir    string
		resume      bool
	)

	cmd := &cobra.Command{
		Use:   "resolve <resolve-url> <file>",
		Short: "Resolve a Hugging Face URL and download through CAS",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return common.ExecuteResolveDownload(cmd.Context(), args[0], token, args[1], concurrency, cacheDir, resume, os.Stderr)
		},
	}

	cmd.Flags().StringVar(&token, "token", "", "Hugging Face access token")
	cmd.Flags().IntVar(&concurrency, "concurrency", 4, "Number of xorb ranges to prefetch concurrently")
	cmd.Flags().StringVar(&cacheDir, "cache-dir", "", "Directory for the chunk cache (default: <os temp dir>/xet-cache)")
	cmd.Flags().BoolVar(&resume, "resume", false, "Resume a partially downloaded file")
	return cmd
}
