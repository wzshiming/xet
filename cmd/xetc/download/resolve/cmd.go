package resolve

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/wzshiming/xet/cmd/xetc/internal/common"
	"github.com/wzshiming/xet/hf"
)

func NewCommand() *cobra.Command {
	var (
		concurrency int
		resume      bool
	)

	cmd := &cobra.Command{
		Use:   "resolve <resolve-url> <file>",
		Short: "Resolve a Hugging Face URL and download through CAS",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			hash, token, err := hf.ResolveDownload(cmd.Context(), nil, args[0])
			if err != nil {
				return fmt.Errorf("resolve download target: %w", err)
			}
			if _, err := fmt.Fprintf(os.Stderr, "%s Resolved Hugging Face file hash: %s\n", args[1], hash.String()); err != nil {
				return err
			}
			return common.ExecuteDownload(cmd.Context(), hash, args[1], token.BaseURL, token.Token, "default", concurrency, resume, os.Stderr)
		},
	}

	cmd.Flags().IntVar(&concurrency, "concurrency", 4, "Number of xorb ranges to prefetch concurrently")
	cmd.Flags().BoolVar(&resume, "resume", false, "Resume a partially downloaded file")
	return cmd
}
