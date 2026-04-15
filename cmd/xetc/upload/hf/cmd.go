package hf

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/wzshiming/xet/cmd/xetc/internal/common"
	"github.com/wzshiming/xet/hf"
)

func NewCommand() *cobra.Command {
	var (
		hfRepoID    string
		hfToken     string
		hfEndpoint  string
		hfRepoType  string
		hfRevision  string
		namespace   string
		concurrency int
	)

	cmd := &cobra.Command{
		Use:   "hf <file>",
		Short: "Upload a file using Hugging Face xet-write-token API",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if hfRepoID == "" {
				return fmt.Errorf("--repo-id is required")
			}
			if hfToken == "" {
				return fmt.Errorf("--token is required")
			}

			target := hf.Target{
				Endpoint: hfEndpoint,
				RepoType: hfRepoType,
				RepoID:   hfRepoID,
				Revision: hfRevision,
			}

			hfInfo, err := hf.ResolveXETWriteToken(cmd.Context(), nil, target, hfToken)
			if err != nil {
				return fmt.Errorf("resolve Hugging Face upload target: %w", err)
			}

			return common.ExecuteUpload(cmd.Context(), args[0], hfInfo.BaseURL, hfInfo.Token, namespace, concurrency, os.Stderr)
		},
	}

	cmd.Flags().StringVar(&hfRepoID, "repo-id", "", "Hugging Face repo ID, e.g. org/repo")
	cmd.Flags().StringVar(&hfToken, "token", "", "Hugging Face access token")
	cmd.Flags().StringVar(&hfEndpoint, "endpoint", common.DefaultHFEndpoint, "Hugging Face Hub endpoint override")
	cmd.Flags().StringVar(&hfRepoType, "repo-type", "model", "Hugging Face repo type: model, dataset, or space")
	cmd.Flags().StringVar(&hfRevision, "revision", "main", "Hugging Face revision")
	cmd.Flags().StringVar(&namespace, "namespace", "default", "Storage namespace")
	cmd.Flags().IntVar(&concurrency, "concurrency", 4, "Number of xorb ranges to prefetch concurrently")
	return cmd
}
