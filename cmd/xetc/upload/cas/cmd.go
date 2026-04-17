package cas

import (
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
	)

	cmd := &cobra.Command{
		Use:   "cas <file>",
		Short: "Upload a file using the native CAS API",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			provider := client.StaticAuthProvider(baseURL, token)
			return common.ExecuteUpload(cmd.Context(), args[0], provider, namespace, concurrency, os.Stderr)
		},
	}

	cmd.Flags().StringVar(&baseURL, "url", common.DefaultHFCASURL, "CAS server URL")
	cmd.Flags().StringVar(&token, "token", "", "CAS token")
	cmd.Flags().StringVar(&namespace, "namespace", "default", "Storage namespace")
	cmd.Flags().IntVar(&concurrency, "concurrency", 4, "Number of upload tasks to run concurrently")
	return cmd
}
