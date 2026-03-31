package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var ctx = context.Background()

func main() {
	cmd := newRootCmd()
	if err := cmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "xetc",
		Short:         "XET content-addressable storage tool",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	cmd.AddCommand(newUploadCmd(), newDownloadCmd())
	return cmd
}
