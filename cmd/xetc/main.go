package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/wzshiming/xet/cmd/xetc/download"
	"github.com/wzshiming/xet/cmd/xetc/upload"
)

var ctx = context.Background()

func main() {
	cmd := newRootCommand()
	if err := cmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "xetc",
		Short:         "xet command-line tool",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	cmd.AddCommand(
		upload.NewCommand(),
		download.NewCommand(),
	)
	return cmd
}
