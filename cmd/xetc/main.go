package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
)

var ctx = context.Background()

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, out, errOut io.Writer) error {
	cmd := newRootCmd(out, errOut)
	cmd.SetArgs(normalizeArgs(args))
	return cmd.ExecuteContext(ctx)
}

func newRootCmd(out, errOut io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "xetc",
		Short:         "XET content-addressable storage tool",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	cmd.SetOut(out)
	cmd.SetErr(errOut)
	cmd.CompletionOptions.DisableDefaultCmd = true
	cmd.AddCommand(newUploadCmd(out), newDownloadCmd(out))
	return cmd
}
