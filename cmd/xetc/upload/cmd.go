package upload

import (
	"github.com/spf13/cobra"
	"github.com/wzshiming/xet/cmd/xetc/upload/cas"
	"github.com/wzshiming/xet/cmd/xetc/upload/hf"
	"github.com/wzshiming/xet/cmd/xetc/upload/lfs"
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upload",
		Short: "Upload files through CAS or Hugging Face LFS/XET flow",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	cmd.AddCommand(
		cas.NewCommand(),
		hf.NewCommand(),
		lfs.NewCommand(),
	)
	return cmd
}
