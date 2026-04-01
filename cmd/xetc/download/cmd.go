package download

import (
	"github.com/spf13/cobra"
	"github.com/wzshiming/xet/cmd/xetc/download/cas"
	"github.com/wzshiming/xet/cmd/xetc/download/hf"
	"github.com/wzshiming/xet/cmd/xetc/download/resolve"
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "download",
		Short: "Download files through CAS, Hugging Face tokens, or resolve URLs",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	cmd.AddCommand(
		cas.NewCommand(),
		hf.NewCommand(),
		resolve.NewCommand(),
	)
	return cmd
}
