package hash

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/wzshiming/xet"
)

func NewCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "hash <file>",
		Short: "Compute the xet hash of a local file (use - for stdin)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var reader io.Reader = cmd.InOrStdin()
			if args[0] != "-" {
				file, err := os.Open(args[0])
				if err != nil {
					return err
				}
				defer file.Close()
				reader = file
			}
			var hashes []xet.ChunkHash
			var sizes []uint64
			if err := xet.ChunkData(reader, func(_ int64, chunk []byte) error {
				hashes = append(hashes, xet.ComputeChunkHash(chunk))
				sizes = append(sizes, uint64(len(chunk)))
				return nil
			}); err != nil {
				return err
			}
			_, err := fmt.Fprintln(cmd.OutOrStdout(), xet.ComputeFileHash(hashes, sizes).String())
			return err
		},
	}
}
