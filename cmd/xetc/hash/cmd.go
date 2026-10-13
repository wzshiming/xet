package hash

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"
)

func NewCommand() *cobra.Command {
	var verbose bool
	cmd := &cobra.Command{
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
			var hashes, uniqueHashes []xet.ChunkHash
			var sizes, packed []uint64
			var uniqueSizes []uint32
			var size, uniqueBytes, packedBytes uint64
			var unique map[xet.ChunkHash]int
			if verbose {
				unique = map[xet.ChunkHash]int{}
			}
			var buf bytes.Buffer
			if err := xet.ChunkData(reader, func(_ int64, chunk []byte) error {
				h := xet.ComputeChunkHash(chunk)
				hashes = append(hashes, h)
				sizes = append(sizes, uint64(len(chunk)))
				if verbose {
					size += uint64(len(chunk))
					if _, ok := unique[h]; !ok {
						unique[h] = len(uniqueHashes)
						buf.Reset()
						if _, err := xorb.NewEncoder(&buf, false).Write(chunk); err != nil {
							return err
						}
						uniqueHashes = append(uniqueHashes, h)
						uniqueSizes = append(uniqueSizes, uint32(len(chunk)))
						uniqueBytes += uint64(len(chunk))
						packed = append(packed, uint64(buf.Len()))
					}
				}
				return nil
			}); err != nil {
				return err
			}
			fileHash := xet.ComputeFileHash(hashes, sizes).String()
			if !verbose {
				_, err := fmt.Fprintln(cmd.OutOrStdout(), fileHash)
				return err
			}
			groups := xorb.GroupChunkIndicesBySize(uniqueSizes, xet.MaxXorbSize)
			report := fmt.Appendf(nil, "Hash: %s\nSize: %d\nChunks: %d total, %d unique, %d duplicate (%d bytes)\nXorbs: %d\n",
				fileHash, size, len(hashes), len(uniqueHashes), len(hashes)-len(uniqueHashes), size-uniqueBytes, len(groups))
			for _, group := range groups {
				groupHashes, groupSizes := make([]xet.ChunkHash, len(group)), make([]uint64, len(group))
				var raw, groupPacked uint64
				for j, i := range group {
					groupHashes[j], groupSizes[j] = uniqueHashes[i], uint64(uniqueSizes[i])
					raw += groupSizes[j]
					groupPacked += packed[i]
				}
				packedBytes += groupPacked
				report = fmt.Appendf(report, "  %s  %d chunks  %d -> %d bytes\n", xet.ComputeXorbHash(groupHashes, groupSizes), len(group), raw, groupPacked)
			}
			report = fmt.Appendf(report, "Packed: %d bytes", packedBytes)
			if packedBytes > 0 {
				report = fmt.Appendf(report, " (%.2fx)", float64(uniqueBytes)/float64(packedBytes))
			}
			_, err := cmd.OutOrStdout().Write(append(report, '\n'))
			return err
		},
	}
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Print chunking, dedup, xorb grouping and compression details (packed bytes exclude xorb footers)")
	return cmd
}
