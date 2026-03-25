//go:build ignore

package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/wzshiming/xet/pkg/gearhash"
	"github.com/wzshiming/xet/pkg/xet"
)

func main() {
	inputFile := flag.String("input", "", "Input file (stdin if not specified)")
	outputFile := flag.String("output", "", "Output file (stdout if not specified)")
	flag.Parse()

	var input io.Reader
	if *inputFile != "" {
		f, err := os.Open(*inputFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening input file: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		input = f
	} else {
		input = os.Stdin
	}

	var output io.Writer
	if *outputFile != "" {
		f, err := os.Create(*outputFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		output = f
	} else {
		output = os.Stdout
	}

	data, err := io.ReadAll(input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		os.Exit(1)
	}

	chunks := gearhash.ChunkData(data)
	for _, chunk := range chunks {
		hash := xet.ComputeChunkHash(chunk.Data)
		fmt.Fprintf(output, "%s %d\n", hash.String(), len(chunk.Data))
	}
}
