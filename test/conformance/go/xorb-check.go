package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/wzshiming/xet/pkg/xet"
	"github.com/wzshiming/xet/pkg/xorb"
)

func main() {
	inputFile := flag.String("input", "", "Input file (stdin if not specified)")
	hashStr := flag.String("hash", "", "Expected hash to check against")
	hashFromPath := flag.Bool("hash-from-path", false, "Parse hash from first 64 chars of filename")
	outputChunks := flag.String("output-chunks", "", "Output file for chunk information")
	outputChunksStdout := flag.Bool("output-chunks-stdout", false, "Output chunks to stdout")
	flag.Parse()

	if *hashFromPath && *inputFile == "" {
		fmt.Fprintf(os.Stderr, "--hash-from-path requires --input to be set\n")
		os.Exit(1)
	}

	var providedHash *xet.Hash
	if *hashStr != "" {
		h, err := xet.StringToHash(*hashStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse hash: %v\n", err)
			os.Exit(1)
		}
		providedHash = &h
	} else if *hashFromPath {
		filename := *inputFile
		if idx := strings.LastIndex(filename, "/"); idx >= 0 {
			filename = filename[idx+1:]
		}
		if len(filename) >= 64 {
			filename = filename[:64]
		}
		h, err := xet.StringToHash(filename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse hash from path: %v\n", err)
			os.Exit(1)
		}
		providedHash = &h
	}

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

	data, err := io.ReadAll(input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		os.Exit(1)
	}

	xorbObj, err := xorb.Deserialize(data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to deserialize xorb: %v\n", err)
		os.Exit(1)
	}

	totalSize := 0
	for _, chunk := range xorbObj.Chunks {
		totalSize += len(chunk.UncompressedData)
	}

	fmt.Fprintf(os.Stderr, "Successfully deserialized xorb with %d chunks totalling %d Bytes!\n",
		len(xorbObj.Chunks), totalSize)

	computedXorbHash := xorbObj.Hash
	fmt.Fprintf(os.Stderr, "computed xorb hash: %s\n", computedXorbHash.String())

	if providedHash != nil {
		if computedXorbHash != *providedHash {
			fmt.Fprintf(os.Stderr, "provided hash does not match computed hash!\n")
		} else {
			fmt.Fprintf(os.Stderr, "provided hash matches computed hash!\n")
		}
	}

	if *outputChunksStdout || *outputChunks != "" {
		var output io.Writer
		if *outputChunksStdout {
			output = os.Stdout
		} else {
			f, err := os.Create(*outputChunks)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
				os.Exit(1)
			}
			defer f.Close()
			output = f
		}

		for _, chunk := range xorbObj.Chunks {
			fmt.Fprintf(output, "%s %d\n", chunk.Hash.String(), len(chunk.UncompressedData))
		}
	}
}
