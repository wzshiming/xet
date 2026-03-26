//go:build ignore

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"

	"github.com/wzshiming/xet"
)

func main() {
	hashType := flag.String("hash-type", "", "Hash type: chunk, xorb, file, range")
	inputFile := flag.String("input", "", "Input file (stdin if not specified)")
	outputFile := flag.String("output", "", "Output file (stdout if not specified)")
	flag.Parse()

	if *hashType == "" {
		fmt.Fprintf(os.Stderr, "Error: --hash-type is required\n")
		os.Exit(1)
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

	if *hashType == "chunk" {
		data, err := io.ReadAll(input)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			os.Exit(1)
		}
		hash := xet.ComputeChunkHash(data)
		fmt.Fprintf(output, "%s", hash.String())
		return
	}

	hashes, sizes := readChunksList(input)

	var hash xet.Hash
	switch *hashType {
	case "xorb":
		hash = xet.ComputeXorbHash(hashes, sizes)
	case "file":
		hash = xet.ComputeFileHash(hashes, sizes)
	case "range":
		for i, hash := range hashes {
			hashes[i] = hash
		}
		hash = xet.ComputeVerificationHash(hashes)
	default:
		fmt.Fprintf(os.Stderr, "Unknown hash type: %s\n", *hashType)
		os.Exit(1)
	}

	fmt.Fprintf(output, "%s", hash.String())
}

func readChunksList(input io.Reader) ([]xet.Hash, []uint64) {
	lineRegex := regexp.MustCompile(`^([0-9a-fA-F]+)\s+(\d+)$`)

	var hashes []xet.Hash
	var sizes []uint64
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		matches := lineRegex.FindStringSubmatch(line)
		if matches == nil {
			fmt.Fprintf(os.Stderr, "Failed to parse line: %s\n", line)
			os.Exit(1)
		}
		hash, err := xet.HashFrom(matches[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse hash: %v\n", err)
			os.Exit(1)
		}
		size, err := strconv.ParseUint(matches[2], 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse size: %v\n", err)
			os.Exit(1)
		}

		hashes = append(hashes, hash)
		sizes = append(sizes, size)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		os.Exit(1)
	}

	return hashes, sizes
}
