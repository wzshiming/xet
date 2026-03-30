package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
)

func downloadCommand() {
	fs := flag.NewFlagSet("download", flag.ExitOnError)
	url := fs.String("url", "", "CAS server URL")
	token := fs.String("token", "", "Authentication token")
	fs.Parse(os.Args[2:])

	if *url == "" {
		fmt.Fprintln(os.Stderr, "Error: --url is required")
		os.Exit(1)
	}

	if fs.NArg() < 2 {
		fmt.Fprintln(os.Stderr, "Usage: xet download <hash> <file> --url <url> [options]")
		os.Exit(1)
	}

	hashStr := fs.Arg(0)
	outputFile := fs.Arg(1)

	// Parse file hash
	fileHash, err := xet.ParseHash(hashStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid file hash: %v\n", err)
		os.Exit(1)
	}

	// Create API client
	cli := client.NewClient(client.ClientOptions{
		BaseURL: *url,
		Token:   *token,
		Timeout: 5 * time.Minute,
	})

	// Create download session
	session := cli.DownloadSession()

	// Download the file
	fmt.Printf("%s Downloading file with hash: %s\n", outputFile, fileHash.String())
	reader, _, err := session.DownloadFile(ctx, fileHash)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Download failed: %v\n", err)
		os.Exit(1)
	}

	file, err := os.Create(outputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// Write downloaded content to output file
	n, err := io.Copy(file, reader)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing to output file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✓ Download complete! (%d bytes)\n", n)
	fmt.Printf("Saved to: %s\n", outputFile)
}
