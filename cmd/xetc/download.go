package main

import (
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/download/hf"
)

func downloadCommand() {
	fs := flag.NewFlagSet("download", flag.ExitOnError)
	url := fs.String("url", "https://cas-server.xethub.hf.co", "CAS server URL")
	token := fs.String("token", "", "Authentication token")
	fs.Parse(os.Args[2:])

	if fs.NArg() < 2 {
		fmt.Fprintln(os.Stderr, "Usage: xet download <hash|hf-resolve-url> <file> [--url <url>] [options]")
		os.Exit(1)
	}

	hashStr := fs.Arg(0)
	outputFile := fs.Arg(1)

	var fileHash xet.Hash
	var err error
	baseURL := *url
	tokenVal := *token

	// Hugging Face resolve URL support
	if isURL(hashStr) {
		var hfInfo hf.Resolved
		hfInfo, err = hf.Resolve(ctx, hashStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Resolve URL failed: %v\n", err)
			os.Exit(1)
		}
		fileHash = hfInfo.Hash
		baseURL = hfInfo.BaseURL
		tokenVal = hfInfo.Token
		fmt.Printf("%s Resolved Hugging Face file hash: %s\n", outputFile, fileHash.String())
	} else {
		// Parse file hash
		fileHash, err = xet.ParseHash(hashStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid file hash: %v\n", err)
			os.Exit(1)
		}
	}

	if baseURL == "" {
		fmt.Fprintln(os.Stderr, "Error: --url is required (or provide a Hugging Face resolve URL as the hash argument)")
		os.Exit(1)
	}

	// Create API client
	cli := client.NewClient(client.WithBaseURL(baseURL), client.WithToken(tokenVal))

	// Create download session
	session := cli.DownloadSession()

	// Download the file
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

func isURL(str string) bool {
	u, err := url.Parse(str)
	if err != nil {
		return false
	}
	return u.Scheme != "" && u.Host != ""
}
