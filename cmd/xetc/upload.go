package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/wzshiming/xet/client"
)

func uploadCommand() {
	fs := flag.NewFlagSet("upload", flag.ExitOnError)
	url := fs.String("url", "", "CAS server URL")
	token := fs.String("token", "", "Authentication token")
	namespace := fs.String("namespace", "default", "Storage namespace")
	noDedup := fs.Bool("no-dedup", false, "Disable global deduplication")
	fs.Parse(os.Args[2:])

	if *url == "" {
		fmt.Fprintln(os.Stderr, "Error: --url is required")
		os.Exit(1)
	}

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "Usage: xet upload <file> --url <url> [options]")
		os.Exit(1)
	}

	filename := fs.Arg(0)

	// Read file
	f, err := os.Open(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	// Create API client
	cli := client.NewClient(client.WithBaseURL(*url), client.WithToken(*token), client.WithNamespace(*namespace))

	// Create upload session
	session := cli.UploadSession(client.WithUploadGlobalDedup(!*noDedup))

	// Upload the file
	fmt.Printf("%s Uploading file\n", filename)
	filehashs, err := session.UploadFiles(ctx, f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Upload failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("✓ Upload complete!")
	for _, h := range filehashs {
		fmt.Printf("File hash: %s\n", h.String())
	}
}
