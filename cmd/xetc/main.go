package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
)

var ctx = context.Background()

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "upload":
		uploadCommand()
	case "download":
		downloadCommand()
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("XET - Content-Addressable Storage Tool")
	fmt.Println()
	fmt.Println("Usage: xetc <command> [options]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  upload <file>            Upload a file to XET CAS server")
	fmt.Println("  download <hash> <output> Download a file from XET CAS server")
	fmt.Println("  help                     Display this help message")
	fmt.Println()
	fmt.Println("Upload Options:")
	fmt.Println("  --url <url>              CAS server URL (required)")
	fmt.Println("  --token <token>          Authentication token")
	fmt.Println("  --namespace <ns>         Storage namespace (default: default)")
	fmt.Println("  --no-dedup               Disable global deduplication")
	fmt.Println()
	fmt.Println("Download Options:")
	fmt.Println("  --url <url>              CAS server URL (required)")
	fmt.Println("  --token <token>          Authentication token")
	fmt.Println("  --cache                  Enable chunk caching")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  xetc upload myfile.txt --url https://xet.example.com --token abc123")
	fmt.Println("  xetc download a1b2c3d4... output.txt --url https://xet.example.com")
}

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
	cli := client.NewClient(client.ClientOptions{
		BaseURL:   *url,
		Token:     *token,
		Namespace: *namespace,
		Timeout:   5 * time.Minute,
	})

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

func downloadCommand() {
	fs := flag.NewFlagSet("download", flag.ExitOnError)
	url := fs.String("url", "", "CAS server URL")
	token := fs.String("token", "", "Authentication token")
	enableCache := fs.Bool("cache", false, "Enable chunk caching")
	fs.Parse(os.Args[2:])

	if *url == "" {
		fmt.Fprintln(os.Stderr, "Error: --url is required")
		os.Exit(1)
	}

	if fs.NArg() < 2 {
		fmt.Fprintln(os.Stderr, "Usage: xet download <hash> <output> --url <url> [options]")
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
	session := cli.DownloadSession(client.WithDownloadCaching(*enableCache))

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
