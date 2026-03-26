package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/client"
	"github.com/wzshiming/xet/pkg/client/download"
	"github.com/wzshiming/xet/pkg/client/upload"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "info":
		infoCommand()
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
	fmt.Println("  info <file>              Display chunking and hashing information for a file")
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
	fmt.Println("  xetc info myfile.txt")
	fmt.Println("  xetc upload myfile.txt --url https://xet.example.com --token abc123")
	fmt.Println("  xetc download a1b2c3d4... output.txt --url https://xet.example.com")
}

func infoCommand() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: xetc info <file>")
		os.Exit(1)
	}

	filename := os.Args[2]

	data, err := os.ReadFile(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading file: %v\n", err)
		os.Exit(1)
	}

	info, err := upload.ComputeFileInfo(data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error computing file info: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("File Hash: %s\n", info.FileHash.String())
	fmt.Printf("SHA256: %s\n", hex.EncodeToString(info.SHA256[:]))
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
	data, err := os.ReadFile(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Uploading: %s (%d bytes)\n", filename, len(data))

	// Compute file info
	fileInfo, err := upload.ComputeFileInfo(data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error computing file info: %v\n", err)
		os.Exit(1)
	}
	fileInfo.Path = filename

	fmt.Printf("File hash: %s\n", fileInfo.FileHash.String())

	// Create API client
	client := client.NewClient(client.ClientOptions{
		BaseURL:   *url,
		Token:     *token,
		Namespace: *namespace,
		Timeout:   5 * time.Minute,
	})

	// Create upload session
	session := upload.NewSession(upload.SessionOptions{
		Client:            client,
		TargetXorbSize:    64 * 1024 * 1024,
		EnableGlobalDedup: !*noDedup,
	})

	// Upload the file
	ctx := context.Background()
	fmt.Println("Uploading...")
	err = session.UploadFiles(ctx, []upload.FileUploadInfo{fileInfo})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Upload failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("✓ Upload complete!")
	fmt.Printf("File hash: %s\n", fileInfo.FileHash.String())
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

	fmt.Printf("Downloading file: %s\n", hashStr)

	// Create API client
	client := client.NewClient(client.ClientOptions{
		BaseURL: *url,
		Token:   *token,
		Timeout: 5 * time.Minute,
	})

	// Create download session
	session := download.NewSession(download.SessionOptions{
		Client:        client,
		EnableCaching: *enableCache,
	})

	// Download the file
	ctx := context.Background()
	fmt.Println("Downloading...")
	data, err := session.DownloadFile(ctx, fileHash)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Download failed: %v\n", err)
		os.Exit(1)
	}

	// Write to output file
	err = os.WriteFile(outputFile, data, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✓ Download complete! (%d bytes)\n", len(data))
	fmt.Printf("Saved to: %s\n", outputFile)
}
