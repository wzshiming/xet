package main

import (
	"context"
	"fmt"
	"os"
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
	fmt.Println("  download <hash> <file>   Download a file from XET CAS server")
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
