package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/wzshiming/xet/pkg/client"
	"github.com/wzshiming/xet/pkg/client/download"
	"github.com/wzshiming/xet/pkg/client/upload"
	"github.com/wzshiming/xet/pkg/gearhash"
	"github.com/wzshiming/xet/pkg/xet"
	"github.com/wzshiming/xet/pkg/xorb"
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

	// Read file
	data, err := os.ReadFile(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("File: %s (%d bytes)\n\n", filename, len(data))

	// Chunk the data
	fmt.Println("=== Chunking ===")
	chunks, err := collectChunks(data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error chunking data: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Number of chunks: %d\n", len(chunks))

	// Compute chunk hashes
	chunkHashes := make([]xet.Hash, len(chunks))
	for i, chunk := range chunks {
		chunkHashes[i] = xet.ComputeChunkHash(chunk.Data)
		fmt.Printf("Chunk %d: offset=%d, size=%d, hash=%s\n",
			i, chunk.Offset, len(chunk.Data), chunkHashes[i].String())
	}

	// Create an xorb
	fmt.Println("\n=== Creating Xorb ===")
	xorbObj := xorb.NewXorb()
	for _, chunk := range chunks {
		if err := xorbObj.AddChunk(chunk.Data); err != nil {
			fmt.Fprintf(os.Stderr, "Error adding chunk: %v\n", err)
			os.Exit(1)
		}
	}

	// Serialize the xorb
	serialized, err := xorbObj.Serialize()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error serializing xorb: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Xorb hash: %s\n", xorbObj.Hash.String())
	fmt.Printf("Xorb size: %d bytes\n", len(serialized))

	// Compute file hash using merkle tree
	fmt.Println("\n=== File Hash ===")
	chunkSizes := make([]uint64, len(chunks))
	for i, chunk := range chunks {
		chunkSizes[i] = uint64(len(chunk.Data))
	}

	// For file hash, we need to apply ZERO_KEY to the merkle root
	// The xorb hash is the merkle root, so we apply the final keyed hash
	fileHash := xet.ComputeFileHash(xorbObj.Hash[:])
	fmt.Printf("File hash: %s\n", fileHash.String())

	// Test deserialization
	fmt.Println("\n=== Testing Deserialization ===")
	deserialized, err := xorb.Deserialize(serialized)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error deserializing xorb: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Deserialized xorb hash: %s\n", deserialized.Hash.String())
	fmt.Printf("Number of chunks: %d\n", len(deserialized.Chunks))

	// Verify data integrity
	fmt.Println("\n=== Verifying Data Integrity ===")
	var reconstructed []byte
	for i, chunk := range deserialized.Chunks {
		reconstructed = append(reconstructed, chunk.UncompressedData...)

		// Verify chunk hash
		computedHash := xet.ComputeChunkHash(chunk.UncompressedData)
		if computedHash != chunk.Hash {
			fmt.Printf("ERROR: Chunk %d hash mismatch!\n", i)
			os.Exit(1)
		}
	}

	if len(reconstructed) != len(data) {
		fmt.Printf("ERROR: Reconstructed size mismatch: %d vs %d\n", len(reconstructed), len(data))
		os.Exit(1)
	}

	// Verify byte-by-byte
	allMatch := true
	for i := range data {
		if reconstructed[i] != data[i] {
			allMatch = false
			break
		}
	}

	if allMatch {
		fmt.Println("✓ Data integrity verified - all chunks match!")
	} else {
		fmt.Println("✗ ERROR: Reconstructed data does not match original!")
		os.Exit(1)
	}

	fmt.Println("\n=== Summary ===")
	fmt.Printf("Original size:  %d bytes\n", len(data))
	fmt.Printf("Xorb size:      %d bytes\n", len(serialized))
	fmt.Printf("Compression:    %.2f%%\n", float64(len(serialized))*100/float64(len(data)))
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

type chunk struct {
	Data   []byte
	Offset int64
}

func collectChunks(data []byte) ([]chunk, error) {
	var chunks []chunk
	err := gearhash.ChunkData(bytes.NewReader(data), func(offset int64, dataChunk []byte) error {
		buf := make([]byte, len(dataChunk))
		copy(buf, dataChunk)
		chunks = append(chunks, chunk{Data: buf, Offset: offset})
		return nil
	})
	return chunks, err
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
