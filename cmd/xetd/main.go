package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/wzshiming/xet/pkg/server"
)

func main() {
	// Parse command line flags
	addr := flag.String("addr", ":8080", "Server address (host:port)")
	storageDir := flag.String("storage", "./xet-data", "Storage directory for xorbs and shards")
	baseURL := flag.String("base-url", "", "Base URL for serving xorb data (optional)")
	authToken := flag.String("token", "", "Authentication token (optional, if set, clients must provide this token)")
	flag.Parse()

	// Create storage
	storage, err := server.NewFileStorage(server.FileStorageOptions{
		BasePath: *storageDir,
		BaseURL:  *baseURL,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create storage: %v\n", err)
		os.Exit(1)
	}

	// Create authentication function if token is provided
	var authFn server.AuthFunc
	if *authToken != "" {
		authFn = func(token string) bool {
			return token == *authToken
		}
		fmt.Println("Authentication enabled")
	} else {
		fmt.Println("⚠️  WARNING: Authentication is disabled. Anyone can access this server.")
	}

	// Create server
	srv := server.NewHandler(server.HandlerOptions{
		Storage: storage,
		AuthFn:  authFn,
	})

	err = http.ListenAndServe(*addr, srv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}
