package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/wzshiming/xet/pkg/server"
)

func main() {
	// Parse command line flags
	addr := flag.String("addr", ":8080", "Server address (host:port)")
	storageDir := flag.String("storage", "./xet-data", "Storage directory for xorbs and shards")
	baseURL := flag.String("base-url", "", "Base URL for serving xorb data (optional)")
	authToken := flag.String("token", "", "Authentication token (optional, if set, clients must provide this token)")
	tlsCert := flag.String("tls-cert", "", "TLS certificate file (optional)")
	tlsKey := flag.String("tls-key", "", "TLS key file (optional)")
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
	srv := server.NewServer(server.ServerOptions{
		Storage: storage,
		AuthFn:  authFn,
	})

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start server in a goroutine
	errChan := make(chan error, 1)
	go func() {
		if *tlsCert != "" && *tlsKey != "" {
			errChan <- srv.ListenAndServeTLS(*addr, *tlsCert, *tlsKey)
		} else {
			errChan <- srv.ListenAndServe(*addr)
		}
	}()

	// Wait for shutdown signal or error
	select {
	case err := <-errChan:
		if err != nil {
			fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
			os.Exit(1)
		}
	case sig := <-sigChan:
		fmt.Printf("\nReceived signal %v, shutting down...\n", sig)
	}
}
