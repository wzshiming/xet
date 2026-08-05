package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gorilla/handlers"
	"github.com/wzshiming/xet/hf/mirror"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
)

func main() {
	// Parse command line flags
	addr := flag.String("addr", ":8080", "Server address (host:port)")
	storageDir := flag.String("storage", "./xet-data", "Storage directory for xorbs and shards")
	baseURL := flag.String("base-url", "", "Base URL for serving xorb data (optional)")
	authToken := flag.String("token", "", "Authentication token (optional, if set, clients must provide this token)")
	hfMirrorUpstream := flag.String("hf-mirror-upstream", "", "Hugging Face upstream endpoint; empty disables the mirror")
	hfMirrorToken := flag.String("hf-mirror-token", os.Getenv("HF_TOKEN"), "Server-side Hugging Face token for the mirror (defaults to HF_TOKEN)")
	hfMirrorCache := flag.String("hf-mirror-cache", "", "Persistent mirror metadata and partial download directory (defaults to <storage>/mirror)")
	hfMirrorConcurrency := flag.Int("hf-mirror-concurrency", 4, "Concurrency used for upstream XET downloads and local conversion")
	flag.Parse()

	// Create storage
	storage, err := storage.NewFileStorage(
		storage.WithBasePath(*storageDir),
		storage.WithBaseURL(*baseURL),
	)
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

	var next http.Handler

	// Create the reusable XET server first. The optional HF mirror is only an
	// adapter in front of it and stores converted files in the same storage.
	next = server.NewHandler(
		server.WithStorage(storage),
		server.WithAuthFunc(authFn),
		server.WithNext(next),
	)
	if *hfMirrorUpstream != "" {
		cacheDir := *hfMirrorCache
		if cacheDir == "" {
			cacheDir = filepath.Join(*storageDir, "mirror")
		}
		mirrorHandler, err := mirror.NewHandler(
			mirror.WithStorage(storage),
			mirror.WithNext(next),
			mirror.WithUpstream(*hfMirrorUpstream),
			mirror.WithUpstreamToken(*hfMirrorToken),
			mirror.WithCASToken(*authToken),
			mirror.WithCacheDir(cacheDir),
			mirror.WithPublicBaseURL(*baseURL),
			mirror.WithConcurrency(*hfMirrorConcurrency),
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create Hugging Face mirror: %v\n", err)
			os.Exit(1)
		}
		next = mirrorHandler
		fmt.Printf("Hugging Face mirror enabled: %s\n", *hfMirrorUpstream)
	}

	next = handlers.CombinedLoggingHandler(os.Stdout, next)

	err = http.ListenAndServe(*addr, next)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}
