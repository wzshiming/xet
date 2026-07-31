package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/gorilla/handlers"
	"github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
)

func main() {
	// Parse command line flags
	addr := flag.String("addr", ":8080", "Server address (host:port)")
	storageDir := flag.String("storage", "./xet-data", "Storage directory for xorbs and shards")
	baseURL := flag.String("base-url", "", "Base URL for serving xorb data (optional)")
	authToken := flag.String("token", "", "Authentication token (optional, if set, clients must provide this token)")
	mirrorUpstream := flag.String("mirror-upstream", "", "Hugging Face upstream URL (enables mirror mode, e.g. https://huggingface.co)")
	hfToken := flag.String("hf-token", "", "Hugging Face token used for upstream requests")
	concurrency := flag.Int("concurrency", 4, "Concurrent Xet cache transfers")
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

	// In mirror mode, unmatched Hugging Face routes are proxied immediately;
	// the local CAS routes remain served by the Xet server.
	var handler http.Handler
	if *mirrorUpstream != "" {
		handler, err = mirror.NewHandler(mirror.Options{Upstream: *mirrorUpstream, HFToken: *hfToken, LocalToken: *authToken, CacheDir: *storageDir + "/mirror", Storage: storage, Concurrency: *concurrency})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create mirror: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Hugging Face mirror enabled: %s\n", *mirrorUpstream)
	}

	// Create server
	handler = server.NewHandler(
		server.WithStorage(storage),
		server.WithAuthFunc(authFn),
		server.WithNext(handler),
	)

	handler = handlers.CombinedLoggingHandler(os.Stderr, handler)

	err = http.ListenAndServe(*addr, handler)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}
