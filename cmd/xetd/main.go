package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gorilla/handlers"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/token"
)

func main() {
	// Parse command line flags
	addr := flag.String("addr", ":8080", "Server address (host:port)")
	storageDir := flag.String("storage", "./xet-data", "Storage directory for xorbs and shards")
	baseURL := flag.String("base-url", "", "Base URL for serving xorb data (optional)")
	authToken := flag.String("token", "", "Authentication token; also the secret for minted short-lived tokens (optional, if set, clients must provide this token or a minted one)")
	upstream := flag.String("upstream", "", "Upstream hub URL to mirror, e.g. https://huggingface.co (enables mirror mode)")
	upstreamToken := flag.String("upstream-token", "", "Bearer token the mirror uses against the upstream hub")
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

	// Create authentication function if token is provided. The token doubles
	// as the issuer secret, so minted short-lived tokens are deterministic:
	// they survive restarts and validate across instances sharing the token.
	// Without a token the issuer falls back to a random per-process secret.
	issuer, err := token.NewIssuer([]byte(*authToken), 15*time.Minute)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create token issuer: %v\n", err)
		os.Exit(1)
	}

	var authFn server.AuthFunc
	if *authToken != "" {
		authFn = func(tok string) bool {
			return tok == *authToken
		}
		fmt.Println("Authentication enabled")
	} else {
		fmt.Println("⚠️  WARNING: Authentication is disabled. Anyone can access this server.")
	}

	var next http.Handler

	if *upstream != "" {
		// Mirror mode: full-cache middle layer in front of the upstream hub.
		// The mirror handles resolve/token requests and proxies the rest to
		// the upstream; the CAS server below matches its own routes first.
		xetClient, err := client.NewClient(
			client.WithCacheDir(filepath.Join(*storageDir, "mirror", "chunks")),
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create xet client: %v\n", err)
			os.Exit(1)
		}

		next, err = mirror.NewHandler(
			mirror.WithStorage(storage),
			mirror.WithUpstream(*upstream),
			mirror.WithUpstreamToken(*upstreamToken),
			mirror.WithExternalURL(*baseURL),
			mirror.WithCacheDir(filepath.Join(*storageDir, "mirror")),
			mirror.WithClient(xetClient),
			mirror.WithMintToken(issuer.Mint),
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create mirror: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Mirror mode enabled, upstream: %s\n", *upstream)
	}

	// Create server
	next = server.NewHandler(
		server.WithStorage(storage),
		server.WithAuthFunc(authFn),
		server.WithNext(next),
	)

	next = handlers.CombinedLoggingHandler(os.Stdout, next)

	err = http.ListenAndServe(*addr, next)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}
