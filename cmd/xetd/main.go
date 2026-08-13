package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gorilla/handlers"
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
	gcInterval := flag.Duration("gc-interval", 0, "Run in-process garbage collection at this interval while serving (0 disables); roots are the mirror index in mirror mode, every uploaded file otherwise")
	gcGrace := flag.Duration("gc-grace", storage.DefaultGCGracePeriod, "GC never removes objects modified within this window; must exceed the longest upload or ingest")
	gcPruneIndex := flag.Duration("gc-prune-index", 0, "During periodic GC, drop mirror index entries and branch pins not used within this window (0 disables)")
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
	var mirrorHandler *mirror.Handler

	if *upstream != "" {
		// Mirror mode: full-cache middle layer in front of the upstream hub.
		// The mirror handles resolve/token requests and proxies the rest to
		// the upstream; the CAS server below matches its own routes first.
		mirrorHandler, err = mirror.NewHandler(
			mirror.WithStorage(storage),
			mirror.WithUpstream(*upstream),
			mirror.WithUpstreamToken(*upstreamToken),
			mirror.WithExternalURL(*baseURL),
			mirror.WithCacheDir(filepath.Join(*storageDir, "mirror")),
			mirror.WithMintToken(issuer.Mint),
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create mirror: %v\n", err)
			os.Exit(1)
		}
		next = mirrorHandler

		fmt.Printf("Mirror mode enabled, upstream: %s\n", *upstream)
	}

	if *gcInterval > 0 {
		go runPeriodicGC(storage, mirrorHandler, *gcInterval, *gcGrace, *gcPruneIndex)
		fmt.Printf("Periodic GC enabled, interval: %s, grace: %s\n", *gcInterval, *gcGrace)
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
