package main

import (
	"context"
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
	s3Bucket := flag.String("s3-bucket", "", "S3 bucket for xorbs and shards (enables S3 storage; credentials come from the standard AWS config chain)")
	s3Prefix := flag.String("s3-prefix", "", "Key prefix within the S3 bucket (optional)")
	s3Endpoint := flag.String("s3-endpoint", "", "Custom S3 endpoint URL, e.g. for MinIO (optional)")
	s3Region := flag.String("s3-region", "", "S3 region (optional, falls back to AWS config chain)")
	s3PathStyle := flag.Bool("s3-path-style", false, "Use path-style S3 addressing (required by MinIO and most self-hosted stores)")
	s3Presign := flag.Bool("s3-presign", true, "Serve xorb downloads as presigned S3 GET URLs; disable when clients cannot reach the S3 endpoint")
	s3PresignExpiry := flag.Duration("s3-presign-expiry", time.Hour, "Validity of presigned xorb URLs")
	s3PresignEndpoint := flag.String("s3-presign-endpoint", "", "Endpoint used in presigned xorb URLs when clients reach the object store at a different address than the server (optional, defaults to -s3-endpoint)")
	flag.Parse()

	// Create storage: S3 when a bucket is configured, local filesystem otherwise.
	var stor storage.Storage
	var err error
	if *s3Bucket != "" {
		stor, err = storage.NewS3Storage(context.Background(),
			storage.WithS3Bucket(*s3Bucket),
			storage.WithS3Prefix(*s3Prefix),
			storage.WithS3Endpoint(*s3Endpoint),
			storage.WithS3Region(*s3Region),
			storage.WithS3PathStyle(*s3PathStyle),
			storage.WithS3Presign(*s3Presign),
			storage.WithS3PresignExpiry(*s3PresignExpiry),
			storage.WithS3PresignEndpoint(*s3PresignEndpoint),
			storage.WithS3BaseURL(*baseURL),
		)
		if err == nil {
			fmt.Printf("S3 storage enabled, bucket: %s\n", *s3Bucket)
		}
	} else {
		stor, err = storage.NewFileStorage(
			storage.WithBasePath(*storageDir),
			storage.WithBaseURL(*baseURL),
		)
	}
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
		fmt.Println("⚠️  WARNING: Authentication is disabled. Anyone can access this server; GC endpoints are disabled.")
	}

	var next http.Handler
	var mirrorHandler *mirror.Handler

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

		proxy, err := mirror.NewUpstreamProxy(*upstream, *upstreamToken)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create upstream proxy: %v\n", err)
			os.Exit(1)
		}

		next, err = mirror.NewHandler(
			mirror.WithStorage(stor),
			mirror.WithUpstream(*upstream),
			mirror.WithUpstreamToken(*upstreamToken),
			mirror.WithExternalURL(*baseURL),
			mirror.WithCacheDir(filepath.Join(*storageDir, "mirror")),
			mirror.WithClient(xetClient),
			mirror.WithMintToken(issuer.Mint),
			mirror.WithNext(proxy),
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create mirror: %v\n", err)
			os.Exit(1)
		}
		mirrorHandler = next.(*mirror.Handler)

		fmt.Printf("Mirror mode enabled, upstream: %s\n", *upstream)
	}

	// Create server
	serverOpts := []server.Option{
		server.WithStorage(stor),
		server.WithAuthFunc(authFn),
		server.WithNext(next),
	}
	if mirrorHandler != nil {
		// Keep the mirror index consistent with GC file deletions.
		serverOpts = append(serverOpts, server.WithFileRemovedHook(func(_ context.Context, sha256Hex, _ string) error {
			_, err := mirrorHandler.RemoveBySHA256(sha256Hex)
			return err
		}))
	}
	next = server.NewHandler(serverOpts...)

	next = handlers.CombinedLoggingHandler(os.Stdout, next)

	err = http.ListenAndServe(*addr, next)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}
