package main

import (
	"context"
	"crypto/subtle"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gorilla/handlers"
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/server/hf"
	"github.com/wzshiming/xet/server/internalapi"
	"github.com/wzshiming/xet/storage"
)

func main() {
	// Parse command line flags
	addr := flag.String("addr", ":8080", "Server address (host:port)")
	storageDir := flag.String("storage", "./xet-data", "Storage directory for xorbs and shards")
	baseURL := flag.String("base-url", "", "Base URL for serving xorb data (optional)")
	signingKey := flag.String("signing-key", "", "HMAC signing key for CAS tokens (optional; unset leaves CAS routes open, xorb and bridge downloads remain anonymous)")
	internalToken := flag.String("internal-token", "", "Bearer token accepted only by /internal/ endpoints")
	internalAPI := flag.Bool("internal", false, "Enable /internal/ endpoints (requires -internal-token)")
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
	if *internalAPI && *internalToken == "" {
		fmt.Fprintln(os.Stderr, "-internal requires -internal-token")
		os.Exit(1)
	}

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

	issuer, err := auth.NewIssuer([]byte(*signingKey), 15*time.Minute, time.Now)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create token issuer: %v\n", err)
		os.Exit(1)
	}

	var authorizer auth.Authorizer
	if *signingKey != "" {
		authorizer = issuer
		fmt.Println("CAS authentication enabled")
	} else {
		fmt.Println("WARNING: CAS authentication is disabled.")
	}

	var next http.Handler

	if *upstream != "" {
		// Mirror mode: full-cache middle layer in front of the upstream hub.
		// The hub front end handles resolve/token/tree requests through the
		// mirror engine and proxies the rest to the upstream.
		xetClient, err := client.NewClient(
			client.WithCacheDir(filepath.Join(*storageDir, "mirror", "chunks")),
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create xet client: %v\n", err)
			os.Exit(1)
		}

		next, err = hf.NewUpstreamProxy(*upstream, *upstreamToken)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create upstream proxy: %v\n", err)
			os.Exit(1)
		}

		mir, err := mirror.NewMirror(
			mirror.WithStorage(stor),
			mirror.WithUpstream(*upstream),
			mirror.WithUpstreamToken(*upstreamToken),
			mirror.WithCacheDir(filepath.Join(*storageDir, "mirror")),
			mirror.WithClient(xetClient),
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create mirror: %v\n", err)
			os.Exit(1)
		}

		next = hf.NewHandler(
			hf.WithMirror(mir),
			hf.WithExternalURL(*baseURL),
			hf.WithMinter(hf.MinterFunc(func(r *http.Request, req hf.TokenRequest) (string, int64, error) {
				if req.Permission != auth.Read {
					return "", 0, hf.ErrNotHandled
				}
				return issuer.Sign(auth.Grant{Permission: auth.Read, File: req.File})
			})),
			hf.WithNext(next),
		)

		fmt.Printf("Mirror mode enabled, upstream: %s\n", *upstream)
	}

	// Create server
	next = server.NewHandler(
		server.WithStorage(stor),
		server.WithAuthorizer(authorizer),
		server.WithNext(next),
	)

	if *internalAPI {
		next = internalapi.NewHandler(
			internalapi.WithStorage(stor),
			internalapi.WithAuthorizer(auth.AuthorizerFunc(func(r *http.Request, _ auth.Grant) error {
				token, ok := auth.BearerToken(r)
				if !ok || subtle.ConstantTimeCompare([]byte(token), []byte(*internalToken)) != 1 {
					return auth.ErrUnauthenticated
				}
				return nil
			})),
			internalapi.WithGCGrace(1*time.Hour),
			internalapi.WithGCAnchor(storage.AnchorBoth),
			internalapi.WithNext(next),
		)
		fmt.Println("Internal management endpoints enabled at /internal/")
	}

	next = handlers.CombinedLoggingHandler(os.Stdout, next)

	err = http.ListenAndServe(*addr, next)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}
