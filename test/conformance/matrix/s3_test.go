package matrix_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	xetdownload "github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/test/conformance/rustref"
)

// startGoServerS3 starts the Go server backed by S3Storage so the matrix also
// covers the S3 read/write paths. Presigning is disabled: presigned transfers
// go straight to the object store and would bypass the recording proxy. This
// keeps every request on the recorded wire for both clients alike, so
// comparisons stay symmetric; the presigned paths are covered by
// TestCompatibilityS3PresignedDownload.
func startGoServerS3(t *testing.T) runningServer {
	t.Helper()
	running, _ := newS3BackedServer(t, false)
	return running
}

// newS3BackedServer returns a recording Go server over an in-process gofakes3
// plus the fake S3 endpoint that presigned URLs must point at.
func newS3BackedServer(t *testing.T, presign bool) (runningServer, string) {
	t.Helper()

	const bucket = "conformance"
	backend := s3mem.New()
	if err := backend.CreateBucket(bucket); err != nil {
		t.Fatalf("create fake S3 bucket: %v", err)
	}
	s3Server := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(s3Server.Close)

	s3Client := s3.New(s3.Options{
		BaseEndpoint: aws.String(s3Server.URL),
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		UsePathStyle: true,
		// Keep request bodies un-chunked for the fake server.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	stor, err := storage.NewS3Storage(context.Background(),
		storage.WithS3Client(s3Client),
		storage.WithS3Bucket(bucket),
		storage.WithS3Presign(presign),
	)
	if err != nil {
		t.Fatalf("create S3 storage: %v", err)
	}

	proxy := &RecordingProxy{backend: server.NewHandler(server.WithStorage(stor))}
	httpServer := httptest.NewServer(proxy)
	t.Cleanup(httpServer.Close)
	return runningServer{endpoint: httpServer.URL, proxy: proxy}, s3Server.URL
}

// TestCompatibilityS3PresignedDownload proves the xet-core client follows the
// presigned S3 fetch-info URLs the S3-backed Go server hands out: uploads go
// through the Go client (which handles the direct-upload redirect to S3), the
// reconstruction is asserted to reference only presigned object-store URLs,
// and the download runs with the reference client for both protocol versions.
func TestCompatibilityS3PresignedDownload(t *testing.T) {
	fixtures := wireFixtures()
	for _, protocol := range []rustref.ProtocolVersion{rustref.ProtocolV1, rustref.ProtocolV2} {
		t.Run(protocol.String(), func(t *testing.T) {
			running, s3Endpoint := newS3BackedServer(t, true)
			hashes := upload(t, goClient, protocol, running.endpoint, fixtures)
			if len(hashes) != len(fixtures) {
				t.Fatalf("uploaded %d files, want %d", len(hashes), len(fixtures))
			}

			for i, fx := range fixtures {
				if len(fx.data) == 0 {
					continue
				}
				recon := getJSON[xetdownload.ReconstructionResponseV1](t,
					running.endpoint+"/v1/reconstructions/"+hashes[i])
				if len(recon.FetchInfo) == 0 {
					t.Fatalf("%s reconstruction has no fetch info", fx.name)
				}
				for xorbHash, entries := range recon.FetchInfo {
					for _, entry := range entries {
						if !strings.HasPrefix(entry.URL, s3Endpoint+"/") || !strings.Contains(entry.URL, "X-Amz-Signature") {
							t.Fatalf("%s xorb %s fetch URL = %q, want presigned URL at %s",
								fx.name, xorbHash, entry.URL, s3Endpoint)
						}
					}
				}
			}

			downloaded := download(t, xetCoreClient, protocol, running.endpoint, hashes, fixtures)
			if len(downloaded) != len(fixtures) {
				t.Fatalf("downloaded %d files, want %d", len(downloaded), len(fixtures))
			}
			for i := range fixtures {
				checkContent(t, fixtures[i].name, downloaded[i], fixtures[i].data)
			}
		})
	}
}
