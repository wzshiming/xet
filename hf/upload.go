package hf

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/lfs"
)

// UploadResult describes the outcome of an upload negotiation.
type UploadResult struct {
	// FileHash is the xet hash of the uploaded content. It is zero when
	// AlreadyExists is true because no upload took place.
	FileHash xet.FileHash
	// OID is the lowercase hex SHA-256 of the content (the Git LFS oid).
	OID string
	// Size is the content size in bytes.
	Size int64
	// AlreadyExists reports that the Hub already has the object and the
	// upload was skipped.
	AlreadyExists bool
}

// Upload uploads content to a Hugging Face repository over the xet protocol:
// it negotiates a Git LFS batch with the xet transfer, uploads chunks to the
// CAS endpoint from the batch response, and verifies the object with the Hub.
// The content is read twice (once to compute the LFS oid, once to upload), so
// r must support seeking; the read starts at the current offset.
func Upload(ctx context.Context, target Target, hfToken string, r io.ReadSeeker, opts ...client.Options) (UploadResult, error) {
	target = normalizeTarget(target)

	oid, size, err := computeOIDAndSize(r)
	if err != nil {
		return UploadResult{}, err
	}
	result := UploadResult{OID: oid, Size: size}

	obj := lfs.BatchObject{OID: oid, Size: size}
	batchResults, err := lfs.ResolveOIDUpload(ctx, hfToken, lfs.Target{
		Endpoint: target.Endpoint,
		RepoType: target.RepoType,
		RepoID:   target.RepoID,
		Revision: target.Revision,
	}, obj)
	if err != nil {
		return UploadResult{}, fmt.Errorf("negotiate LFS upload: %w", err)
	}
	if len(batchResults) == 0 {
		return UploadResult{}, fmt.Errorf("no batch results returned from LFS batch API")
	}
	batchResult := batchResults[0]

	if batchResult.Upload == nil {
		result.AlreadyExists = true
		return result, nil
	}

	cli, err := client.NewClient(opts...)
	if err != nil {
		return UploadResult{}, fmt.Errorf("create client: %w", err)
	}

	provider := uploadAuthProvider(batchResult.Upload, target, hfToken)
	fileHash, err := cli.UploadFileWithAuthProvider(ctx, provider, r)
	if err != nil {
		return UploadResult{}, fmt.Errorf("upload file: %w", err)
	}
	result.FileHash = fileHash

	if batchResult.Verify != nil {
		if err := lfs.VerifyObject(ctx, batchResult.Verify, obj); err != nil {
			return UploadResult{}, fmt.Errorf("verify LFS object: %w", err)
		}
	}

	return result, nil
}

// UploadFile uploads the named file via Upload.
func UploadFile(ctx context.Context, target Target, hfToken string, filename string, opts ...client.Options) (UploadResult, error) {
	f, err := os.Open(filename)
	if err != nil {
		return UploadResult{}, fmt.Errorf("open input file: %w", err)
	}
	defer f.Close()

	return Upload(ctx, target, hfToken, f, opts...)
}

// uploadAuthProvider prefers the session credentials carried in the batch
// upload action and falls back to the repository's xet-write-token endpoint
// when they are absent.
func uploadAuthProvider(action *lfs.Action, target Target, hfToken string) client.AuthProvider {
	casURL := action.Header["X-Xet-Cas-Url"]
	casToken := action.Header["X-Xet-Access-Token"]
	if casURL != "" && casToken != "" {
		return client.StaticAuthProvider(casURL, casToken)
	}
	return NewWriteTokenProvider(nil, target, hfToken)
}

// computeOIDAndSize hashes r from its current offset to EOF and restores the
// offset afterwards.
func computeOIDAndSize(r io.ReadSeeker) (string, int64, error) {
	start, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", 0, fmt.Errorf("locate input offset: %w", err)
	}

	h := sha256.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return "", 0, fmt.Errorf("hash input: %w", err)
	}

	if _, err := r.Seek(start, io.SeekStart); err != nil {
		return "", 0, fmt.Errorf("rewind input: %w", err)
	}

	return hex.EncodeToString(h.Sum(nil)), n, nil
}
