package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/crc64nvme"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/xorb"
)

// GrantXorbUpload issues a grant whose upload goes through the CAS server, or
// with presign on a presigned PutObject of exactly the declared length and
// CRC-64/NVME to a staging key not yet written. A presigned grant keeps no
// registry entry, so PutStagedXorb refuses its id.
func (ss *Storage) GrantXorbUpload(ctx context.Context, xorbHash xet.XorbHash, length int64, checksum uint64) (*storage.XorbUploadGrant, error) {
	if !ss.presign {
		id := ss.grants.Issue(xorbHash, length, checksum)
		return &storage.XorbUploadGrant{
			ID:      id,
			Method:  http.MethodPut,
			URL:     ss.baseURL + storage.XorbUploadPath(id),
			Headers: map[string]string{},
		}, nil
	}

	id := storage.NewGrantID()
	req, err := ss.uploadPresignClient.PresignPutObject(ctx, &awss3.PutObjectInput{
		Bucket:            aws.String(ss.bucket),
		Key:               aws.String(ss.objectKey("staging", id)),
		ContentLength:     aws.Int64(length),
		IfNoneMatch:       aws.String("*"),
		ChecksumCRC64NVME: aws.String(crc64nvme.Base64(checksum)),
	})
	if err != nil {
		return nil, fmt.Errorf("presign xorb upload: %w", err)
	}
	headers := make(map[string]string, len(req.SignedHeader))
	for name, values := range req.SignedHeader {
		// The client's HTTP stack sends Host from the URL.
		if name = strings.ToLower(name); name != "host" {
			headers[name] = strings.Join(values, ",")
		}
	}
	return &storage.XorbUploadGrant{ID: id, Method: req.Method, URL: req.URL, Headers: headers}, nil
}

// PutStagedXorb spools at most the granted length plus one byte to a
// temporary file and uploads it in one PutObject once the length and
// CRC-64/NVME match the grant.
func (ss *Storage) PutStagedXorb(ctx context.Context, grantID string, r io.Reader) error {
	// Only issued, well-formed ids pass Lookup, so grantID is safe in a key below.
	grant, err := ss.grants.Lookup(grantID)
	if err != nil {
		return err
	}
	key := ss.objectKey("staging", grantID)
	if _, exists, err := ss.headObject(ctx, key); err != nil {
		return fmt.Errorf("check staged xorb: %w", err)
	} else if exists {
		return storage.ErrStagedXorbExists
	}

	f, err := os.CreateTemp("", "xet-staged-*")
	if err != nil {
		return fmt.Errorf("create staged xorb spool file: %w", err)
	}
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	sum := crc64nvme.New()
	n, err := io.Copy(io.MultiWriter(f, sum), io.LimitReader(r, grant.Length+1))
	if err != nil {
		return fmt.Errorf("receive staged xorb: %w", err)
	}
	if n != grant.Length || sum.Sum64() != grant.Checksum {
		return fmt.Errorf("%w: got %d bytes with crc64nvme %d, want %d bytes with %d", storage.ErrStagedXorbMismatch, n, sum.Sum64(), grant.Length, grant.Checksum)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind staged xorb spool file: %w", err)
	}
	if err := ss.putObject(ctx, key, f); err != nil {
		return fmt.Errorf("upload staged xorb: %w", err)
	}
	return nil
}

// CommitStagedXorb validates a copy of the staged upload as xorbHash, copies it
// on to the xorb key server-side and deletes both staging objects.
func (ss *Storage) CommitStagedXorb(ctx context.Context, grantID string, xorbHash xet.XorbHash) (bool, error) {
	if !storage.ValidGrantID(grantID) {
		return false, storage.ErrStagedXorbNotFound
	}
	// A presigned URL can rewrite the upload until it expires, so validate and publish a private copy.
	copyID := storage.NewGrantID()
	copyKey := ss.objectKey("staging", copyID)
	if err := ss.copyObject(ctx, ss.objectKey("staging", grantID), copyKey); err != nil {
		if isS3NotFound(err) {
			return false, storage.ErrStagedXorbNotFound
		}
		return false, fmt.Errorf("copy staged xorb: %w", err)
	}
	// A client disconnect must not orphan the staged objects.
	cleanupCtx := context.WithoutCancel(ctx)
	defer ss.DeleteStagedXorb(cleanupCtx, copyID)

	exists, err := ss.HasXorb(ctx, xorbHash)
	if err != nil {
		return false, err
	}
	if exists {
		_ = ss.dropStaged(cleanupCtx, grantID)
		return false, nil
	}
	staged, err := ss.getObjectReader(ctx, copyKey)
	if err != nil {
		// A sweep may reclaim the copy before the GET.
		if isS3NotFound(err) {
			return false, storage.ErrStagedXorbNotFound
		}
		return false, fmt.Errorf("open staged xorb: %w", err)
	}
	defer staged.Close()
	body := &stagedBody{r: staged}
	err = xorb.Validate(body, xorbHash)
	if err == nil {
		// Validate stops at a footer; reading on to EOF finds trailing bytes, Content-Length or not.
		xorbSize := body.n
		if _, err = io.Copy(io.Discard, body); err == nil && body.n > xorbSize {
			err = fmt.Errorf("%d trailing bytes after the xorb", body.n-xorbSize)
		}
	}
	if body.err != nil {
		return false, fmt.Errorf("read staged xorb: %w", body.err)
	}
	if err != nil {
		return false, errors.Join(fmt.Errorf("%w: %w", storage.ErrStagedXorbMismatch, err), ss.dropStaged(cleanupCtx, grantID))
	}

	if err := ss.copyObject(ctx, copyKey, ss.objectKey("xorbs", xorbHash.String())); err != nil {
		// A sweep may reclaim the copy after the GET.
		if isS3NotFound(err) {
			return false, storage.ErrStagedXorbNotFound
		}
		return false, fmt.Errorf("publish staged xorb: %w", err)
	}
	// Published: staged objects the deletes leave behind are the staging sweep's.
	_ = ss.dropStaged(cleanupCtx, grantID)
	return true, nil
}

// stagedBody counts the staged bytes read and keeps the first transfer error,
// so a broken GET never passes for bad content.
type stagedBody struct {
	r   io.Reader
	n   int64
	err error
}

func (b *stagedBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n += int64(n)
	if err != nil && err != io.EOF && b.err == nil {
		b.err = err
	}
	return n, err
}

// dropStaged forgets the grant and deletes its staged upload.
func (ss *Storage) dropStaged(ctx context.Context, grantID string) error {
	ss.grants.Remove(grantID)
	return ss.DeleteStagedXorb(ctx, grantID)
}

// WalkStagedXorbs calls fn for every staged upload; uploads land in one
// PutObject, so every listed object is complete.
func (ss *Storage) WalkStagedXorbs(ctx context.Context, fn func(grantID string, size int64, modTime time.Time) error) error {
	base := "staging/"
	if ss.prefix != "" {
		base = ss.prefix + "/" + base
	}
	paginator := awss3.NewListObjectsV2Paginator(ss.client, &awss3.ListObjectsV2Input{
		Bucket: aws.String(ss.bucket),
		Prefix: aws.String(base),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list staging: %w", err)
		}
		for _, obj := range page.Contents {
			grantID := strings.ReplaceAll(strings.TrimPrefix(aws.ToString(obj.Key), base), "/", "")
			if !storage.ValidGrantID(grantID) {
				continue
			}
			var modTime time.Time
			if obj.LastModified != nil {
				modTime = *obj.LastModified
			}
			if err := fn(grantID, aws.ToInt64(obj.Size), modTime); err != nil {
				return err
			}
		}
	}
	return nil
}

// DeleteStagedXorb removes a staged upload; a missing one is not an error.
func (ss *Storage) DeleteStagedXorb(ctx context.Context, grantID string) error {
	if !storage.ValidGrantID(grantID) {
		return nil
	}
	if err := ss.deleteObject(ctx, ss.objectKey("staging", grantID)); err != nil {
		return fmt.Errorf("delete staged xorb: %w", err)
	}
	return nil
}
