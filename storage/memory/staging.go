package memory

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/crc64nvme"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/xorb"
)

// GrantXorbUpload issues a grant whose upload goes through the CAS server.
func (s *Storage) GrantXorbUpload(_ context.Context, xorbHash xet.XorbHash, length int64, checksum uint64) (*storage.XorbUploadGrant, error) {
	id := s.grants.Issue(xorbHash, length, checksum)
	return &storage.XorbUploadGrant{
		ID:      id,
		Method:  http.MethodPut,
		URL:     s.baseURL + storage.XorbUploadPath(id),
		Headers: map[string]string{},
	}, nil
}

// PutStagedXorb buffers at most the granted length plus one byte and stores
// the buffer whole once the length and CRC-64/NVME match the grant.
func (s *Storage) PutStagedXorb(_ context.Context, grantID string, r io.Reader) error {
	grant, err := s.grants.Lookup(grantID)
	if err != nil {
		return err
	}
	if _, exists := s.get("staging", grantID); exists {
		return storage.ErrStagedXorbExists
	}
	var buf bytes.Buffer
	sum := crc64nvme.New()
	n, err := io.Copy(io.MultiWriter(&buf, sum), io.LimitReader(r, grant.Length+1))
	if err != nil {
		return fmt.Errorf("receive staged xorb: %w", err)
	}
	if n != grant.Length || sum.Sum64() != grant.Checksum {
		return fmt.Errorf("%w: got %d bytes with crc64nvme %d, want %d bytes with %d", storage.ErrStagedXorbMismatch, n, sum.Sum64(), grant.Length, grant.Checksum)
	}
	if !s.put("staging", grantID, buf.Bytes(), false) {
		return storage.ErrStagedXorbExists
	}
	return nil
}

// CommitStagedXorb validates the staged upload as xorbHash and stores it as
// that xorb; the staged upload and its grant go either way.
func (s *Storage) CommitStagedXorb(_ context.Context, grantID string, xorbHash xet.XorbHash) (bool, error) {
	staged, ok := s.get("staging", grantID)
	if !ok {
		return false, storage.ErrStagedXorbNotFound
	}
	defer func() {
		s.grants.Remove(grantID)
		s.delete("staging", grantID)
	}()
	if _, exists := s.get("xorbs", xorbHash.String()); exists {
		return false, nil
	}
	r := bytes.NewReader(staged.data)
	err := xorb.Validate(r, xorbHash)
	// Validate stops at a footer, leaving any staged bytes past it unread.
	if err == nil && r.Len() > 0 {
		err = fmt.Errorf("%d trailing bytes after the xorb", r.Len())
	}
	if err != nil {
		return false, fmt.Errorf("%w: %w", storage.ErrStagedXorbMismatch, err)
	}
	// A sweep may reclaim the upload after the commit read it.
	if !s.delete("staging", grantID) {
		return false, storage.ErrStagedXorbNotFound
	}
	return s.put("xorbs", xorbHash.String(), staged.data, false), nil
}

// WalkStagedXorbs calls fn for every staged upload.
func (s *Storage) WalkStagedXorbs(ctx context.Context, fn func(grantID string, size int64, modTime time.Time) error) error {
	return s.walkObjects(ctx, "staging", fn)
}

// DeleteStagedXorb removes a staged upload; a missing one is not an error.
func (s *Storage) DeleteStagedXorb(_ context.Context, grantID string) error {
	s.delete("staging", grantID)
	return nil
}
