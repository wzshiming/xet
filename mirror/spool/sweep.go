package spool

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/wzshiming/xet/storage"
)

// SweepResult reports one Sweep pass; in a dry run the counts are what a real pass would remove.
type SweepResult struct {
	DryRun         bool  `json:"dry_run"`
	SweptSpools    int   `json:"swept_spools"`
	ReclaimedBytes int64 `json:"reclaimed_bytes"`
}

// Sweep removes spool files that no flight holds and that were last written before opts.Grace (zero: storage.DefaultSweepGrace; negative: any age); opts.DryRun only reports, a spool that cannot be unlinked is left uncounted for the next pass, and Anchor, MaxDeletes and Budget do not apply.
func (s *Spool) Sweep(ctx context.Context, opts storage.SweepOptions) (SweepResult, error) {
	res := SweepResult{DryRun: opts.DryRun}
	grace := opts.Grace
	if grace == 0 {
		grace = storage.DefaultSweepGrace
	}
	s.openMu.Lock()
	defer s.openMu.Unlock()

	// Spools of finished flights stay deletable while readers drain them: their open descriptors outlive the unlink.
	s.mu.Lock()
	inUse := make(map[string]bool, len(s.flights))
	for p := range s.flights {
		inUse[p] = true
	}
	s.mu.Unlock()

	err := s.walkSpools(ctx, func(p string, info fs.FileInfo) error {
		if inUse[p] || grace > 0 && time.Since(info.ModTime()) < grace {
			return nil
		}
		// An open handle on Windows, or a permission denial, leaves the file for the next pass.
		if !opts.DryRun && os.Remove(p) != nil {
			return nil
		}
		res.SweptSpools++
		res.ReclaimedBytes += info.Size()
		return nil
	})
	return res, err
}

// walkSpools calls fn for each regular .spool file in the directory; a missing directory holds none and a file removed mid-walk is skipped.
func (s *Spool) walkSpools(ctx context.Context, fn func(path string, info fs.FileInfo) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ents, err := os.ReadDir(s.dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, ent := range ents {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !ent.Type().IsRegular() || !strings.HasSuffix(ent.Name(), ".spool") {
			continue
		}
		info, err := ent.Info()
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if err := fn(filepath.Join(s.dir, ent.Name()), info); err != nil {
			return err
		}
	}
	return nil
}
