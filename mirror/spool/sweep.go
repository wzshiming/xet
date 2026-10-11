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

// SweepOptions configures one Sweep pass; a zero Grace means
// storage.DefaultSweepGrace, a negative one disables the window.
type SweepOptions struct {
	Grace  time.Duration // shields spools written (mtime) within the window; negative disables it
	DryRun bool          // report removable spools without deleting
	Budget time.Duration // wall-clock cap per pass, checked before each unlink once anything was swept; 0 = unlimited, ignored by dry runs
}

// SweepResult reports one Sweep pass; in a dry run the counts are what a real pass would remove.
type SweepResult struct {
	DryRun         bool  `json:"dry_run"`
	SweptSpools    int   `json:"swept_spools"`
	ReclaimedBytes int64 `json:"reclaimed_bytes"`

	// Done reports a finished pass; RemainingSpools counts idle spools past the grace it did not reach, 0 whenever Done.
	Done            bool `json:"done"`
	RemainingSpools int  `json:"remaining_spools"`
}

// Sweep runs one stateless pass removing spool files that no flight holds and that were last
// written before opts.Grace (zero: storage.DefaultSweepGrace; negative: any age); repeat until Done.
// opts.DryRun only reports. opts.Budget caps the wall clock spent unlinking once a spool was
// swept, so every bounded pass makes progress. A spool that cannot be unlinked is left uncounted
// for the next pass.
func (s *Spool) Sweep(ctx context.Context, opts SweepOptions) (SweepResult, error) {
	res := SweepResult{DryRun: opts.DryRun}
	grace := opts.Grace
	if grace == 0 {
		grace = storage.DefaultSweepGrace
	}
	budget := opts.Budget
	if opts.DryRun {
		budget = 0
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

	type candidate struct {
		path string
		size int64
	}
	var cands []candidate
	err := s.walkSpools(ctx, func(p string, info fs.FileInfo) error {
		if !inUse[p] && (grace <= 0 || time.Since(info.ModTime()) >= grace) {
			cands = append(cands, candidate{p, info.Size()})
		}
		return nil
	})
	if err != nil {
		return res, err
	}

	// The walk is mark work, uncharged to Budget.
	start := time.Now()
	swept := 0
	exhausted := func() bool {
		return budget > 0 && swept > 0 && time.Since(start) >= budget
	}
	for i, c := range cands {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		if exhausted() {
			res.RemainingSpools = len(cands) - i
			return res, nil
		}
		if !opts.DryRun {
			// An open handle on Windows, or a permission denial, leaves the file for the next pass.
			if os.Remove(c.path) != nil {
				continue
			}
			swept++
		}
		res.SweptSpools++
		res.ReclaimedBytes += c.size
	}
	res.Done = true
	return res, nil
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
