package main

import (
	"context"
	"path/filepath"

	"github.com/wzshiming/xet/test/benchgate/sample"
	"github.com/wzshiming/xet/test/benchgate/verdict"
)

// result is what a gate run found.
type result struct {
	first     pass
	confirmed []finding
}

// pass is a sampling of groups and the comparison of each group with samples.
type pass struct {
	sample.Result
	verdicts map[string]verdict.Result // by group name
}

// finding is a row of a group.
type finding struct {
	group string
	row   verdict.Row
}

// gate samples the configured groups of base and head into out and judges them.
func gate(ctx context.Context, sampleRun sampler, cfg config, base, head, out string) (result, error) {
	req := sample.Request{Base: base, Head: head, MeasureIdentical: cfg.MeasureIdentical, Out: filepath.Join(out, "first")}
	for _, g := range cfg.Groups {
		req.Groups = append(req.Groups, sample.Group{Name: g.Name, Packages: g.Packages, Bench: g.Bench, Benchtime: g.Benchtime, Rounds: g.Rounds})
	}
	first, err := measure(ctx, sampleRun, cfg, req)
	if err != nil {
		return result{}, err
	}
	res := result{first: first}
	for _, g := range cfg.Groups {
		for _, row := range first.verdicts[g.Name].Rows {
			if row.Class == verdict.Regression {
				res.confirmed = append(res.confirmed, finding{group: g.Name, row: row})
			}
		}
	}
	return res, nil
}

// measure samples as req says and compares each group with samples under its policy.
func measure(ctx context.Context, sampleRun sampler, cfg config, req sample.Request) (pass, error) {
	res, err := sampleRun(ctx, req)
	if err != nil {
		return pass{}, err
	}
	p := pass{Result: res, verdicts: make(map[string]verdict.Result)}
	for _, g := range res.Groups {
		if g.Base == "" {
			continue
		}
		v, err := verdict.Compare(g.Base, g.Head, cfg.policy(cfg.group(g.Name)))
		if err != nil {
			return pass{}, err
		}
		p.verdicts[g.Name] = v
	}
	return p, nil
}
