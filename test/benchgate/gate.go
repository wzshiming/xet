package main

import (
	"context"
	"path/filepath"
	"slices"

	"github.com/wzshiming/xet/test/benchgate/sample"
	"github.com/wzshiming/xet/test/benchgate/verdict"
)

// result is what a gate run found.
type result struct {
	first, recheck pass
	suspects       []suspect
}

// pass is a sampling of groups and the comparison of each group with samples.
type pass struct {
	sample.Result
	verdicts map[string]verdict.Result // by group name
}

// files are the sample files of group name, empty without samples.
func (p pass) files(name string) (base, head string) {
	for _, g := range p.Groups {
		if g.Name == name {
			return g.Base, g.Head
		}
	}
	return "", ""
}

// suspect is a row of a group that regressed in the first pass.
type suspect struct {
	group          string
	first, recheck verdict.Row // recheck is zero when the re-check has no such row
}

// split divides the suspects into those the re-check found regressed too and the others.
func (r result) split() (confirmed, excluded []suspect) {
	for _, s := range r.suspects {
		if s.recheck.Class == verdict.Regression {
			confirmed = append(confirmed, s)
		} else {
			excluded = append(excluded, s)
		}
	}
	return confirmed, excluded
}

// gate samples the configured groups of base and head into out and judges them, then samples the
// rows that regressed again and judges those samples alone.
func gate(ctx context.Context, sampleRun sampler, cfg config, base, head, out string) (result, error) {
	req := sample.Request{Base: base, Head: head, MeasureIdentical: cfg.MeasureIdentical, Out: filepath.Join(out, "first")}
	for _, g := range cfg.Groups {
		req.Groups = append(req.Groups, sample.Group{Name: g.Name, Packages: g.Packages, Bench: g.Bench, Benchtime: g.Benchtime, Rounds: g.Rounds})
	}
	var res result
	var err error
	if res.first, err = measure(ctx, sampleRun, cfg, req); err != nil {
		return result{}, err
	}

	recheck := req
	recheck.Groups, recheck.Out = nil, filepath.Join(out, "recheck")
	for _, g := range req.Groups {
		var rows []sample.Row
		for _, row := range res.first.verdicts[g.Name].Rows {
			if row.Class != verdict.Regression {
				continue
			}
			res.suspects = append(res.suspects, suspect{group: g.Name, first: row})
			if r := (sample.Row{Pkg: row.Pkg, Name: row.Name}); !slices.Contains(rows, r) {
				rows = append(rows, r)
			}
		}
		if len(rows) > 0 {
			g.Rows = rows
			recheck.Groups = append(recheck.Groups, g)
		}
	}
	if len(res.suspects) == 0 {
		return res, nil
	}
	if res.recheck, err = measure(ctx, sampleRun, cfg, recheck); err != nil {
		return result{}, err
	}
	for i, s := range res.suspects {
		for _, row := range res.recheck.verdicts[s.group].Rows {
			if row.Key == s.first.Key {
				res.suspects[i].recheck = row
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
