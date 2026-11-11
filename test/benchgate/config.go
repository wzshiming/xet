package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/wzshiming/xet/test/benchgate/verdict"
)

// config is the gate's configuration file.
type config struct {
	Alpha            float64 `json:"alpha"`            // significance level
	MinSamples       int     `json:"minSamples"`       // samples each side needs before a row is judged
	MeasureIdentical bool    `json:"measureIdentical"` // also measure packages whose test binaries are identical
	Groups           []group `json:"groups"`
}

// group is a set of benchmarks sampled and judged together.
type group struct {
	Name       string             `json:"name"`
	Packages   []string           `json:"packages"`   // directories relative to the tree roots
	Bench      string             `json:"bench"`      // regular expression selecting benchmarks, as for go test -bench
	Benchtime  string             `json:"benchtime"`  // as for go test -benchtime, empty for Go's default
	Rounds     int                `json:"rounds"`     // samples of each row on each side
	Thresholds map[string]float64 `json:"thresholds"` // judged metric to its least relative increase, 0.1 for 10%
	ReportOnly bool               `json:"reportOnly"` // compared without being judged, in place of thresholds
}

func loadConfig(path string) (config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return config{}, err
	}
	var c config
	if err := json.Unmarshal(data, &c); err != nil {
		return config{}, fmt.Errorf("%s: %w", path, err)
	}
	return c, nil
}

// group is the group named name.
func (c config) group(name string) group {
	for _, g := range c.Groups {
		if g.Name == name {
			return g
		}
	}
	return group{}
}

// policy is how group g is judged.
func (c config) policy(g group) verdict.Policy {
	return verdict.Policy{Thresholds: g.Thresholds, Alpha: c.Alpha, MinSamples: c.MinSamples}
}
