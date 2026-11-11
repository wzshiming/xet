package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/wzshiming/xet/test/benchgate/verdict"
)

// judgedMetrics are the metrics a threshold may be set for.
var judgedMetrics = []string{"sec/op", "B/op", "allocs/op"}

// minJudgedRounds is the fewest rounds of a judged group.
const minJudgedRounds = 10

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
	f, err := os.Open(path)
	if err != nil {
		return config{}, err
	}
	defer f.Close()
	var c config
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return config{}, fmt.Errorf("%s: %w", path, err)
	}
	if err := c.validate(); err != nil {
		return config{}, fmt.Errorf("%s: %w", path, err)
	}
	return c, nil
}

func (c config) validate() error {
	switch {
	case c.Alpha <= 0 || c.Alpha >= 1:
		return fmt.Errorf("alpha: %v, want more than 0 and less than 1", c.Alpha)
	case c.MinSamples < 1:
		return fmt.Errorf("minSamples: %d, want at least 1", c.MinSamples)
	case len(c.Groups) == 0:
		return errors.New("groups: none")
	}
	for i, g := range c.Groups {
		if err := g.validate(c.MinSamples); err != nil {
			return fmt.Errorf("groups[%d].%w", i, err)
		}
	}
	return nil
}

// validate checks g, whose rows are judged only with minSamples samples on each side.
func (g group) validate(minSamples int) error {
	switch {
	case len(g.Packages) == 0:
		return errors.New("packages: none")
	case g.Bench == "":
		return errors.New("bench: empty, which selects no benchmark")
	case g.ReportOnly && len(g.Thresholds) > 0:
		return errors.New("thresholds: set in a reportOnly group")
	case !g.ReportOnly && len(g.Thresholds) == 0:
		return errors.New("thresholds: none, want some or reportOnly")
	}
	if _, err := regexp.Compile(g.Bench); err != nil {
		return fmt.Errorf("bench: %w", err)
	}
	for _, m := range slices.Sorted(maps.Keys(g.Thresholds)) {
		switch t := g.Thresholds[m]; {
		case !slices.Contains(judgedMetrics, m):
			return fmt.Errorf("thresholds[%q]: not a metric the gate judges, want one of %s", m, strings.Join(judgedMetrics, ", "))
		case t < 0:
			return fmt.Errorf("thresholds[%q]: %v, want at least 0", m, t)
		}
	}
	switch {
	case g.ReportOnly:
	case g.Rounds < minJudgedRounds:
		return fmt.Errorf("rounds: %d, want at least %d in a judged group", g.Rounds, minJudgedRounds)
	case g.Rounds < minSamples:
		return fmt.Errorf("rounds: %d, fewer than minSamples %d, so no row would be judged", g.Rounds, minSamples)
	}
	return nil
}

// only keeps just the groups named in names.
func (c *config) only(names []string) error {
	for _, n := range names {
		if !slices.ContainsFunc(c.Groups, func(g group) bool { return g.Name == n }) {
			return fmt.Errorf("-groups: no group %q in the configuration", n)
		}
	}
	c.Groups = slices.DeleteFunc(c.Groups, func(g group) bool { return !slices.Contains(names, g.Name) })
	return nil
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
