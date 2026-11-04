//go:build !unix

package server_test

import "testing"

// readRusage has no getrusage to read here, so the rusage metrics report zero.
func readRusage(testing.TB) rusage { return rusage{} }
