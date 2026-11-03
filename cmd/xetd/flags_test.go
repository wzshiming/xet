package main

import (
	"strings"
	"testing"
)

// Presigning covers xorb upload grants as well as downloads, and the presign
// flags' help says so.
func TestXetdPresignFlagsHelpCoversUploadGrants(t *testing.T) {
	output, err := xetdCommand(t, "-h").CombinedOutput()
	if err != nil {
		t.Fatalf("xetd -h = %v, output = %s", err, output)
	}
	help := map[string]string{}
	// PrintDefaults starts each flag on a "  -name [type]" line, its help indented below.
	for _, entry := range strings.Split(string(output), "\n  -")[1:] {
		head, usage, _ := strings.Cut(entry, "\n")
		name, _, _ := strings.Cut(head, " ")
		help[name] = strings.TrimSpace(usage)
	}
	for _, name := range []string{"s3-presign", "s3-presign-expiry"} {
		if !strings.Contains(strings.ToLower(help[name]), "upload") {
			t.Errorf("-%s help = %q, want it to say presigning also covers xorb upload grants", name, help[name])
		}
	}
}
