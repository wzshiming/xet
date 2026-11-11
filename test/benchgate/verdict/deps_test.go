package verdict_test

import (
	"encoding/json"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

func TestDependsOnlyOnPerfAndStd(t *testing.T) {
	out, err := exec.Command("go", "mod", "edit", "-json").Output()
	if err != nil {
		t.Fatal(err)
	}
	var mod struct {
		Require []struct {
			Path     string
			Indirect bool
		}
	}
	if err := json.Unmarshal(out, &mod); err != nil {
		t.Fatal(err)
	}
	var direct []string
	for _, r := range mod.Require {
		if !r.Indirect {
			direct = append(direct, r.Path)
		}
	}
	if !slices.Equal(direct, []string{"golang.org/x/perf"}) {
		t.Errorf("go.mod requires %q directly, want golang.org/x/perf alone", direct)
	}

	out, err = exec.Command("go", "list", "-f", `{{join .Imports " "}}`, ".").Output()
	if err != nil {
		t.Fatal(err)
	}
	for path := range strings.FieldsSeq(string(out)) {
		elem, _, _ := strings.Cut(path, "/")
		std := !strings.Contains(elem, ".")
		public := strings.HasPrefix(path, "golang.org/x/perf/") && !strings.Contains(path, "/internal")
		if !std && !public {
			t.Errorf("verdict imports %s, want only the standard library and public golang.org/x/perf packages", path)
		}
	}
}
