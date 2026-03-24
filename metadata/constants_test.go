package metadata

import (
	"testing"

	"github.com/wzshiming/xet/merklehash"
)

func TestHashIsGlobalDedupEligible(t *testing.T) {
	// Hash where d[3] % 1024 == 0 should be eligible
	eligible := merklehash.DataHash{0, 0, 0, 2048}
	if !HashIsGlobalDedupEligible(eligible) {
		t.Fatal("expected eligible (2048 % 1024 == 0)")
	}

	// Hash where d[3] == 0 should be eligible
	zero := merklehash.DataHash{}
	if !HashIsGlobalDedupEligible(zero) {
		t.Fatal("expected eligible (0 % 1024 == 0)")
	}

	// Hash where d[3] % 1024 != 0 should not be eligible
	ineligible := merklehash.DataHash{0, 0, 0, 1}
	if HashIsGlobalDedupEligible(ineligible) {
		t.Fatal("expected ineligible (1 % 1024 != 0)")
	}

	// Another ineligible hash
	ineligible2 := merklehash.DataHash{100, 200, 300, 1023}
	if HashIsGlobalDedupEligible(ineligible2) {
		t.Fatal("expected ineligible (1023 % 1024 != 0)")
	}

	// Eligible with exact modulus
	eligible2 := merklehash.DataHash{100, 200, 300, 1024}
	if !HashIsGlobalDedupEligible(eligible2) {
		t.Fatal("expected eligible (1024 % 1024 == 0)")
	}
}
