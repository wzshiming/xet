package merkle

import (
	"testing"

	"github.com/wzshiming/xet/pkg/xet"
)

// These test vectors are from the xet-core Rust implementation's test_correctness test
// in xet_core_structures/src/merklehash/aggregated_hashes.rs.
// All hashes are in XET string format.

type refEntry struct {
	hash string
	size uint64
}

type refTest struct {
	name     string
	entries  []refEntry
	xorbHash string
	fileHash string // with salt=[0;32]
}

var referenceTests = []refTest{
	{
		name:     "single_entry_with_hash",
		entries:  []refEntry{{"cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100}},
		xorbHash: "cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6",
		fileHash: "8e16257caa3fe079d484d872a8975264b2ff683b0d6db9028cc7c0f968a45661",
	},
	{
		name: "three_entries",
		entries: []refEntry{
			{"cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100},
			{"c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200},
			{"0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300},
		},
		xorbHash: "71ec1275fca074724e2dd666921b3277c7cee603e4d025bcab2d4050015be2bc",
		fileHash: "54e55dccc6653c612bdb5576c5d3cb34bb31bc4e100248abccf4c908b3ef7715",
	},
	{
		name: "four_identical_entries",
		entries: []refEntry{
			{"cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100},
			{"cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100},
			{"cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100},
			{"cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100},
		},
		xorbHash: "89f2ada89ff8c96763c6b25010e6dd76a4c05b1466207633ea559acf2093211b",
		fileHash: "2cdba690d0e09563596e0cda626d43eb4c96ef1e994fe72d9b2f5a83cfcd36dd",
	},
	{
		name: "four_mixed_entries",
		entries: []refEntry{
			{"cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100},
			{"c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200},
			{"cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100},
			{"c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200},
		},
		xorbHash: "90f8313ef12df385d237a067aded02562c35ded12116e32eba401dbc86c38031",
		fileHash: "284ea045e5a579e99c21ec597c20de1fc0c09e7168162aac00db8f61b3d82dbb",
	},
	{
		name: "six_entries",
		entries: []refEntry{
			{"cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100},
			{"c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200},
			{"cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100},
			{"c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200},
			{"0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300},
			{"adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784", 400},
		},
		xorbHash: "52c826f99507aa05d0b45e9837fa1709e0485425cfbcb1e80db3905cf98b3ee9",
		fileHash: "91d21684db364c8883ab8209fa5eb2e781cf07f37e1fa43e731c30839afe422f",
	},
	{
		name: "two_entries_with_zero",
		entries: []refEntry{
			{"cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100},
			{"0000000000000000000000000000000000000000000000000000000000000000", 0},
		},
		xorbHash: "e8660f81494ca836a58e395c1395ef97870ed71e2b113eec1fab6b3361f46dd6",
		fileHash: "274d92f7e2acebaa2b8d63c0b5e7a4fc15814a606e3e3825d55609e671bcc5d9",
	},
	{
		name: "eight_entries",
		entries: []refEntry{
			{"0000000000000000000000000000000000000000000000000000000000000000", 0},
			{"cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100},
			{"c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200},
			{"0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300},
			{"adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784", 400},
			{"4ac202caf347fc1e9c874b1ef6a1c5e619141eb775a6f43f0f0124ccd0060d9e", 500},
			{"b3b28636f65c149ea52eb1f94669466f70f033b54cea792824c696ba6ef3c389", 600},
			{"0e2c1a002aae913d2c0fc8ddfa4e9e14b7b311b3b0d458726d5d9f6a6318013c", 700},
		},
		xorbHash: "f62abe77e3fb9c954fe52b0028027ddc90c064c45951a4fd2211d87e5c0011db",
		fileHash: "d1b068be5bbdb38992269e8efe61f601881e39f7a7585fd76883cc6ea5c23b44",
	},
	{
		// 64 entries (8 repeats of the 8-entry pattern) - exercises multi-level Merkle tree
		name:     "sixty_four_entries",
		entries:  make64Entries(),
		xorbHash: "6554007c9b5d0a5e7918f79596a1b68815c1407535585435f5735db761f21b88",
		fileHash: "a8640ab81d48854e00078e12b1ea8be5d90be0ffb5f73a15b7009981d093ddd8",
	},
}

func make64Entries() []refEntry {
	block := []refEntry{
		{"0000000000000000000000000000000000000000000000000000000000000000", 0},
		{"cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6", 100},
		{"c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72", 200},
		{"0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37", 300},
		{"adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784", 400},
		{"4ac202caf347fc1e9c874b1ef6a1c5e619141eb775a6f43f0f0124ccd0060d9e", 500},
		{"b3b28636f65c149ea52eb1f94669466f70f033b54cea792824c696ba6ef3c389", 600},
		{"0e2c1a002aae913d2c0fc8ddfa4e9e14b7b311b3b0d458726d5d9f6a6318013c", 700},
	}
	entries := make([]refEntry, 0, 64)
	for i := 0; i < 8; i++ {
		entries = append(entries, block...)
	}
	return entries
}

func TestReferenceXorbHash(t *testing.T) {
	for _, tc := range referenceTests {
		t.Run(tc.name, func(t *testing.T) {
			hashes := make([][32]byte, len(tc.entries))
			sizes := make([]uint64, len(tc.entries))
			for i, e := range tc.entries {
				h, err := xet.StringToHash(e.hash)
				if err != nil {
					t.Fatalf("invalid hash %q: %v", e.hash, err)
				}
				hashes[i] = h
				sizes[i] = e.size
			}

			got := ComputeXorbHash(hashes, sizes)
			gotStr := xet.HashToString(got)
			if gotStr != tc.xorbHash {
				t.Errorf("xorb hash mismatch:\n  got:  %s\n  want: %s", gotStr, tc.xorbHash)
			}
		})
	}
}

func TestReferenceFileHash(t *testing.T) {
	for _, tc := range referenceTests {
		t.Run(tc.name, func(t *testing.T) {
			hashes := make([][32]byte, len(tc.entries))
			sizes := make([]uint64, len(tc.entries))
			for i, e := range tc.entries {
				h, err := xet.StringToHash(e.hash)
				if err != nil {
					t.Fatalf("invalid hash %q: %v", e.hash, err)
				}
				hashes[i] = h
				sizes[i] = e.size
			}

			got := ComputeFileHash(hashes, sizes)
			gotStr := xet.HashToString(got)
			if gotStr != tc.fileHash {
				t.Errorf("file hash mismatch:\n  got:  %s\n  want: %s", gotStr, tc.fileHash)
			}
		})
	}
}
