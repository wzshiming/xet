package storage

import (
	"context"
	"encoding/hex"
	"slices"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

func TestListFiles(t *testing.T) {
	for name, st := range gcBackends(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			chunkA1 := []byte("list-file-a-first-chunk")
			chunkA2 := []byte("list-file-a-second-chunk-with-more-bytes")
			chunkB := []byte("list-file-b-chunk")

			xorbA := putGCXorb(t, st, chunkA1, chunkA2)
			xorbB := putGCXorb(t, st, chunkB)

			fileHashA := xet.FileHash{0xA1}
			fileHashB := xet.FileHash{0xB1}
			// File C carries the same content as A under a different xet
			// hash, as a different chunker would produce.
			fileHashC := xet.FileHash{0xC1}
			termsA := []gcTerm{{xorbA, [][]byte{chunkA1, chunkA2}}}
			termsB := []gcTerm{{xorbB, [][]byte{chunkB}}}

			putGCShard(t, st,
				[]shard.FileBlock{gcFileBlock(fileHashA, termsA...)},
				[]shard.CASBlock{gcCASBlock(xorbA, xet.ChunkHash{1}, xet.ChunkHash{2})})
			putGCShard(t, st,
				[]shard.FileBlock{gcFileBlock(fileHashB, termsB...)},
				[]shard.CASBlock{gcCASBlock(xorbB, xet.ChunkHash{3})})
			putGCShard(t, st,
				[]shard.FileBlock{gcFileBlock(fileHashC, termsA...)},
				[]shard.CASBlock{})

			sha256A := gcSHA256(termsA...)
			sha256B := gcSHA256(termsB...)
			sizeA := int64(len(chunkA1) + len(chunkA2))
			sizeB := int64(len(chunkB))

			byFileHash := func(list []FileListEntry, fileHash string) *FileListEntry {
				for i := range list {
					if slices.Contains(list[i].FileHashes, fileHash) {
						return &list[i]
					}
				}
				t.Fatalf("file %s not listed", fileHash)
				return nil
			}
			totalSize := func(list []FileListEntry) int64 {
				var n int64
				for _, e := range list {
					n += e.Size
				}
				return n
			}

			// A and C share one SHA-256 and merge into one entry whose size
			// counts once; the index alone could not reveal that, it points
			// at only one of them.
			list, err := ListFiles(ctx, st)
			if err != nil {
				t.Fatalf("ListFiles: %v", err)
			}
			if len(list) != 2 || totalSize(list) != sizeA+sizeB {
				t.Fatalf("list = entries %d, total %d; want 2, %d", len(list), totalSize(list), sizeA+sizeB)
			}
			a := byFileHash(list, fileHashA.String())
			wantHashes := []string{fileHashA.String(), fileHashC.String()}
			slices.Sort(wantHashes)
			if a.SHA256 != hex.EncodeToString(sha256A[:]) || !slices.Equal(a.FileHashes, wantHashes) || a.Size != sizeA || a.Missing {
				t.Fatalf("entry A+C = %+v", a)
			}
			b := byFileHash(list, fileHashB.String())
			if b.SHA256 != hex.EncodeToString(sha256B[:]) || len(b.FileHashes) != 1 || b.Size != sizeB || b.Missing {
				t.Fatalf("entry B = %+v", b)
			}

			var shardHashA, shardHashB, shardHashC string
			if err := st.WalkFileIndex(ctx, func(fileHash, shardHash string, _ time.Time) error {
				switch fileHash {
				case fileHashA.String():
					shardHashA = shardHash
				case fileHashB.String():
					shardHashB = shardHash
				case fileHashC.String():
					shardHashC = shardHash
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			// With B's shard gone its SHA-256 falls back to the index entry;
			// the entry goes missing and drops out of the total.
			if err := st.DeleteShard(ctx, shardHashB); err != nil {
				t.Fatal(err)
			}
			list, err = ListFiles(ctx, st)
			if err != nil {
				t.Fatalf("ListFiles after shard B delete: %v", err)
			}
			if len(list) != 2 || totalSize(list) != sizeA {
				t.Fatalf("list after shard B delete = entries %d, total %d; want 2, %d", len(list), totalSize(list), sizeA)
			}
			b = byFileHash(list, fileHashB.String())
			if b.SHA256 != hex.EncodeToString(sha256B[:]) || !b.Missing || b.Size != 0 {
				t.Fatalf("entry B after shard B delete = %+v", b)
			}

			// The SHA-256 index entry's deletion does not touch A and C: their
			// SHA-256 comes from shard metadata and the merge holds.
			if _, err := st.DeleteSHA256IndexEntry(ctx, hex.EncodeToString(sha256A[:])); err != nil {
				t.Fatal(err)
			}
			list, err = ListFiles(ctx, st)
			if err != nil {
				t.Fatalf("ListFiles after sha256 A delete: %v", err)
			}
			a = byFileHash(list, fileHashA.String())
			if a.SHA256 != hex.EncodeToString(sha256A[:]) || !slices.Equal(a.FileHashes, wantHashes) || a.Size != sizeA || a.Missing {
				t.Fatalf("entry A+C after sha256 A delete = %+v", a)
			}

			// C now has neither shard metadata nor an index entry, so no
			// SHA-256 is known for it; it splits into its own missing entry
			// instead of merging with other unknowns.
			if err := st.DeleteShard(ctx, shardHashC); err != nil {
				t.Fatal(err)
			}
			list, err = ListFiles(ctx, st)
			if err != nil {
				t.Fatalf("ListFiles after shard C delete: %v", err)
			}
			if len(list) != 3 || totalSize(list) != sizeA {
				t.Fatalf("list after shard C delete = entries %d, total %d; want 3, %d", len(list), totalSize(list), sizeA)
			}
			a = byFileHash(list, fileHashA.String())
			if a.SHA256 != hex.EncodeToString(sha256A[:]) || len(a.FileHashes) != 1 || a.Size != sizeA || a.Missing {
				t.Fatalf("entry A after shard C delete = %+v", a)
			}
			c := byFileHash(list, fileHashC.String())
			if c.SHA256 != "" || !c.Missing || c.Size != 0 || len(c.FileHashes) != 1 {
				t.Fatalf("entry C after shard C delete = %+v", c)
			}

			// A joins C in the unknown state: two files without a SHA-256 stay
			// two separate entries.
			if err := st.DeleteShard(ctx, shardHashA); err != nil {
				t.Fatal(err)
			}
			list, err = ListFiles(ctx, st)
			if err != nil {
				t.Fatalf("ListFiles after shard A delete: %v", err)
			}
			if len(list) != 3 || totalSize(list) != 0 {
				t.Fatalf("list after shard A delete = entries %d, total %d; want 3, 0", len(list), totalSize(list))
			}
			a = byFileHash(list, fileHashA.String())
			if a.SHA256 != "" || !a.Missing || a.Size != 0 || len(a.FileHashes) != 1 {
				t.Fatalf("entry A after shard A delete = %+v", a)
			}
			if c := byFileHash(list, fileHashC.String()); slices.Equal(c.FileHashes, a.FileHashes) {
				t.Fatalf("A and C merged into one entry without a SHA-256: %+v", c)
			}
		})
	}
}
