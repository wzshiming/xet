package main

import (
	"bytes"
	"fmt"
	"io/fs"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage/local"
)

func TestCacheDirFlag(t *testing.T) {
	root := newRootCommand()
	for _, tc := range []struct {
		path      []string
		wantUsage string
	}{
		{[]string{"upload", "cas"}, "upload"},
		{[]string{"upload", "hf"}, "upload"},
		{[]string{"upload", "lfs"}, "upload"},
		{[]string{"download", "cas"}, "xet-cache"},
		{[]string{"download", "hf"}, "xet-cache"},
		{[]string{"download", "resolve"}, "xet-cache"},
	} {
		t.Run(strings.Join(tc.path, "/"), func(t *testing.T) {
			cmd, _, err := root.Find(tc.path)
			if err != nil {
				t.Fatal(err)
			}
			f := cmd.Flags().Lookup("cache-dir")
			if f == nil {
				t.Fatalf("%s: --cache-dir flag missing", cmd.CommandPath())
			}
			if f.Value.Type() != "string" || f.DefValue != "" {
				t.Fatalf("--cache-dir type = %q, default = %q; want string with empty default", f.Value.Type(), f.DefValue)
			}
			if !strings.Contains(f.Usage, tc.wantUsage) {
				t.Fatalf("--cache-dir usage = %q, want mention of %q", f.Usage, tc.wantUsage)
			}
			if !strings.Contains(cmd.Flags().FlagUsages(), "--cache-dir string") {
				t.Fatalf("help does not list --cache-dir:\n%s", cmd.Flags().FlagUsages())
			}
		})
	}
	hashCmd, _, err := root.Find([]string{"hash"})
	if err != nil {
		t.Fatal(err)
	}
	if hashCmd.Flags().Lookup("cache-dir") != nil {
		t.Fatal("hash must not expose --cache-dir")
	}
}

func TestCacheDirRoundTrip(t *testing.T) {
	stor, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	uploadDir := t.TempDir()
	var stagedSeen atomic.Int64
	var counting atomic.Bool
	var chunkQueries, xorbWrites atomic.Int64
	handler := server.NewHandler(server.WithStorage(stor))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/xorbs/") {
			staged, _ := filepath.Glob(filepath.Join(uploadDir, "upload", "staging", "xorb-*"))
			stagedSeen.Add(int64(len(staged)))
		}
		if counting.Load() {
			if strings.HasPrefix(r.URL.Path, "/v1/chunks") {
				chunkQueries.Add(1)
			}
			if strings.HasPrefix(r.URL.Path, "/v1/xorbs/") && (r.Method == http.MethodPost || r.Method == http.MethodHead) {
				xorbWrites.Add(1)
			}
		}
		handler.ServeHTTP(w, r)
	}))
	defer srv.Close()

	content := make([]byte, 1<<20)
	rand.New(rand.NewSource(1)).Read(content)
	input := filepath.Join(t.TempDir(), "input.bin")
	if err := os.WriteFile(input, content, 0o644); err != nil {
		t.Fatal(err)
	}

	run := func(args ...string) {
		t.Helper()
		cmd := newRootCommand()
		cmd.SetArgs(args)
		if err := cmd.ExecuteContext(t.Context()); err != nil {
			t.Fatalf("xetc %s: %v", strings.Join(args, " "), err)
		}
	}

	run("upload", "cas", input, "--url", srv.URL, "--cache-dir", uploadDir)
	if stagedSeen.Load() == 0 {
		t.Fatal("no staging/xorb-* file observed in --cache-dir during upload")
	}
	if leftover, _ := filepath.Glob(filepath.Join(uploadDir, "upload", "staging", "xorb-*")); len(leftover) != 0 {
		t.Fatalf("upload staging leftover: %v", leftover)
	}
	dirs, err := os.ReadDir(filepath.Join(uploadDir, "upload"))
	if err != nil || len(dirs) != 2 {
		t.Fatalf("upload cache dir = %v, %v; want the chunks and staging dirs", dirs, err)
	}
	for i, want := range []string{"chunks", "staging"} {
		if dirs[i].Name() != want || !dirs[i].IsDir() {
			t.Fatalf("upload cache dir = %v; want the chunks and staging dirs", dirs)
		}
	}
	if staged, err := os.ReadDir(filepath.Join(uploadDir, "upload", "staging")); err != nil || len(staged) != 0 {
		t.Fatalf("staging after upload = %v, %v; want empty", staged, err)
	}
	entryPath := regexp.MustCompile(`^[0-9a-f]{16}/[0-9a-f]{2}/[0-9a-f]{2}/[0-9a-f]{60}$`)
	chunkEntries := func() int {
		t.Helper()
		root := filepath.Join(uploadDir, "upload", "chunks")
		n := 0
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil || !entry.Type().IsRegular() {
				return err
			}
			if rel, _ := filepath.Rel(root, path); !entryPath.MatchString(filepath.ToSlash(rel)) {
				return fmt.Errorf("unexpected file under chunks: %s", rel)
			}
			n++
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	cached := chunkEntries()
	if cached == 0 {
		t.Fatal("first upload cached no chunk locations")
	}

	counting.Store(true)
	run("upload", "cas", input, "--url", srv.URL, "--cache-dir", uploadDir)
	counting.Store(false)
	if q, x := chunkQueries.Load(), xorbWrites.Load(); q != 0 || x != 0 {
		t.Fatalf("second upload made %d /v1/chunks requests and %d xorb HEAD/POST requests; want 0 (dedup from the cached locations)", q, x)
	}
	if n := chunkEntries(); n != cached {
		t.Fatalf("cached chunk locations after second upload = %d; want still %d, a shard without new chunks adds none", n, cached)
	}

	var hashes []xet.ChunkHash
	var sizes []uint64
	if err := xet.ChunkData(bytes.NewReader(content), func(_ int64, chunk []byte) error {
		hashes = append(hashes, xet.ComputeChunkHash(chunk))
		sizes = append(sizes, uint64(len(chunk)))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	fileHash := xet.ComputeFileHash(hashes, sizes)

	downloadDir := t.TempDir()
	output := filepath.Join(t.TempDir(), "output.bin")
	run("download", "cas", output, fileHash.String(), "--url", srv.URL, "--cache-dir", downloadDir)
	got, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("downloaded %d bytes differ from uploaded %d bytes", len(got), len(content))
	}

	entryPattern := regexp.MustCompile(`^download/[0-9a-f]{2}/[0-9a-f]{2}/[0-9a-f]{60}/\d+-\d+_\d+-\d+$`)
	var entries []string
	err = filepath.WalkDir(downloadDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(downloadDir, path)
		if err != nil {
			return err
		}
		entries = append(entries, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("no chunk cache entries written under --cache-dir")
	}
	for _, entry := range entries {
		if !entryPattern.MatchString(entry) {
			t.Fatalf("unexpected cache entry %q", entry)
		}
	}
	t.Logf("cache entries under %s: %v", downloadDir, entries)
}
