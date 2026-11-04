package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/storage/storagetest"
)

func TestXetdProcess(t *testing.T) {
	if os.Getenv("XETD_TEST_PROCESS") != "1" {
		return
	}
	os.Args = append(os.Args[:1], os.Args[3:]...)
	flag.CommandLine = flag.NewFlagSet("xetd", flag.ExitOnError)
	main()
}

func xetdCommand(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, append([]string{"-test.run=^TestXetdProcess$", "--"}, args...)...)
	command.Env = append(os.Environ(), "XETD_TEST_PROCESS=1")
	return command
}

func TestXetdRequiresInternalToken(t *testing.T) {
	command := xetdCommand(t, "-internal", "-addr=invalid", "-storage="+t.TempDir())
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "-internal requires -internal-token") {
		t.Fatalf("startup = %v, output = %s", err, output)
	}
}

func TestXetdHelpListsTermSplitDefaults(t *testing.T) {
	output, err := xetdCommand(t, "-help").CombinedOutput()
	for _, entry := range []string{
		`-reconstruction-min-terms int\n.*\(default 16\)`,
		`-reconstruction-min-term-bytes int\n.*\(default 1048576\)`,
	} {
		if !regexp.MustCompile(`(?m)^  ` + entry + `$`).Match(output) {
			t.Errorf("-help = %v, output = %s; want an entry matching %#q", err, output, entry)
		}
	}
}

func TestXetdRejectsNegativeTermSplitFlags(t *testing.T) {
	for _, name := range []string{"-reconstruction-min-terms", "-reconstruction-min-term-bytes"} {
		t.Run(name, func(t *testing.T) {
			command := xetdCommand(t, name+"=-1", "-addr=invalid", "-storage="+t.TempDir())
			output, err := command.CombinedOutput()
			if want := name + " must not be negative"; err == nil || !strings.Contains(string(output), want) {
				t.Fatalf("startup with %s=-1 = %v, output = %s; want a failure with %q", name, err, output, want)
			}
		})
	}
}

func TestXetdInternalTokenIsolation(t *testing.T) {
	for _, signingKey := range []string{"signing-secret", ""} {
		t.Run("signing-key="+signingKey, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := listener.Addr().String()
			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
			upstream := httptest.NewServer(http.NotFoundHandler())
			defer upstream.Close()
			command := xetdCommand(t, "-addr="+address, "-storage="+t.TempDir(), "-upstream="+upstream.URL,
				"-signing-key="+signingKey, "-internal", "-internal-token=internal-secret")
			var output bytes.Buffer
			command.Stdout = &output
			command.Stderr = &output
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			var processErr error
			go func() {
				processErr = command.Wait()
				close(done)
			}()
			t.Cleanup(func() {
				_ = command.Process.Kill()
				<-done
			})
			client := &http.Client{Timeout: time.Second}
			base := "http://" + address
			deadline := time.NewTimer(10 * time.Second)
			defer deadline.Stop()
			retry := time.NewTicker(10 * time.Millisecond)
			defer retry.Stop()
			for {
				response, err := client.Get(base + "/internal/files")
				if err == nil {
					_ = response.Body.Close()
					break
				}
				select {
				case <-done:
					t.Fatalf("startup = %v, output = %s", processErr, output.String())
				case <-deadline.C:
					t.Fatal("xetd did not start")
				case <-retry.C:
				}
			}
			response, err := client.Get(base + "/api/models/org/repo/xet-read-token/main")
			if err != nil {
				t.Fatal(err)
			}
			readToken := response.Header.Get("X-Xet-Access-Token")
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK || readToken == "" {
				t.Fatalf("mint Read token: status = %d", response.StatusCode)
			}
			issuer, err := auth.NewIssuer([]byte(signingKey), 0, nil)
			if err != nil {
				t.Fatal(err)
			}
			writeToken, _, err := issuer.Sign(auth.Grant{Permission: auth.Write})
			if err != nil {
				t.Fatal(err)
			}
			for _, route := range []struct {
				method string
				path   string
				status int
			}{
				{http.MethodGet, "/internal/files", http.StatusOK},
				{http.MethodDelete, "/internal/files/xet/" + strings.Repeat("ab", 32), http.StatusNotFound},
				{http.MethodDelete, "/internal/files/sha256/" + strings.Repeat("ab", 32), http.StatusNotFound},
				{http.MethodPost, "/internal/gc?dry_run=true", http.StatusOK},
				{http.MethodGet, "/reconstructions", http.StatusOK},
			} {
				for _, token := range []string{"", "wrong-token", "signing-secret", readToken, writeToken, "internal-secret"} {
					request, err := http.NewRequest(route.method, base+route.path, nil)
					if err != nil {
						t.Fatal(err)
					}
					if token != "" {
						request.Header.Set("Authorization", "Bearer "+token)
					}
					response, err := client.Do(request)
					if err != nil {
						t.Fatal(err)
					}
					_, err = io.Copy(io.Discard, response.Body)
					_ = response.Body.Close()
					if err != nil {
						t.Fatal(err)
					}
					want := http.StatusUnauthorized
					if strings.HasPrefix(route.path, "/internal/") {
						if token == "internal-secret" {
							want = route.status
						}
					} else if signingKey == "" || token == readToken {
						want = http.StatusOK
					} else if token == writeToken {
						want = http.StatusForbidden
					}
					if response.StatusCode != want {
						t.Fatalf("%s %s: status = %d, want %d", route.method, route.path, response.StatusCode, want)
					}
					if want == http.StatusUnauthorized && response.Header.Get("WWW-Authenticate") != "Bearer" {
						t.Fatalf("%s: missing Bearer challenge", route.path)
					}
				}
			}
		})
	}
}

func TestXetdReconstructionTermSplit(t *testing.T) {
	storageDir := t.TempDir()
	st, err := local.NewStorage(local.WithBasePath(storageDir))
	if err != nil {
		t.Fatal(err)
	}
	// 16 MiB in one xorb of 64 KiB chunks: a single V2 term unless split.
	file := storagetest.PutXorbFile(t, t.Context(), st, storagetest.ChunkClasses[0].Xorbs(1, 256, 64<<10))
	for _, c := range []struct {
		name  string
		flags []string
		terms int
	}{
		{"default splits into 16 terms", nil, 16},
		{"-reconstruction-min-terms=0 keeps 1 term", []string{"-reconstruction-min-terms=0"}, 1},
		{"-reconstruction-min-term-bytes=8388608 stops at 2 terms", []string{"-reconstruction-min-term-bytes=8388608"}, 2},
		{"-reconstruction-min-term-bytes=0 starts and splits into 16 terms", []string{"-reconstruction-min-term-bytes=0"}, 16},
	} {
		t.Run(c.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := listener.Addr().String()
			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
			command := xetdCommand(t, append([]string{"-addr=" + address, "-storage=" + storageDir}, c.flags...)...)
			var output bytes.Buffer
			command.Stdout = &output
			command.Stderr = &output
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() {
				_ = command.Wait()
				close(done)
			}()
			t.Cleanup(func() {
				_ = command.Process.Kill()
				<-done
			})
			client := &http.Client{Timeout: 10 * time.Second}
			deadline := time.After(10 * time.Second)
			for {
				response, err := client.Get("http://" + address + "/v2/reconstructions/" + file.FileHash.String())
				if err == nil {
					var reconstruction download.ReconstructionResponseV2
					err = json.NewDecoder(response.Body).Decode(&reconstruction)
					_ = response.Body.Close()
					if response.StatusCode != http.StatusOK || err != nil || len(reconstruction.Terms) != c.terms {
						t.Fatalf("xetd %q: V2 reconstruction of a 16 MiB term = status %d, %d terms, %v; want %d terms", c.flags, response.StatusCode, len(reconstruction.Terms), err, c.terms)
					}
					return
				}
				select {
				case <-done:
					t.Fatalf("xetd %q exited: %s", c.flags, output.String())
				case <-deadline:
					t.Fatal("xetd did not start")
				case <-time.After(10 * time.Millisecond):
				}
			}
		})
	}
}
