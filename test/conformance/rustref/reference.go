// Package rustref invokes the pinned huggingface/xet-core Rust implementation.
package rustref

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

type TokenInfo struct {
	Token  string `json:"token"`
	Expiry uint64 `json:"expiry"`
}

type UploadResult struct {
	Hash     string `json:"hash"`
	FileSize uint64 `json:"file_size"`
	SHA256   string `json:"sha256"`
}

type DownloadRequest struct {
	DestinationPath string `json:"destination_path"`
	Hash            string `json:"hash"`
	FileSize        int64  `json:"file_size"`
}

type ChunkInfo struct {
	Hash string `json:"hash"`
	Size uint64 `json:"size"`
}

// ProtocolVersion selects the xet-core CAS API version without fallback.
type ProtocolVersion uint32

const (
	ProtocolV1 ProtocolVersion = 1
	ProtocolV2 ProtocolVersion = 2
)

func (v ProtocolVersion) String() string {
	return fmt.Sprintf("v%d", v)
}

type uploadRequest struct {
	FilePaths  []string         `json:"file_paths"`
	Endpoint   string           `json:"endpoint"`
	Token      *TokenInfo       `json:"token,omitempty"`
	SHA256s    []string         `json:"sha256s,omitempty"`
	SkipSHA256 bool             `json:"skip_sha256"`
	APIVersion *ProtocolVersion `json:"api_version,omitempty"`
}

type downloadRequest struct {
	Files      []DownloadRequest `json:"files"`
	Endpoint   string            `json:"endpoint"`
	Token      *TokenInfo        `json:"token,omitempty"`
	APIVersion *ProtocolVersion  `json:"api_version,omitempty"`
}

var (
	buildOnce sync.Once
	buildErr  error
)

// Server is a running xet-core LocalTestServer owned by the reference helper.
// Closing stdin asks the Rust process to stop and releases its temporary storage.
type Server struct {
	Endpoint string

	cmd       *exec.Cmd
	stdin     io.WriteCloser
	cacheDir  string
	stderr    bytes.Buffer
	wait      chan error
	closeOnce sync.Once
	closeErr  error
}

// StartServer starts the xet-core in-memory CAS server and waits until its HTTP
// endpoint is ready. The returned server must be closed by the caller.
func StartServer() (*Server, error) {
	if err := ensureBuilt(); err != nil {
		return nil, err
	}

	cacheDir, err := os.MkdirTemp("", "xet-core-reference-server-cache-*")
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(referenceBinary(), "serve")
	cmd.Env = referenceEnv(cacheDir)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		_ = os.RemoveAll(cacheDir)
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		_ = os.RemoveAll(cacheDir)
		return nil, err
	}

	server := &Server{
		cmd:      cmd,
		stdin:    stdin,
		cacheDir: cacheDir,
		wait:     make(chan error, 1),
	}
	cmd.Stderr = &server.stderr
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = os.RemoveAll(cacheDir)
		return nil, err
	}
	go func() {
		server.wait <- cmd.Wait()
	}()

	type startupResult struct {
		endpoint string
		err      error
		reader   *bufio.Reader
	}
	startup := make(chan startupResult, 1)
	go func() {
		reader := bufio.NewReader(stdout)
		line, err := reader.ReadString('\n')
		startup <- startupResult{
			endpoint: strings.TrimSpace(line),
			err:      err,
			reader:   reader,
		}
	}()

	var result startupResult
	select {
	case result = <-startup:
	case <-time.After(30 * time.Second):
		return nil, server.abortStart("timed out waiting for endpoint")
	}
	if result.err != nil {
		return nil, server.abortStart(fmt.Sprintf("read endpoint: %v", result.err))
	}
	if !strings.HasPrefix(result.endpoint, "http://") && !strings.HasPrefix(result.endpoint, "https://") {
		return nil, server.abortStart(fmt.Sprintf("invalid endpoint %q", result.endpoint))
	}

	server.Endpoint = result.endpoint
	go func() {
		_, _ = io.Copy(io.Discard, result.reader)
	}()
	return server, nil
}

func (s *Server) abortStart(message string) error {
	_ = s.stdin.Close()
	_ = s.cmd.Process.Kill()
	<-s.wait
	_ = os.RemoveAll(s.cacheDir)
	if stderr := strings.TrimSpace(s.stderr.String()); stderr != "" {
		return fmt.Errorf("start xet-core reference server: %s: %s", message, stderr)
	}
	return fmt.Errorf("start xet-core reference server: %s", message)
}

// Close gracefully stops the server and removes its temporary cache.
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		_ = s.stdin.Close()
		select {
		case err := <-s.wait:
			if err != nil {
				s.closeErr = fmt.Errorf("stop xet-core reference server: %w: %s", err, strings.TrimSpace(s.stderr.String()))
			}
		case <-time.After(10 * time.Second):
			_ = s.cmd.Process.Kill()
			<-s.wait
			s.closeErr = errors.New("stop xet-core reference server: timed out")
		}
		if err := os.RemoveAll(s.cacheDir); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

func ChunkData(data []byte) ([]ChunkInfo, error) {
	var result []ChunkInfo
	err := runRaw("chunk", nil, bytes.NewReader(data), &result)
	return result, err
}

func HashChunk(data []byte) (string, error) {
	var result string
	err := runRaw("hash-chunk", nil, bytes.NewReader(data), &result)
	return result, err
}

func ComputeXorbHash(chunks []ChunkInfo) (string, error) {
	return hashList("hash-xorb", chunks)
}

func ComputeFileHash(chunks []ChunkInfo) (string, error) {
	return hashList("hash-file", chunks)
}

func ComputeRangeHash(chunks []ChunkInfo) (string, error) {
	return hashList("hash-range", chunks)
}

func hashList(command string, chunks []ChunkInfo) (string, error) {
	var result string
	err := runJSON(command, chunks, &result)
	return result, err
}

// HMACCase pairs a chunk hash with an HMAC key, both in XET hex form.
type HMACCase struct {
	Hash string `json:"hash"`
	Key  string `json:"key"`
}

// HashHMAC computes keyed chunk hashes via xet-core's MerkleHash::hmac, as
// applied to chunk hashes in HMAC-keyed global-dedup shards.
func HashHMAC(cases []HMACCase) ([]string, error) {
	var result []string
	err := runJSON("hash-hmac", cases, &result)
	return result, err
}

func HashFiles(filePaths []string) ([]UploadResult, error) {
	var result []UploadResult
	err := runJSON("hash-files", filePaths, &result)
	return result, err
}

func UploadFiles(filePaths []string, endpoint string, token *TokenInfo, sha256s []string, skipSHA256 bool) ([]UploadResult, error) {
	return uploadFiles(filePaths, endpoint, token, sha256s, skipSHA256, nil)
}

// UploadFilesWithVersion uploads files while forcing xet-core to use the
// selected shard API version without auto-detection or fallback.
func UploadFilesWithVersion(filePaths []string, endpoint string, token *TokenInfo, sha256s []string, skipSHA256 bool, version ProtocolVersion) ([]UploadResult, error) {
	if err := validateProtocolVersion(version); err != nil {
		return nil, err
	}
	return uploadFiles(filePaths, endpoint, token, sha256s, skipSHA256, &version)
}

func uploadFiles(filePaths []string, endpoint string, token *TokenInfo, sha256s []string, skipSHA256 bool, version *ProtocolVersion) ([]UploadResult, error) {
	var result []UploadResult
	err := runJSON("upload-files", uploadRequest{
		FilePaths:  filePaths,
		Endpoint:   endpoint,
		Token:      token,
		SHA256s:    sha256s,
		SkipSHA256: skipSHA256,
		APIVersion: version,
	}, &result)
	return result, err
}

func DownloadFiles(files []DownloadRequest, endpoint string, token *TokenInfo) ([]string, error) {
	return downloadFiles(files, endpoint, token, nil)
}

// DownloadFilesWithVersion downloads files while forcing xet-core to use the
// selected reconstruction API version without auto-detection or fallback.
func DownloadFilesWithVersion(files []DownloadRequest, endpoint string, token *TokenInfo, version ProtocolVersion) ([]string, error) {
	if err := validateProtocolVersion(version); err != nil {
		return nil, err
	}
	return downloadFiles(files, endpoint, token, &version)
}

func downloadFiles(files []DownloadRequest, endpoint string, token *TokenInfo, version *ProtocolVersion) ([]string, error) {
	var result []string
	err := runJSON("download-files", downloadRequest{
		Files:      files,
		Endpoint:   endpoint,
		Token:      token,
		APIVersion: version,
	}, &result)
	return result, err
}

func validateProtocolVersion(version ProtocolVersion) error {
	if version != ProtocolV1 && version != ProtocolV2 {
		return fmt.Errorf("unsupported protocol version %d", version)
	}
	return nil
}

func EncodeXorb(data []byte, withFooter bool, compression string) ([]byte, error) {
	if compression == "" {
		compression = "auto"
	}
	return runBytes("encode-xorb", []string{fmt.Sprint(withFooter), compression}, bytes.NewReader(data))
}

func DecodeXorb(data []byte, withFooter bool) ([]ChunkInfo, error) {
	var result []ChunkInfo
	err := runRaw("decode-xorb", []string{fmt.Sprint(withFooter)}, bytes.NewReader(data), &result)
	return result, err
}

func runJSON(command string, request, response any) error {
	var input bytes.Buffer
	if err := json.NewEncoder(&input).Encode(request); err != nil {
		return err
	}
	return runRaw(command, nil, &input, response)
}

func runRaw(command string, args []string, input io.Reader, response any) error {
	output, err := runBytes(command, args, input)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(output, response); err != nil {
		return fmt.Errorf("decode xet-core reference %s response: %w; output=%q", command, err, output)
	}
	return nil
}

func runBytes(command string, args []string, input io.Reader) ([]byte, error) {
	if err := ensureBuilt(); err != nil {
		return nil, err
	}

	cacheDir, err := os.MkdirTemp("", "xet-core-reference-cache-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(cacheDir)

	commandArgs := append([]string{command}, args...)
	cmd := exec.Command(referenceBinary(), commandArgs...)
	cmd.Stdin = input
	cmd.Env = referenceEnv(cacheDir)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = "no stderr output"
		}
		return nil, fmt.Errorf("xet-core reference %s: %w: %s", command, err, message)
	}
	return stdout.Bytes(), nil
}

func referenceEnv(cacheDir string) []string {
	noProxy := "127.0.0.1,localhost"
	if existing := os.Getenv("NO_PROXY"); existing != "" {
		noProxy += "," + existing
	}
	return append(os.Environ(),
		"HF_XET_CACHE="+cacheDir,
		"NO_PROXY="+noProxy,
		"no_proxy="+noProxy,
	)
}

func ensureBuilt() error {
	if override := os.Getenv("XET_CORE_REFERENCE_BIN"); override != "" {
		if info, err := os.Stat(override); err != nil {
			return fmt.Errorf("XET_CORE_REFERENCE_BIN: %w", err)
		} else if info.IsDir() {
			return errors.New("XET_CORE_REFERENCE_BIN points to a directory")
		}
		return nil
	}

	buildOnce.Do(func() {
		cmd := exec.Command("cargo", "build", "--release", "--locked", "--manifest-path", filepath.Join(conformanceRoot(), "Cargo.toml"))
		cmd.Dir = conformanceRoot()
		var output bytes.Buffer
		cmd.Stdout = &output
		cmd.Stderr = &output
		if err := cmd.Run(); err != nil {
			buildErr = fmt.Errorf("build xet-core reference: %w:\n%s", err, output.String())
		}
	})
	return buildErr
}

func referenceBinary() string {
	if override := os.Getenv("XET_CORE_REFERENCE_BIN"); override != "" {
		return override
	}
	name := "xet-core-reference"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	return filepath.Join(conformanceRoot(), "target", "release", name)
}

func conformanceRoot() string {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		panic("cannot locate conformance source directory")
	}
	return filepath.Dir(filepath.Dir(source))
}
