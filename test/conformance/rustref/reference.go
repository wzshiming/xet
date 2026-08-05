// Package rustref invokes the pinned huggingface/xet-core Rust implementation.
package rustref

import (
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

type uploadRequest struct {
	FilePaths  []string   `json:"file_paths"`
	Endpoint   string     `json:"endpoint"`
	Token      *TokenInfo `json:"token,omitempty"`
	SHA256s    []string   `json:"sha256s,omitempty"`
	SkipSHA256 bool       `json:"skip_sha256"`
}

type downloadRequest struct {
	Files    []DownloadRequest `json:"files"`
	Endpoint string            `json:"endpoint"`
	Token    *TokenInfo        `json:"token,omitempty"`
}

var (
	buildOnce sync.Once
	buildErr  error
)

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

func HashFiles(filePaths []string) ([]UploadResult, error) {
	var result []UploadResult
	err := runJSON("hash-files", filePaths, &result)
	return result, err
}

func UploadFiles(filePaths []string, endpoint string, token *TokenInfo, sha256s []string, skipSHA256 bool) ([]UploadResult, error) {
	var result []UploadResult
	err := runJSON("upload-files", uploadRequest{
		FilePaths:  filePaths,
		Endpoint:   endpoint,
		Token:      token,
		SHA256s:    sha256s,
		SkipSHA256: skipSHA256,
	}, &result)
	return result, err
}

func DownloadFiles(files []DownloadRequest, endpoint string, token *TokenInfo) ([]string, error) {
	var result []string
	err := runJSON("download-files", downloadRequest{
		Files:    files,
		Endpoint: endpoint,
		Token:    token,
	}, &result)
	return result, err
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
	noProxy := "127.0.0.1,localhost"
	if existing := os.Getenv("NO_PROXY"); existing != "" {
		noProxy += "," + existing
	}
	cmd.Env = append(os.Environ(),
		"HF_XET_CACHE="+cacheDir,
		"NO_PROXY="+noProxy,
		"no_proxy="+noProxy,
	)
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
