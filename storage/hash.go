package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
)

func computeShardHashFromReader(r io.Reader) (string, error) {
	hasher := sha256.New()
	_, err := io.Copy(hasher, r)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}
