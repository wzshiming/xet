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

// isHexHash64 reports whether name is a 64-character hex hash, the only
// shape stored object names take.
func isHexHash64(name string) bool {
	if len(name) != 64 {
		return false
	}
	_, err := hex.DecodeString(name)
	return err == nil
}
