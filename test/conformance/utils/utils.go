package utils

import (
	"math/rand"
)

var seed = rand.NewSource(1)

// MakeRandData creates a deterministic byte sequence of the given size.
func MakeRandData(size int) []byte {
	result := make([]byte, size)
	for i := range result {
		result[i] = byte(seed.Int63() % 256)
	}
	return result
}

// MakeRepeatData creates a byte sequence of the given size with a repeating pattern.
func MakeRepeatData(size int) []byte {
	result := make([]byte, size)
	for i := range result {
		result[i] = byte(i % 256)
	}
	return result
}
