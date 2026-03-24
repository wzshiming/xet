// Package xet provides a Go implementation of the Xet protocol data structures.
package xet

import "errors"

// Common error types for the Xet protocol.
var (
	// ErrIO represents an I/O error.
	ErrIO = errors.New("I/O error")

	// ErrInternalError represents an internal error.
	ErrInternalError = errors.New("internal error")

	// ErrTruncatedHashCollision indicates too many collisions for a truncated hash.
	ErrTruncatedHashCollision = errors.New("too many collisions for truncated hash")

	// ErrShardVersion indicates a shard version error.
	ErrShardVersion = errors.New("shard version error")

	// ErrBadFilename indicates a bad filename.
	ErrBadFilename = errors.New("bad filename")

	// ErrShardNotFound indicates a shard was not found.
	ErrShardNotFound = errors.New("shard not found")

	// ErrFileNotFound indicates a file was not found.
	ErrFileNotFound = errors.New("file not found")

	// ErrQueryFailed indicates a query failure.
	ErrQueryFailed = errors.New("query failed")

	// ErrInvalidShard indicates an invalid shard.
	ErrInvalidShard = errors.New("invalid shard")

	// ErrInvalidRange indicates an invalid range.
	ErrInvalidRange = errors.New("invalid range")

	// ErrInvalidArguments indicates invalid arguments.
	ErrInvalidArguments = errors.New("invalid arguments")

	// ErrMalformedData indicates malformed data.
	ErrMalformedData = errors.New("malformed data")

	// ErrHashMismatch indicates a hash mismatch.
	ErrHashMismatch = errors.New("hash mismatch")

	// ErrCompressionError indicates a compression error.
	ErrCompressionError = errors.New("compression error")

	// ErrHashParsing indicates a hash parsing error.
	ErrHashParsing = errors.New("hash parsing error")

	// ErrChunkHeaderParse indicates a chunk header parse error.
	ErrChunkHeaderParse = errors.New("chunk header parse error")
)
