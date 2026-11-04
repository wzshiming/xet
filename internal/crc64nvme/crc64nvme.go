// Package crc64nvme computes CRC-64/NVME, the checksum S3 carries in x-amz-checksum-crc64nvme.
package crc64nvme

import (
	"encoding/base64"
	"encoding/binary"
	"hash"
	"hash/crc64"
)

const HeaderName = "x-amz-checksum-crc64nvme"

// poly is CRC-64/NVME's polynomial 0xAD93D23594C93659 bit-reversed, the form crc64.MakeTable takes.
const poly = 0x9A6C9329AC4BC9B5

// crc64 inverts the crc on entry and exit, which is NVME's all-ones init and xorout.
var table = crc64.MakeTable(poly)

// New returns a CRC-64/NVME hash whose Sum appends the 8 big-endian bytes.
func New() hash.Hash64 {
	return crc64.New(table)
}

// Checksum returns the CRC-64/NVME of data, 0 when data is empty.
func Checksum(data []byte) uint64 {
	return crc64.Checksum(data, table)
}

// Base64 renders sum in the form the HeaderName header carries.
func Base64(sum uint64) string {
	return base64.StdEncoding.EncodeToString(binary.BigEndian.AppendUint64(nil, sum))
}
