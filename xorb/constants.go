package xorb

// XORB object format identifiers
var (
	XorbObjectFormatIdent           = [7]byte{'X', 'E', 'T', 'B', 'L', 'O', 'B'}
	XorbObjectFormatIdentHashes     = [7]byte{'X', 'B', 'L', 'B', 'H', 'S', 'H'}
	XorbObjectFormatIdentBoundaries = [7]byte{'X', 'B', 'L', 'B', 'B', 'N', 'D'}
)

const (
	XorbObjectFormatVersion                         uint8 = 1
	XorbObjectFormatHashesVersion                   uint8 = 0
	XorbObjectFormatBoundariesVersionNoUnpackedInfo uint8 = 0
	XorbObjectFormatBoundariesVersion               uint8 = 1
	XorbObjectInfoDefaultLength                           = 92
)
