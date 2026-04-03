package xorb

// Binary format identifiers
var (
	xorbIdentifier       = [...]byte{'X', 'E', 'T', 'B', 'L', 'O', 'B'}
	hashSectionIdent     = [...]byte{'X', 'B', 'L', 'B', 'H', 'S', 'H'}
	boundarySectionIdent = [...]byte{'X', 'B', 'L', 'B', 'B', 'N', 'D'}
)
