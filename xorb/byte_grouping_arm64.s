#include "textflag.h"

// func interleave4Asm(dst, p0, p1, p2, p3 *byte, n int)
TEXT ·interleave4Asm(SB), NOSPLIT, $0-48
	MOVD	dst+0(FP), R0
	MOVD	p0+8(FP), R1
	MOVD	p1+16(FP), R2
	MOVD	p2+24(FP), R3
	MOVD	p3+32(FP), R4
	MOVD	n+40(FP), R5
	CMP	$16, R5
	BLT	tail

loop16:
	VLD1.P	16(R1), [V0.B16]
	VLD1.P	16(R2), [V1.B16]
	VLD1.P	16(R3), [V2.B16]
	VLD1.P	16(R4), [V3.B16]
	// ST4 writes byte i of V0, V1, V2 and V3 next to each other, for i from 0 to 15.
	VST4.P	[V0.B16, V1.B16, V2.B16, V3.B16], 64(R0)
	SUB	$16, R5, R5
	CMP	$16, R5
	BGE	loop16

tail:
	CMP	$0, R5
	BLE	done

loop1:
	MOVBU.P	1(R1), R6
	MOVBU.P	1(R2), R7
	MOVBU.P	1(R3), R8
	MOVBU.P	1(R4), R9
	MOVB.P	R6, 1(R0)
	MOVB.P	R7, 1(R0)
	MOVB.P	R8, 1(R0)
	MOVB.P	R9, 1(R0)
	SUBS	$1, R5, R5
	BNE	loop1

done:
	RET
