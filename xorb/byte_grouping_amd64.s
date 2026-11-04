#include "textflag.h"

// func interleave4Asm(dst, p0, p1, p2, p3 *byte, n int)
TEXT ·interleave4Asm(SB), NOSPLIT, $0-48
	MOVQ	dst+0(FP), DI
	MOVQ	p0+8(FP), AX
	MOVQ	p1+16(FP), BX
	MOVQ	p2+24(FP), CX
	MOVQ	p3+32(FP), DX
	MOVQ	n+40(FP), SI
	XORQ	R8, R8
	CMPQ	SI, $16
	JLT	tail

loop16:
	// a, b, c and d are the 16 bytes read from p0, p1, p2 and p3.
	MOVOU	(AX)(R8*1), X0
	MOVOU	(BX)(R8*1), X1
	MOVOU	(CX)(R8*1), X2
	MOVOU	(DX)(R8*1), X3
	MOVO	X0, X4
	PUNPCKLBW	X1, X0 // a0 b0 a1 b1 ... a7 b7
	PUNPCKHBW	X1, X4 // a8 b8 ... a15 b15
	MOVO	X2, X5
	PUNPCKLBW	X3, X2 // c0 d0 ... c7 d7
	PUNPCKHBW	X3, X5 // c8 d8 ... c15 d15
	MOVO	X0, X1
	PUNPCKLWL	X2, X0 // a0 b0 c0 d0 ... a3 b3 c3 d3
	PUNPCKHWL	X2, X1 // a4 b4 c4 d4 ... a7 b7 c7 d7
	MOVO	X4, X3
	PUNPCKLWL	X5, X4 // a8 b8 c8 d8 ... a11 b11 c11 d11
	PUNPCKHWL	X5, X3 // a12 b12 c12 d12 ... a15 b15 c15 d15
	MOVOU	X0, (DI)
	MOVOU	X1, 16(DI)
	MOVOU	X4, 32(DI)
	MOVOU	X3, 48(DI)
	ADDQ	$16, R8
	ADDQ	$64, DI
	SUBQ	$16, SI
	CMPQ	SI, $16
	JGE	loop16

tail:
	TESTQ	SI, SI
	JLE	done

loop1:
	MOVBLZX	(AX)(R8*1), R9
	MOVB	R9, (DI)
	MOVBLZX	(BX)(R8*1), R9
	MOVB	R9, 1(DI)
	MOVBLZX	(CX)(R8*1), R9
	MOVB	R9, 2(DI)
	MOVBLZX	(DX)(R8*1), R9
	MOVB	R9, 3(DI)
	INCQ	R8
	ADDQ	$4, DI
	DECQ	SI
	JNZ	loop1

done:
	RET
