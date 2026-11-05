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

// func deinterleave4Asm(p0, p1, p2, p3, src *byte, n int)
TEXT ·deinterleave4Asm(SB), NOSPLIT, $0-48
	MOVQ	p0+0(FP), AX
	MOVQ	p1+8(FP), BX
	MOVQ	p2+16(FP), CX
	MOVQ	p3+24(FP), DX
	MOVQ	src+32(FP), SI
	MOVQ	n+40(FP), R9
	XORQ	R8, R8
	CMPQ	R9, $16
	JLT	tail

loop16:
	// Group i of the 64 bytes read from src is ai bi ci di, whose bytes go to p0, p1, p2 and p3.
	MOVOU	(SI), X0
	MOVOU	16(SI), X1
	MOVOU	32(SI), X2
	MOVOU	48(SI), X3
	MOVO	X0, X4
	PUNPCKLBW	X1, X0 // a0 a4 b0 b4 c0 c4 d0 d4 a1 a5 b1 b5 c1 c5 d1 d5
	PUNPCKHBW	X1, X4 // a2 a6 b2 b6 c2 c6 d2 d6 a3 a7 b3 b7 c3 c7 d3 d7
	MOVO	X2, X5
	PUNPCKLBW	X3, X2 // a8 a12 b8 b12 ... a9 a13 b9 b13 ...
	PUNPCKHBW	X3, X5 // a10 a14 b10 b14 ... a11 a15 b11 b15 ...
	MOVO	X0, X1
	PUNPCKLBW	X4, X0 // a0 a2 a4 a6 b0 b2 b4 b6 c0 c2 c4 c6 d0 d2 d4 d6
	PUNPCKHBW	X4, X1 // a1 a3 a5 a7 b1 b3 b5 b7 c1 c3 c5 c7 d1 d3 d5 d7
	MOVO	X2, X3
	PUNPCKLBW	X5, X2 // a8 a10 a12 a14 b8 b10 b12 b14 ...
	PUNPCKHBW	X5, X3 // a9 a11 a13 a15 b9 b11 b13 b15 ...
	MOVO	X0, X4
	PUNPCKLBW	X1, X0 // a0 ... a7 b0 ... b7
	PUNPCKHBW	X1, X4 // c0 ... c7 d0 ... d7
	MOVO	X2, X5
	PUNPCKLBW	X3, X2 // a8 ... a15 b8 ... b15
	PUNPCKHBW	X3, X5 // c8 ... c15 d8 ... d15
	MOVO	X0, X1
	PUNPCKLQDQ	X2, X0 // a0 ... a15
	PUNPCKHQDQ	X2, X1 // b0 ... b15
	MOVO	X4, X3
	PUNPCKLQDQ	X5, X4 // c0 ... c15
	PUNPCKHQDQ	X5, X3 // d0 ... d15
	MOVOU	X0, (AX)(R8*1)
	MOVOU	X1, (BX)(R8*1)
	MOVOU	X4, (CX)(R8*1)
	MOVOU	X3, (DX)(R8*1)
	ADDQ	$16, R8
	ADDQ	$64, SI
	SUBQ	$16, R9
	CMPQ	R9, $16
	JGE	loop16

tail:
	TESTQ	R9, R9
	JLE	done

loop1:
	MOVBLZX	(SI), R10
	MOVB	R10, (AX)(R8*1)
	MOVBLZX	1(SI), R10
	MOVB	R10, (BX)(R8*1)
	MOVBLZX	2(SI), R10
	MOVB	R10, (CX)(R8*1)
	MOVBLZX	3(SI), R10
	MOVB	R10, (DX)(R8*1)
	INCQ	R8
	ADDQ	$4, SI
	DECQ	R9
	JNZ	loop1

done:
	RET
