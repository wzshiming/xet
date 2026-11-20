//go:build windows

package sysmem

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

// memoryStatusEx is MEMORYSTATUSEX, which golang.org/x/sys/windows lacks.
type memoryStatusEx struct {
	length    uint32
	_         uint32
	totalPhys uint64
	_         [6]uint64
}

var procGlobalMemoryStatusEx = windows.NewLazySystemDLL("kernel32.dll").NewProc("GlobalMemoryStatusEx")

func usable() (int64, bool) {
	if procGlobalMemoryStatusEx.Find() != nil {
		return 0, false
	}
	var m memoryStatusEx
	m.length = uint32(unsafe.Sizeof(m))
	if r, _, _ := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&m))); r == 0 {
		return 0, false
	}
	return int64(m.totalPhys), true
}
