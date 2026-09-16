//go:build windows

package storage

import (
	"errors"
	"syscall"
	"unsafe"
)

const (
	errorHandleDiskFull syscall.Errno = 39
	errorDiskFull       syscall.Errno = 112
)

var getDiskFreeSpaceEx = syscall.NewLazyDLL("kernel32.dll").NewProc("GetDiskFreeSpaceExW")

// freeBytes is the space available to this process on dir's volume.
func freeBytes(dir string) (uint64, error) {
	p, err := syscall.UTF16PtrFromString(dir)
	if err != nil {
		return 0, err
	}
	var avail, total, free uint64
	r, _, err := getDiskFreeSpaceEx.Call(uintptr(unsafe.Pointer(p)),
		uintptr(unsafe.Pointer(&avail)), uintptr(unsafe.Pointer(&total)), uintptr(unsafe.Pointer(&free)))
	if r == 0 {
		return 0, err
	}
	return avail, nil
}

// isNoSpace reports whether err is the filesystem refusing for lack of space.
func isNoSpace(err error) bool {
	return errors.Is(err, errorHandleDiskFull) || errors.Is(err, errorDiskFull) || errors.Is(err, syscall.ENOSPC)
}
