//go:build unix

package storage

import (
	"errors"
	"syscall"
)

// freeBytes is the space available to this process on dir's filesystem.
func freeBytes(dir string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil
}

// isNoSpace reports whether err is the filesystem refusing for lack of space.
func isNoSpace(err error) bool {
	return errors.Is(err, syscall.ENOSPC)
}
