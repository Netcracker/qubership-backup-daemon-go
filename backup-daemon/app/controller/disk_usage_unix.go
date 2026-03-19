//go:build !windows

package controller

import (
	"math"
	"syscall"
)

// getDiskUsage returns totalSpace, freeSpace, usedSpace, totalInodes, freeInodes, usedInodes
// for the filesystem containing the given path.
func getDiskUsage(fsPath string) (int, int, int, int, int, int) {
	if fsPath == "" {
		fsPath = "."
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(fsPath, &stat); err != nil {
		return 0, 0, 0, 0, 0, 0
	}
	totalSpace := uint64(stat.Blocks) * uint64(stat.Bsize)
	freeSpace := uint64(stat.Bfree) * uint64(stat.Bsize)
	size := totalSpace - freeSpace
	totalInodes := uint64(stat.Files)
	freeInodes := uint64(stat.Ffree)
	usedInodes := totalInodes - freeInodes
	return clampToInt(totalSpace), clampToInt(freeSpace), clampToInt(size),
		clampToInt(totalInodes), clampToInt(freeInodes), clampToInt(usedInodes)
}

func clampToInt(v uint64) int {
	if v > uint64(math.MaxInt) {
		return math.MaxInt
	}
	return int(v)
}
