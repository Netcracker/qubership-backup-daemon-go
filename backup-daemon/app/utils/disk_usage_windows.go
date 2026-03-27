//go:build windows

package utils

// GetDiskUsage is not supported on Windows; returns zeros.
func getDiskUsage(fsPath string) (int, int, int, int, int, int) {
	return 0, 0, 0, 0, 0, 0
}

func clampToInt(v uint64) int {
	return int(v)
}
