package repo

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// makeVaultDir creates a directory named after the given timestamp string.
func makeVaultDir(root, name string) {
	_ = os.MkdirAll(filepath.Join(root, name), 0o755)
}

// makeSomeEvictLocked marks the first `locked` vaults as non-evictable.
func makeSomeEvictLocked(root string, names []string, locked int) {
	for i, name := range names {
		if i >= locked {
			break
		}
		_ = os.WriteFile(filepath.Join(root, name, ".evictlock"), nil, 0o644)
	}
}

// buildVaultNames returns n vault folder names spread across `days` calendar days.
func buildVaultNames(n, days int) []string {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	names := make([]string, n)
	for i := range n {
		t := base.Add(time.Duration(i) * (time.Duration(days) * 24 * time.Hour / time.Duration(n)))
		names[i] = t.Format(VaultNameFormat)
	}
	return names
}

// setupStorage creates a temporary directory tree with `n` full-backup vaults.
// Returns the root path and a cleanup function.
func setupStorage(tb testing.TB, n int) (root string, names []string) {
	tb.Helper()
	root = tb.TempDir()
	names = buildVaultNames(n, 365)
	for _, name := range names {
		makeVaultDir(root, name)
	}
	return root, names
}

// setupStorageWithGranular creates full + granular vaults.
func setupStorageWithGranular(tb testing.TB, nFull, nGranular int) string {
	tb.Helper()
	root := tb.TempDir()
	fullNames := buildVaultNames(nFull, 180)
	for _, name := range fullNames {
		makeVaultDir(root, name)
	}
	granNames := buildVaultNames(nGranular, 90)
	for _, name := range granNames {
		makeVaultDir(filepath.Join(root, GRANULAR), name)
	}
	return root
}

// ---------------------------------------------------------------------------
// BenchmarkStorageList — primary hot path: ReadDir + N×GetVault (N×os.Stat)
// ---------------------------------------------------------------------------

func BenchmarkStorageList_10(b *testing.B)   { benchStorageList(b, 10) }
func BenchmarkStorageList_100(b *testing.B)  { benchStorageList(b, 100) }
func BenchmarkStorageList_1000(b *testing.B) { benchStorageList(b, 1000) }

func benchStorageList(b *testing.B, n int) {
	b.Helper()
	root, _ := setupStorage(b, n)
	repo := NewStorageRepo(root, "", "", false)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = repo.List(FULL, "")
	}
}

// BenchmarkStorageList_ALL — also scans granular sub-directory
func BenchmarkStorageList_ALL_50_50(b *testing.B) {
	root := setupStorageWithGranular(b, 50, 50)
	repo := NewStorageRepo(root, "", "", false)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = repo.List(ALL, "")
	}
}

// ---------------------------------------------------------------------------
// BenchmarkListVaultNames — List + string conversion
// ---------------------------------------------------------------------------

func BenchmarkListVaultNames_100_NoConvert(b *testing.B) {
	root, _ := setupStorage(b, 100)
	repo := NewStorageRepo(root, "", "", false)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = repo.ListVaultNames(false, FULL, "")
	}
}

func BenchmarkListVaultNames_100_ConvertToTS(b *testing.B) {
	root, _ := setupStorage(b, 100)
	repo := NewStorageRepo(root, "", "", false)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = repo.ListVaultNames(true, FULL, "")
	}
}

// ---------------------------------------------------------------------------
// BenchmarkGetNonEvictableVaults — calls List internally (double stat cost)
// ---------------------------------------------------------------------------

func BenchmarkGetNonEvictableVaults_100_None(b *testing.B) {
	// No vaults have .evictlock — fast path.
	root, _ := setupStorage(b, 100)
	repo := NewStorageRepo(root, "", "", false)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = repo.GetNonEvictableVaults(ALL)
	}
}

func BenchmarkGetNonEvictableVaults_100_Half(b *testing.B) {
	root, names := setupStorage(b, 100)
	makeSomeEvictLocked(root, names, 50)
	repo := NewStorageRepo(root, "", "", false)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = repo.GetNonEvictableVaults(ALL)
	}
}

// ---------------------------------------------------------------------------
// BenchmarkFindByTS — List + linear scan
// ---------------------------------------------------------------------------

func BenchmarkFindByTS_100(b *testing.B) {
	root, names := setupStorage(b, 100)
	repo := NewStorageRepo(root, "", "", false)
	// Search for the timestamp of the last vault (worst case — scan all).
	lastTS := fmt.Sprintf("%d", mustCreateTime(names[len(names)-1]))

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = repo.FindByTS(lastTS, FULL, "")
	}
}

// mustCreateTime parses the vault folder name to a Unix-millisecond timestamp.
func mustCreateTime(name string) int64 {
	t, err := time.Parse(VaultNameFormat, name)
	if err != nil {
		panic(err)
	}
	return t.UnixMilli()
}

// ---------------------------------------------------------------------------
// BenchmarkGetVault — single vault lookup (3× os.Stat)
// ---------------------------------------------------------------------------

func BenchmarkGetVault_Exists(b *testing.B) {
	root, names := setupStorage(b, 1)
	repo := NewStorageRepo(root, "", "", false)
	name := names[0]

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = repo.GetVault(name, false, "", "", false)
	}
}

func BenchmarkGetVault_NotExists(b *testing.B) {
	root, _ := setupStorage(b, 1)
	repo := NewStorageRepo(root, "", "", false)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = repo.GetVault("20991231T235959", false, "", "", false)
	}
}

// ---------------------------------------------------------------------------
// BenchmarkOpenVault — creates a new vault directory + marker files
// ---------------------------------------------------------------------------

func BenchmarkOpenVault(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		root := b.TempDir()
		repo := NewStorageRepo(root, "", "", false)
		name := fmt.Sprintf("20260101T%06d", i%1000000)
		_, _ = repo.OpenVault(name, true, false, false, false, "", "", "")
	}
}
