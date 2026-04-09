package repo

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
)

func createVaultDir(t *testing.T, folder string, lock, evictlock, sharded bool) {
	t.Helper()
	if err := os.MkdirAll(folder, 0o755); err != nil {
		t.Fatalf("failed to create vault dir %s: %v", folder, err)
	}
	touch := func(name string) {
		f, err := os.Create(filepath.Join(folder, name))
		if err != nil {
			t.Fatalf("failed to create %s: %v", name, err)
		}
		_ = f.Close()
	}
	if lock {
		touch(".lock")
	}
	if evictlock {
		touch(".evictlock")
	}
	if sharded {
		touch(".sharded")
	}
}

func TestGetVault(t *testing.T) {
	root := t.TempDir()
	externalRoot := t.TempDir()

	if err := os.MkdirAll(filepath.Join(root, "skipFSCheck_20240101T000000.txt"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, GRANULAR, "skipFSChec_20240101T000000.txt"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(externalRoot, "skipFSCheck_20240101T000000.txt"), 0o755); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name          string
		vaultName     string
		external      bool
		vaultPath     string
		skipFSCheck   bool
		expectedVault entity.Vault
	}{
		{
			name:        "skipFSCheck true",
			vaultName:   "skipFSCheck_20240101T000000.txt",
			external:    false,
			vaultPath:   "",
			skipFSCheck: true,
			expectedVault: entity.Vault{
				Folder:             filepath.Join(root, "skipFSCheck_20240101T000000.txt"),
				TimeStamp:          1704067200000,
				MetricsFilePath:    fmt.Sprintf("%s/.metrics", filepath.Join(root, "skipFSCheck_20240101T000000.txt")),
				CustomVarsFilePath: fmt.Sprintf("%s/.custom_vars", filepath.Join(root, "skipFSCheck_20240101T000000.txt")),
				IsEvictable:        true,
				IsSharded:          false,
				External:           false,
			},
		},
		{
			name:        "skipFSCheck false",
			vaultName:   "skipFSCheck_20240101T000000.txt",
			external:    false,
			vaultPath:   "",
			skipFSCheck: false,
			expectedVault: entity.Vault{
				Folder:             filepath.Join(root, "skipFSCheck_20240101T000000.txt"),
				TimeStamp:          1704067200000,
				MetricsFilePath:    fmt.Sprintf("%s/.metrics", filepath.Join(root, "skipFSCheck_20240101T000000.txt")),
				CustomVarsFilePath: fmt.Sprintf("%s/.custom_vars", filepath.Join(root, "skipFSCheck_20240101T000000.txt")),
				IsEvictable:        true,
				IsSharded:          false,
				External:           false,
			},
		},
		{
			name:        "granular folder",
			vaultName:   "skipFSChec_20240101T000000.txt",
			external:    false,
			vaultPath:   "",
			skipFSCheck: false,
			expectedVault: entity.Vault{
				Folder:             filepath.Join(root, GRANULAR, "skipFSChec_20240101T000000.txt"),
				TimeStamp:          1704067200000,
				MetricsFilePath:    fmt.Sprintf("%s/.metrics", filepath.Join(root, GRANULAR, "skipFSChec_20240101T000000.txt")),
				CustomVarsFilePath: fmt.Sprintf("%s/.custom_vars", filepath.Join(root, GRANULAR, "skipFSChec_20240101T000000.txt")),
				IsEvictable:        true,
				IsSharded:          false,
				External:           false,
				IsGranular:         true,
			},
		},
		{

			name:        "external folder",
			vaultName:   "skipFSCheck_20240101T000000.txt",
			external:    true,
			vaultPath:   "/",
			skipFSCheck: false,
			expectedVault: entity.Vault{
				Folder:             filepath.Join(externalRoot, "skipFSCheck_20240101T000000.txt"),
				TimeStamp:          1704067200000,
				MetricsFilePath:    fmt.Sprintf("%s/.metrics", filepath.Join(externalRoot, "skipFSCheck_20240101T000000.txt")),
				CustomVarsFilePath: fmt.Sprintf("%s/.custom_vars", filepath.Join(externalRoot, "skipFSCheck_20240101T000000.txt")),
				IsEvictable:        true, // нет .evictlock → evictable
				IsSharded:          false,
				External:           true,
			},
		},
		{
			name:          "empty vault",
			vaultName:     "skipFSCheck_20240101T000000.txt",
			external:      true,
			vaultPath:     "nonexistent_path",
			skipFSCheck:   false,
			expectedVault: entity.Vault{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storageRepo := NewStorageRepo(root, externalRoot,
				"namespace", false)
			vault := storageRepo.GetVault(tc.vaultName, tc.external, tc.vaultPath, "", tc.skipFSCheck)
			if !reflect.DeepEqual(vault, tc.expectedVault) {
				t.Fatalf("Expected Vault %v, got %v", tc.expectedVault, vault)
			}
		})
	}
}

func TestFindByTS(t *testing.T) {
	root := t.TempDir()

	if err := os.MkdirAll(filepath.Join(root, GRANULAR, "skipFSChec_20240101T000000.txt"), 0o755); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name             string
		timeStamp        string
		typeOfBackup     string
		storagePath      string
		expectedFileName string
		expectedError    error
	}{
		{
			name:             "success",
			timeStamp:        "1704067200",
			typeOfBackup:     GRANULAR,
			storagePath:      "",
			expectedFileName: "skipFSChec_20240101T000000.txt",
			expectedError:    nil,
		},
		{
			name:             "incorrect timestamp",
			timeStamp:        "1755065220R",
			typeOfBackup:     FULL,
			storagePath:      "",
			expectedFileName: "",
			expectedError:    strconv.ErrSyntax,
		},
		{
			name:             "vault not found with timestamp",
			timeStamp:        "1855072420000",
			typeOfBackup:     GRANULAR,
			storagePath:      "",
			expectedFileName: "",
			expectedError:    ErrNoVaults,
		},
		{
			name:             "vault not found in storage",
			timeStamp:        "1755072420",
			typeOfBackup:     FULL,
			storagePath:      "/eeeeee",
			expectedFileName: "",
			expectedError:    ErrNoVaults,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storageRepo := NewStorageRepo(root, root,
				"namespace", false)
			fileName, err := storageRepo.FindByTS(tc.timeStamp, tc.typeOfBackup, tc.storagePath)
			if !errors.Is(err, tc.expectedError) {
				t.Fatalf("Expected error %v, got %v", tc.expectedError, err)
			}
			if fileName != tc.expectedFileName {
				t.Fatalf("Expected file name %s, got %s", tc.expectedFileName, fileName)
			}
		})
	}
}

func TestListValueName(t *testing.T) {
	root := t.TempDir()

	if err := os.MkdirAll(filepath.Join(root, GRANULAR, "skipFSChec_20240101T000000.txt"), 0o755); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name          string
		convertToTS   bool
		typeOfBackup  string
		storagePath   string
		expectedList  []string
		expectedError error
	}{
		{
			name:          "success",
			typeOfBackup:  GRANULAR,
			convertToTS:   false,
			storagePath:   "",
			expectedList:  []string{"skipFSChec_20240101T000000.txt"},
			expectedError: nil,
		},
		{
			name:          "timestamp",
			typeOfBackup:  GRANULAR,
			convertToTS:   true,
			storagePath:   "",
			expectedList:  []string{"1704067200000"},
			expectedError: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storageRepo := NewStorageRepo(root, root,
				"namespace", false)
			vaults, err := storageRepo.ListVaultNames(tc.convertToTS, tc.typeOfBackup, tc.storagePath)
			if !errors.Is(err, tc.expectedError) {
				t.Fatalf("Expected error %v, got %v", tc.expectedError, err)
			}
			if len(vaults) != len(tc.expectedList) {
				t.Fatalf("Expected %d vaults, got %d", len(tc.expectedList), len(vaults))
			}
			if vaults[0] != tc.expectedList[0] {
				t.Fatalf("Expected %s vault, got %s", tc.expectedList[0], vaults[0])
			}
		})
	}
}

func TestInitVault(t *testing.T) {
	testCases := []struct {
		name            string
		isEvictable     bool
		isSharded       bool
		expectLock      bool
		expectEvictlock bool
		expectSharded   bool
	}{
		{
			name:            "evictable non-sharded",
			isEvictable:     true,
			isSharded:       false,
			expectLock:      false,
			expectEvictlock: false,
			expectSharded:   false,
		},
		{
			name:            "non-evictable non-sharded",
			isEvictable:     false,
			isSharded:       false,
			expectLock:      false,
			expectEvictlock: true,
			expectSharded:   false,
		},
		{
			name:            "evictable sharded",
			isEvictable:     true,
			isSharded:       true,
			expectLock:      false,
			expectEvictlock: false,
			expectSharded:   true,
		},
		{
			name:            "non-evictable sharded",
			isEvictable:     false,
			isSharded:       true,
			expectLock:      false,
			expectEvictlock: true,
			expectSharded:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			repo := NewStorageRepo(root, root, "ns", false).(*StorageRepo)
			vault := entity.Vault{
				Folder:      filepath.Join(root, "20240101T000000"),
				IsEvictable: tc.isEvictable,
				IsSharded:   tc.isSharded,
			}

			if err := repo.InitVault(vault); err != nil {
				t.Fatalf("InitVault returned error: %v", err)
			}

			if _, err := os.Stat(vault.Folder); os.IsNotExist(err) {
				t.Error("vault directory was not created")
			}

			checkFile := func(name string, expect bool) {
				path := filepath.Join(vault.Folder, name)
				_, err := os.Stat(path)
				exists := !os.IsNotExist(err)
				if exists != expect {
					t.Errorf("file %s: expected exists=%v, got %v", name, expect, exists)
				}
			}

			checkFile(".lock", tc.expectLock)
			checkFile(".evictlock", tc.expectEvictlock)
			checkFile(".sharded", tc.expectSharded)
		})
	}
}

func TestCloseVault(t *testing.T) {
	t.Run("removes .lock file", func(t *testing.T) {
		root := t.TempDir()
		repo := NewStorageRepo(root, root, "ns", false).(*StorageRepo)
		folder := filepath.Join(root, "20240101T000000")
		createVaultDir(t, folder, true, false, false)

		vault := entity.Vault{Folder: folder}
		if err := repo.CloseVault(vault); err != nil {
			t.Fatalf("CloseVault returned error: %v", err)
		}
		lockPath := filepath.Join(folder, ".lock")
		if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
			t.Error(".lock file should be removed after CloseVault")
		}
	})

	t.Run("no error if .lock does not exist", func(t *testing.T) {
		root := t.TempDir()
		repo := NewStorageRepo(root, root, "ns", false).(*StorageRepo)
		folder := filepath.Join(root, "20240101T000000")
		createVaultDir(t, folder, false, false, false)

		vault := entity.Vault{Folder: folder}
		if err := repo.CloseVault(vault); err != nil {
			t.Fatalf("CloseVault should not return error when .lock missing: %v", err)
		}
	})
}

func TestOpenVault(t *testing.T) {
	t.Run("creates new full vault", func(t *testing.T) {
		root := t.TempDir()
		repo := NewStorageRepo(root, root, "ns", false)

		vault, err := repo.OpenVault("20240101T000000", true, false, false, false, "", "", "")
		if err != nil {
			t.Fatalf("OpenVault error: %v", err)
		}
		if vault.Folder == "" {
			t.Fatal("vault.Folder should not be empty")
		}
	})

	t.Run("creates new granular vault", func(t *testing.T) {
		root := t.TempDir()
		repo := NewStorageRepo(root, root, "ns", false)

		vault, err := repo.OpenVault("20240101T120000", true, true, false, false, "", "", "")
		if err != nil {
			t.Fatalf("OpenVault error: %v", err)
		}
		// должна быть в granular подпапке
		if !strings.Contains(vault.Folder, GRANULAR) {
			t.Errorf("granular vault folder should contain %q, got %s", GRANULAR, vault.Folder)
		}
	})

	t.Run("creates non-evictable vault with .evictlock", func(t *testing.T) {
		root := t.TempDir()
		repo := NewStorageRepo(root, root, "ns", false)

		vault, err := repo.OpenVault("20240101T000000", false, false, false, false, "", "", "")
		if err != nil {
			t.Fatalf("OpenVault error: %v", err)
		}
		// .evictlock должен быть создан
		if _, err := os.Stat(filepath.Join(vault.Folder, ".evictlock")); os.IsNotExist(err) {
			t.Error(".evictlock file should exist for non-evictable vault")
		}
	})

	t.Run("creates sharded vault with .sharded", func(t *testing.T) {
		root := t.TempDir()
		repo := NewStorageRepo(root, root, "ns", false)

		vault, err := repo.OpenVault("20240101T000000", true, false, true, false, "", "", "")
		if err != nil {
			t.Fatalf("OpenVault error: %v", err)
		}
		if _, err := os.Stat(filepath.Join(vault.Folder, ".sharded")); os.IsNotExist(err) {
			t.Error(".sharded file should exist for sharded vault")
		}
	})

	t.Run("returns existing vault without reinit", func(t *testing.T) {
		root := t.TempDir()
		repo := NewStorageRepo(root, root, "ns", false)

		// создаём директорию заранее
		existingFolder := filepath.Join(root, "20240101T000000")
		createVaultDir(t, existingFolder, false, false, false)

		vault, err := repo.OpenVault("20240101T000000", true, false, false, false, "", "", "")
		if err != nil {
			t.Fatalf("OpenVault error: %v", err)
		}
		if vault.Folder != existingFolder {
			t.Errorf("expected existing folder %s, got %s", existingFolder, vault.Folder)
		}
	})

	t.Run("creates vault for backup with blob path", func(t *testing.T) {
		root := t.TempDir()
		repo := NewStorageRepo(root, root, "ns", true)

		vault, err := repo.OpenVault("", false, false, false, false, "", "prefixed", "backups/test-path")
		if err != nil {
			t.Fatalf("OpenVault error: %v", err)
		}
		expectedPathPrefix := filepath.Join(root, "backups/test-path")
		if !strings.Contains(vault.Folder, expectedPathPrefix) {
			t.Fatalf("wrong folder: %s, expected to contain %s", vault.Folder, expectedPathPrefix)
		}
	})
}

// TestList проверяет листинг vault'ов с учётом .lock фильтрации
func TestList(t *testing.T) {
	t.Run("lists full vaults (unlocked only)", func(t *testing.T) {
		root := t.TempDir()
		// vault без .lock → должен попасть в список
		createVaultDir(t, filepath.Join(root, "20240101T000000"), false, false, false)
		// vault с .lock → должен быть отфильтрован
		createVaultDir(t, filepath.Join(root, "20240201T000000"), true, false, false)

		repo := NewStorageRepo(root, root, "ns", false)
		vaults, err := repo.List(FULL, "")
		if err != nil {
			t.Fatalf("List error: %v", err)
		}
		if len(vaults) != 1 {
			t.Fatalf("expected 1 vault, got %d", len(vaults))
		}
		if vaults[0].IsLocked {
			t.Error("listed vault should not be locked")
		}
	})

	t.Run("lists granular vaults", func(t *testing.T) {
		root := t.TempDir()
		createVaultDir(t, filepath.Join(root, GRANULAR, "20240101T000000"), false, false, false)

		repo := NewStorageRepo(root, root, "ns", false)
		vaults, err := repo.List(GRANULAR, "")
		if err != nil {
			t.Fatalf("List error: %v", err)
		}
		if len(vaults) != 1 {
			t.Fatalf("expected 1 granular vault, got %d", len(vaults))
		}
		if !vaults[0].IsGranular {
			t.Error("vault should be marked as granular")
		}
	})

	t.Run("lists all vaults (full + granular)", func(t *testing.T) {
		root := t.TempDir()
		createVaultDir(t, filepath.Join(root, "20240101T000000"), false, false, false)
		createVaultDir(t, filepath.Join(root, GRANULAR, "20240201T000000"), false, false, false)

		repo := NewStorageRepo(root, root, "ns", false)
		vaults, err := repo.List(ALL, "")
		if err != nil {
			t.Fatalf("List error: %v", err)
		}
		if len(vaults) != 2 {
			t.Fatalf("expected 2 vaults (full+granular), got %d", len(vaults))
		}
	})

	t.Run("returns ErrNoVaults for nonexistent root", func(t *testing.T) {
		root := t.TempDir()
		repo := NewStorageRepo(root, root, "ns", false)
		_, err := repo.List(FULL, "/nonexistent")
		if !errors.Is(err, ErrNoVaults) {
			t.Errorf("expected ErrNoVaults, got %v", err)
		}
	})

	t.Run("vaults sorted by timestamp ascending", func(t *testing.T) {
		root := t.TempDir()
		createVaultDir(t, filepath.Join(root, "20240301T000000"), false, false, false)
		createVaultDir(t, filepath.Join(root, "20240101T000000"), false, false, false)
		createVaultDir(t, filepath.Join(root, "20240201T000000"), false, false, false)

		repo := NewStorageRepo(root, root, "ns", false)
		vaults, err := repo.List(FULL, "")
		if err != nil {
			t.Fatalf("List error: %v", err)
		}
		if len(vaults) != 3 {
			t.Fatalf("expected 3 vaults, got %d", len(vaults))
		}
		for i := 1; i < len(vaults); i++ {
			if vaults[i].TimeStamp < vaults[i-1].TimeStamp {
				t.Errorf("vaults not sorted: vault[%d].TimeStamp=%d < vault[%d].TimeStamp=%d",
					i, vaults[i].TimeStamp, i-1, vaults[i-1].TimeStamp)
			}
		}
	})
}

// TestGetNonEvictableVaults проверяет что возвращаются только vault'ы с .evictlock
func TestGetNonEvictableVaults(t *testing.T) {
	t.Run("returns only non-evictable vaults", func(t *testing.T) {
		root := t.TempDir()
		// evictable vault (нет .evictlock)
		createVaultDir(t, filepath.Join(root, "20240101T000000"), false, false, false)
		// non-evictable vault (есть .evictlock)
		createVaultDir(t, filepath.Join(root, "20240201T000000"), false, true, false)

		repo := NewStorageRepo(root, root, "ns", false)
		vaults, err := repo.GetNonEvictableVaults(FULL)
		if err != nil {
			t.Fatalf("GetNonEvictableVaults error: %v", err)
		}
		if len(vaults) != 1 {
			t.Fatalf("expected 1 non-evictable vault, got %d", len(vaults))
		}
		// значение должно быть true
		for _, v := range vaults {
			if !v {
				t.Error("non-evictable vault should have value true")
			}
		}
	})

	t.Run("no non-evictable vaults returns empty map", func(t *testing.T) {
		root := t.TempDir()
		createVaultDir(t, filepath.Join(root, "20240101T000000"), false, false, false)

		repo := NewStorageRepo(root, root, "ns", false)
		vaults, err := repo.GetNonEvictableVaults(FULL)
		if err != nil {
			t.Fatalf("GetNonEvictableVaults error: %v", err)
		}
		if len(vaults) != 0 {
			t.Errorf("expected empty map, got %d entries", len(vaults))
		}
	})
}

// TestEvict проверяет удаление vault директории
func TestEvict(t *testing.T) {
	t.Run("removes vault directory", func(t *testing.T) {
		root := t.TempDir()
		vaultFolder := filepath.Join(root, "20240101T000000")
		createVaultDir(t, vaultFolder, false, false, false)

		repo := NewStorageRepo(root, root, "ns", false)
		if err := repo.Evict(vaultFolder); err != nil {
			t.Fatalf("Evict error: %v", err)
		}
		if _, err := os.Stat(vaultFolder); !os.IsNotExist(err) {
			t.Error("vault directory should be removed after Evict")
		}
	})

	t.Run("no error for nonexistent path", func(t *testing.T) {
		root := t.TempDir()
		repo := NewStorageRepo(root, root, "ns", false)
		// os.RemoveAll не возвращает ошибку для несуществующего пути
		if err := repo.Evict(filepath.Join(root, "nonexistent")); err != nil {
			t.Fatalf("Evict should not error for nonexistent path: %v", err)
		}
	})
}

// TestIsGranular проверяет что isGranular смотрит на полный путь, а не только basename
func TestIsGranular(t *testing.T) {
	root := t.TempDir()
	repo := NewStorageRepo(root, root, "ns", false).(*StorageRepo)

	testCases := []struct {
		name     string
		folder   string
		expected bool
	}{
		{
			name:     "full path contains granular",
			folder:   filepath.Join(root, GRANULAR, "20240101T000000"),
			expected: true,
		},
		{
			name:     "basename only (no granular in path)",
			folder:   filepath.Join(root, "20240101T000000"),
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := repo.isGranular(tc.folder)
			if got != tc.expected {
				t.Errorf("isGranular(%q) = %v, want %v", tc.folder, got, tc.expected)
			}
		})
	}
}
