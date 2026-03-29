package repo

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
)

func TestGetVault(t *testing.T) {
	root := t.TempDir()
	externalRoot := t.TempDir()

	// Create vault directories needed for non-skipFSCheck tests
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
				IsEvictable:        false,
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

	// Create granular vault directory
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

	// Create granular vault directory
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
