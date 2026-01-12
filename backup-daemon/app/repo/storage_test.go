package repo

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
)

func TestGetVault(t *testing.T) {
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
			vaultPath:   "./path",
			skipFSCheck: true,
			expectedVault: entity.Vault{
				Folder:             filepath.Join("./", "skipFSCheck_20240101T000000.txt"),
				TimeStamp:          1704067200,
				MetricsFilePath:    fmt.Sprintf("%s/.metrics", "skipFSCheck_20240101T000000.txt"),
				CustomVarsFilePath: fmt.Sprintf("%s/.custom_vars", "skipFSCheck_20240101T000000.txt"),
				IsEvictable:        true,
				IsSharded:          false,
				External:           false,
			},
		},
		{
			name:        "skipFSCheck false",
			vaultName:   "skipFSCheck_20240101T000000.txt",
			external:    false,
			vaultPath:   "./path",
			skipFSCheck: false,
			expectedVault: entity.Vault{
				Folder:             filepath.Join("./", "skipFSCheck_20240101T000000.txt"),
				TimeStamp:          1704067200,
				MetricsFilePath:    fmt.Sprintf("%s/.metrics", "skipFSCheck_20240101T000000.txt"),
				CustomVarsFilePath: fmt.Sprintf("%s/.custom_vars", "skipFSCheck_20240101T000000.txt"),
				IsEvictable:        true,
				IsSharded:          false,
				External:           false,
			},
		},
		{
			name:        "granular folder",
			vaultName:   "skipFSChec_20240101T000000.txt",
			external:    false,
			vaultPath:   "./path",
			skipFSCheck: false,
			expectedVault: entity.Vault{
				Folder:             filepath.Join(GRANULAR, "skipFSChec_20240101T000000.txt"),
				TimeStamp:          1704067200,
				MetricsFilePath:    fmt.Sprintf("%s/.metrics", "skipFSChec_20240101T000000.txt"),
				CustomVarsFilePath: fmt.Sprintf("%s/.custom_vars", "skipFSChec_20240101T000000.txt"),
				IsEvictable:        true,
				IsSharded:          false,
				External:           false,
			},
		},
		{
			name:        "external folder",
			vaultName:   "skipFSCheck_20240101T000000.txt",
			external:    true,
			vaultPath:   "/",
			skipFSCheck: false,
			expectedVault: entity.Vault{
				Folder:             filepath.Join("./", "skipFSCheck_20240101T000000.txt"),
				TimeStamp:          1704067200,
				MetricsFilePath:    fmt.Sprintf("%s/.metrics", "skipFSCheck_20240101T000000.txt"),
				CustomVarsFilePath: fmt.Sprintf("%s/.custom_vars", "skipFSCheck_20240101T000000.txt"),
				IsEvictable:        true,
				IsSharded:          false,
				External:           true,
			},
		},
		{
			name:          "empty vault",
			vaultName:     "skipFSCheck_20240101T000000.txt",
			external:      true,
			vaultPath:     "d",
			skipFSCheck:   false,
			expectedVault: entity.Vault{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storageRepo := NewStorageRepo("./", "./",
				"namespace", false)
			vault := storageRepo.GetVault(tc.vaultName, tc.external, tc.vaultPath, "", tc.skipFSCheck)
			if !reflect.DeepEqual(vault, tc.expectedVault) {
				t.Fatalf("Expected Vault %v, got %v", tc.expectedVault, vault)
			}
		})
	}
}

func TestFindByTS(t *testing.T) {
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
			timeStamp:        "1855072420",
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
			storageRepo := NewStorageRepo("./", "fileSystem",
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
			expectedList:  []string{"1704067200"},
			expectedError: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storageRepo := NewStorageRepo("./", "fileSystem",
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
