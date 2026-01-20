package repo

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
)

const VaultNameFormat = "20060102T150405"
const FULL = "full"
const GRANULAR = "granular"
const ALL = "all"
const SHARDED = "sharded"

type StorageRepository interface {
	GetVault(vaultName string, external bool, vaultPath string, blobPath string, skipFSCheck bool) entity.Vault
	FindByTS(timestamp string, typeOfBackup string, storagePath string) (string, error)
	OpenVault(vaultName string, allowEviction bool, isGranular bool, isSharded bool, isExternal bool, vaultPath string, backupPrefix string, blobPath string) entity.Vault
	Evict(vaultName string) error
	ProtGetAsStream(backupID string, archiveFile string) (*os.File, error)
	List(typeOfBackup string, storagePath string) ([]entity.Vault, error)
	ListVaultNames(convertToTs bool, typeOfBackup string, storagePath string) ([]string, error)
	GetNonEvictableVaults(typeOfBackup string) (map[int64]bool, error)
	GetName(folder string) string
}

type StorageRepo struct {
	root                string
	granularFolder      string
	externalRoot        string
	namespace           string
	restoreLogsFolder   string
	allowPrefix         bool
	vaultDirnameMatcher *regexp.Regexp
	skipLockCheck       bool
}

func NewStorageRepo(root string, externalRoot string, namespace string, allowPrefix bool) StorageRepository {
	return &StorageRepo{
		root:                root,
		granularFolder:      filepath.Join(root, GRANULAR),
		externalRoot:        externalRoot,
		namespace:           namespace,
		restoreLogsFolder:   filepath.Join(root, "restore_logs"),
		allowPrefix:         allowPrefix,
		vaultDirnameMatcher: regexp.MustCompile(`(?i)\d{8}T\d{4,6}`),
		skipLockCheck:       strings.ToLower(os.Getenv("SKIP_LOCK_CHECK")) == "true",
	}
}

func (v *StorageRepo) GetVault(vaultName string, external bool, vaultPath string, blobPath string, skipFSCheck bool) entity.Vault {
	if strings.TrimSpace(vaultName) == "" {
		return entity.Vault{}
	}

	makeVault := func(folder string) entity.Vault {
		return entity.Vault{
			Folder:             folder,
			TimeStamp:          v.createTime(vaultName),
			MetricsFilePath:    fmt.Sprintf("%s/.metrics", folder),
			CustomVarsFilePath: fmt.Sprintf("%s/.custom_vars", folder),
			IsEvictable:        true,
			IsSharded:          false,
			External:           false,
			IsLocked:           v.isLocked(folder),
			IsGranular:         v.isGranular(folder),
		}
	}

	if !external {
		if strings.TrimSpace(blobPath) != "" {
			base := filepath.Join(v.root, blobPath)
			folder := filepath.Join(base, vaultName)

			if skipFSCheck || v.exists(folder) {
				return makeVault(folder)
			}
			return entity.Vault{}
		}

		folder := filepath.Join(v.root, vaultName)
		if skipFSCheck || v.exists(folder) {
			return makeVault(folder)
		}

		granularFolderPath := filepath.Join(v.granularFolder, vaultName)
		if v.exists(granularFolderPath) {
			vault := makeVault(granularFolderPath)
			vault.IsGranular = true
			return vault
		}

		return entity.Vault{}
	}

	if len(vaultPath) > 0 {
		externalFolder := filepath.Join(v.externalRoot, vaultPath, vaultName)
		if skipFSCheck || v.exists(externalFolder) {
			vault := makeVault(externalFolder)
			vault.External = true
			return vault
		}
	}

	return entity.Vault{}
}

func (v *StorageRepo) FindByTS(timestamp string, typeOfBackup string, storagePath string) (string, error) {
	vaults, err := v.List(typeOfBackup, storagePath)
	if err != nil {
		return "", fmt.Errorf("error listing vaults: %w", err)
	}
	convertedTimestamp, err := strconv.Atoi(timestamp)
	if err != nil {
		return "", fmt.Errorf("timestamp %s is in incorrect format: %w", timestamp, err)
	}
	for _, vault := range vaults {
		if vault.TimeStamp >= int64(convertedTimestamp) {
			return filepath.Base(vault.Folder), nil
		}
	}
	return "", fmt.Errorf("%w in timestamp %s", ErrNoVaults, timestamp)
}

func (v *StorageRepo) OpenVault(vaultName string, allowEviction bool, isGranular bool, isSharded bool,
	isExternal bool, vaultPath string, backupPrefix string, blobPath string) entity.Vault {
	vault := v.GetVault(vaultName, isExternal, vaultPath, blobPath, false)
	if len(vault.Folder) > 0 {
		return vault
	}
	folder := ""
	if isGranular {
		folder = v.granularFolder
	} else {
		if blobPath != "" {
			folder = blobPath
		} else if !isExternal {
			folder = v.root
		} else {
			folder = filepath.Join(v.externalRoot, vaultPath)
		}
	}
	if len(vaultName) == 0 {
		return entity.Vault{
			Folder:      filepath.Join(folder, v.getVaultName(backupPrefix, isGranular)),
			TimeStamp:   v.createTime(v.basename(folder)),
			IsEvictable: allowEviction,
			IsSharded:   isSharded,
		}
	}
	return entity.Vault{
		Folder:      filepath.Join(folder, vaultName),
		TimeStamp:   v.createTime(v.basename(folder)),
		IsEvictable: allowEviction,
		IsSharded:   isSharded,
	}
}

func (v *StorageRepo) Evict(vaultName string) error {
	return v.removeTree(vaultName)
}

func (v *StorageRepo) ProtGetAsStream(backupID string, archiveFile string) (*os.File, error) {
	backupFolder := v.GetVault(backupID, false, "", "", false).Folder
	fullFilePath := filepath.Join(backupFolder, archiveFile)
	file, err := os.Open(fullFilePath)
	if err != nil {
		return nil, fmt.Errorf("error opening backup file: %v", err)
	}
	return file, nil
}

func (v *StorageRepo) createTime(folderName string) int64 {
	parts := strings.Split(folderName, "_")
	if len(parts) == 0 {
		return time.Now().UnixMilli()
	}
	dateStr := parts[len(parts)-1]
	if idx := strings.LastIndex(dateStr, "."); idx != -1 {
		dateStr = dateStr[:idx]
	}
	t, err := time.Parse(VaultNameFormat, dateStr)
	if err != nil {
		return time.Now().UnixMilli()
	}
	return t.UnixMilli()
}

func (v *StorageRepo) GetName(folder string) string {
	return v.basename(folder)
}

func (v *StorageRepo) basename(path string) string {
	return filepath.Base(path)
}

func (v *StorageRepo) exists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func (v *StorageRepo) List(typeOfBackup string, storagePath string) ([]entity.Vault, error) {
	storageRootPath := filepath.Join(v.externalRoot, storagePath)
	if len(storagePath) == 0 {
		storageRootPath = v.root
	}
	var dirs []string
	if !v.exists(storageRootPath) {
		return []entity.Vault{}, ErrNoVaults
	}
	if typeOfBackup == GRANULAR || typeOfBackup == ALL {
		pathToDir := filepath.Join(storageRootPath, GRANULAR)
		files, err := os.ReadDir(pathToDir)
		if err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read dir %s: %v", pathToDir, err)
		}
		for _, file := range files {
			dirs = append(dirs, filepath.Join(GRANULAR, file.Name()))
		}
	}
	if typeOfBackup == FULL || typeOfBackup == ALL {
		files, err := os.ReadDir(storageRootPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read dir %s: %v", storageRootPath, err)
		}
		for _, file := range files {
			dirs = append(dirs, file.Name())
		}
	}
	var vaults []entity.Vault
	for _, dir := range dirs {
		trimmed := strings.Replace(dir, GRANULAR+"/", "", 1)
		parts := strings.Split(trimmed, "_")
		lastPart := parts[len(parts)-1]
		if v.vaultDirnameMatcher.MatchString(lastPart) {
			vault := v.GetVault(dir, false, storagePath, "", true)
			vaults = append(vaults, vault)
		}
	}
	if typeOfBackup == SHARDED {
		var shardedVaults []entity.Vault
		for _, vault := range vaults {
			if v.exists(filepath.Join(vault.Folder, ".sharded")) {
				shardedVaults = append(shardedVaults, vault)
			}
		}
		vaults = shardedVaults
	}
	if !v.skipLockCheck {
		var lockedVaults []entity.Vault
		for _, vault := range vaults {
			if !v.exists(filepath.Join(vault.Folder, ".lock")) {
				lockedVaults = append(lockedVaults, vault)
			}
		}
		vaults = lockedVaults
	}

	sort.Slice(vaults, func(i, j int) bool {
		return vaults[i].TimeStamp < vaults[j].TimeStamp
	})
	return vaults, nil
}

func (v *StorageRepo) ListVaultNames(convertToTs bool, typeOfBackup string, storagePath string) ([]string, error) {
	vaults, err := v.List(typeOfBackup, storagePath)
	if err != nil {
		return nil, fmt.Errorf("error listing vaults: %v", err)
	}
	var vaultNames []string
	for _, vault := range vaults {
		if convertToTs {
			vaultNames = append(vaultNames, strconv.Itoa(int(v.createTime(v.basename(vault.Folder)))))
			continue
		}
		vaultNames = append(vaultNames, v.basename(vault.Folder))
	}
	return vaultNames, nil
}

func (v *StorageRepo) getVaultName(prefix string, isGranular bool) string {
	if !isGranular || v.namespace == "" || !v.allowPrefix {
		return time.Now().Format(VaultNameFormat)
	}
	vaultName := ""
	if len(prefix) > 0 {
		vaultName += prefix + "_"
	}
	vaultName += v.namespace + "_" + time.Now().Format(VaultNameFormat)
	return vaultName
}

func (v *StorageRepo) removeTree(path string) error {
	err := os.RemoveAll(path)
	if err != nil {
		return fmt.Errorf("failed to remove %s: %v", path, err)
	}
	return nil
}

func (v *StorageRepo) GetNonEvictableVaults(typeOfBackup string) (map[int64]bool, error) {
	vaults := make(map[int64]bool)
	listVaults, err := v.List(typeOfBackup, "")
	if err != nil {
		return nil, fmt.Errorf("error listing vaults: %v", err)
	}
	for _, vault := range listVaults {
		if v.exists(filepath.Join(vault.Folder, ".evictlock")) {
			vaults[vault.TimeStamp] = true
		}
	}
	return vaults, nil
}

func (v *StorageRepo) isLocked(folder string) bool {
	return v.exists(filepath.Join(folder, ".lock"))
}

func (v *StorageRepo) isGranular(folder string) bool {
	return strings.Contains(v.basename(folder), GRANULAR)
}
