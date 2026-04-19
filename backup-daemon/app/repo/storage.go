package repo

import (
	"errors"
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
const S3_PROCESSING = "s3-processing"

//go:generate mockgen -source=storage.go -destination=../repo/storage-mock.go -package=repo
type StorageRepository interface {
	GetVault(vaultName string, external bool, vaultPath string, blobPath string, skipFSCheck bool) entity.Vault
	FindByTS(timestamp string, typeOfBackup string, storagePath string) (string, error)
	OpenVault(vaultName string, allowEviction bool, isGranular bool, isSharded bool, isExternal bool, vaultPath string, backupPrefix string, blobPath string) (entity.Vault, error)
	Evict(vaultName string) error
	ProtGetAsStream(backupID string, archiveFile string) (*os.File, error)
	List(typeOfBackup string, storagePath string) ([]entity.Vault, error)
	ListVaultNames(convertToTs bool, typeOfBackup string, storagePath string) ([]string, error)
	GetNonEvictableVaults(typeOfBackup string) (map[int64]bool, error)
	GetName(folder string) string
	CloseVault(vault entity.Vault) error
	GetRoot() string
}

type vaultMarkers struct {
	locked    bool
	evictLock bool
	sharded   bool
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

	makeVault := func(folder string, markers vaultMarkers) entity.Vault {
		return entity.Vault{
			Folder:             folder,
			TimeStamp:          v.createTime(v.basename(folder)),
			MetricsFilePath:    fmt.Sprintf("%s/.metrics", folder),
			CustomVarsFilePath: fmt.Sprintf("%s/.custom_vars", folder),
			IsEvictable:        !markers.evictLock,
			IsSharded:          markers.sharded,
			External:           false,
			IsLocked:           markers.locked,
			IsGranular:         v.isGranular(folder),
		}
	}

	if !external {
		if strings.TrimSpace(blobPath) != "" {
			base := filepath.Join(v.root, S3_PROCESSING)
			folder := filepath.Join(base, vaultName)

			if skipFSCheck || v.exists(folder) {
				mk, err := v.readVaultMarkers(folder)
				if err != nil {
					// need to add return err
					return entity.Vault{}
				}

				return makeVault(folder, mk)
			}
			return entity.Vault{}
		}

		folder := filepath.Join(v.root, vaultName)
		if skipFSCheck || v.exists(folder) {

			mk, err := v.readVaultMarkers(folder)
			if err != nil {
				// need to add return err
				return entity.Vault{}
			}
			return makeVault(folder, mk)
		}

		granularFolderPath := filepath.Join(v.granularFolder, vaultName)
		if v.exists(granularFolderPath) {
			mk, err := v.readVaultMarkers(granularFolderPath)
			if err != nil {
				// need to add return err
				return entity.Vault{}
			}
			vault := makeVault(granularFolderPath, mk)
			vault.IsGranular = true
			return vault
		}

		return entity.Vault{}
	}

	if len(vaultPath) > 0 {
		externalFolder := filepath.Join(v.externalRoot, vaultPath, vaultName)
		if skipFSCheck || v.exists(externalFolder) {
			mk, err := v.readVaultMarkers(externalFolder)
			if err != nil {
				// need to add return err
				return entity.Vault{}
			}
			vault := makeVault(externalFolder, mk)
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
			return v.GetName(vault.Folder), nil
		}
	}
	return "", fmt.Errorf("%w in timestamp %s", ErrNoVaults, timestamp)
}

func (v *StorageRepo) OpenVault(vaultName string, allowEviction bool, isGranular bool, isSharded bool, isExternal bool, vaultPath string, backupPrefix string, blobPath string) (entity.Vault, error) {
	vault := v.GetVault(vaultName, isExternal, vaultPath, blobPath, false)
	if len(vault.Folder) > 0 {
		return vault, nil
	}
	folder := ""
	if blobPath != "" {
		folder = filepath.Join(v.root, S3_PROCESSING)
	} else if isGranular {
		folder = v.granularFolder
	} else {
		if !isExternal {
			folder = v.root
		} else {
			folder = filepath.Join(v.externalRoot, vaultPath)
		}
	}
	targetFolder := filepath.Join(folder, vaultName)
	if len(vaultName) == 0 {
		targetFolder = filepath.Join(folder, v.getVaultName(backupPrefix, isGranular))
	}

	result := entity.Vault{
		Folder:      targetFolder,
		TimeStamp:   v.createTime(v.basename(folder)),
		IsEvictable: allowEviction,
		IsSharded:   isSharded,
	}

	return result, v.InitVault(result)
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

func (v *StorageRepo) GetRoot() string {
	return v.root
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

	dirs := make([]string, 0, 10)
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
	vaults := make([]entity.Vault, 0, len(dirs))
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
		shardedVaults := make([]entity.Vault, 0, len(vaults))
		for _, vault := range vaults {
			if vault.IsSharded {
				shardedVaults = append(shardedVaults, vault)
			}
		}
		vaults = shardedVaults
	}
	if !v.skipLockCheck {
		lockedVaults := make([]entity.Vault, 0, len(vaults))
		for _, vault := range vaults {
			if !vault.IsLocked {
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
		if errors.Is(err, ErrNoVaults) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("error listing vaults: %w", err)
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

func (v *StorageRepo) InitVault(vault entity.Vault) error {
	if err := os.MkdirAll(vault.Folder, 0755); err != nil {
		return fmt.Errorf("failed to create vault dir %s: %w", vault.Folder, err)
	}

	if !vault.IsEvictable {
		evictLockPath := filepath.Join(vault.Folder, ".evictlock")
		if err := v.touchFile(evictLockPath); err != nil {
			return fmt.Errorf("failed to create .evictlock: %w", err)
		}
	}

	if vault.IsSharded {
		shardedPath := filepath.Join(vault.Folder, ".sharded")
		if err := v.touchFile(shardedPath); err != nil {
			return fmt.Errorf("failed to create .sharded: %w", err)
		}
	}

	return nil
}

func (v *StorageRepo) CloseVault(vault entity.Vault) error {
	lockPath := filepath.Join(vault.Folder, ".lock")
	if err := os.Remove(lockPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove .lock: %w", err)
	}
	return nil
}

func (v *StorageRepo) touchFile(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	return f.Close()
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
		if v.isNoneEvictable(vault.Folder) {
			vaults[vault.TimeStamp] = true
		}
	}
	return vaults, nil
}

// nolint
func (v *StorageRepo) isLocked(folder string) bool {
	return v.exists(filepath.Join(folder, ".lock"))
}

// nolint
func (v *StorageRepo) isSharded(folder string) bool {
	return v.exists(filepath.Join(folder, ".sharded"))
}

// nolint
func (v *StorageRepo) isNoneEvictable(folder string) bool {
	return v.exists(filepath.Join(folder, ".evictlock"))
}

// nolint
func (v *StorageRepo) isGranular(folder string) bool {
	return strings.Contains(folder, GRANULAR)
}

func (v *StorageRepo) readVaultMarkers(folder string) (vaultMarkers, error) {
	entries, err := os.ReadDir(folder)
	if err != nil {
		return vaultMarkers{}, err
	}

	var m vaultMarkers
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		switch entry.Name() {
		case ".lock":
			m.locked = true
		case ".evictlock":
			m.evictLock = true
		case ".sharded":
			m.sharded = true
		}
		if m.locked && m.evictLock && m.sharded {
			break
		}
	}
	return m, nil
}
