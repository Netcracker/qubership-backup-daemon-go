package repo

import (
	"encoding/json"
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
	List(typeOfBackup string, storagePath string) ([]entity.Vault, error)
	ListVaultNames(convertToTs bool, typeOfBackup string, storagePath string) ([]string, error)
	GetNonEvictableVaults(typeOfBackup string) (map[int64]bool, error)
	GetName(folder string) string
	CloseVault(vault entity.Vault) error
	GetRoot() string
	GetFSType() string
	HasCustomVars(v entity.Vault) bool
	LoadCustomVariables(v entity.Vault) interface{}
	LoadMetrics(v entity.Vault) (map[string]interface{}, error)
	Health() error
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
	fs                  FileSystem
}

func NewStorageRepo(root string, externalRoot string, namespace string, allowPrefix bool) StorageRepository {
	return NewStorageRepoWithFS(root, externalRoot, namespace, allowPrefix, NewLocalFileSystem())
}

func NewStorageRepoWithFS(root string, externalRoot string, namespace string, allowPrefix bool, fs FileSystem) StorageRepository {
	repo := &StorageRepo{
		root:                root,
		granularFolder:      filepath.Join(root, GRANULAR),
		externalRoot:        externalRoot,
		namespace:           namespace,
		restoreLogsFolder:   filepath.Join(root, "restore_logs"),
		allowPrefix:         allowPrefix,
		vaultDirnameMatcher: regexp.MustCompile(`(?i)\d{8}T\d{4,6}`),
		skipLockCheck:       strings.ToLower(os.Getenv("SKIP_LOCK_CHECK")) == "true",
		fs:                  fs,
	}
	// Ensure required subdirectories exist (no-op for S3).
	_ = fs.MkdirAll(repo.granularFolder)
	_ = fs.MkdirAll(repo.restoreLogsFolder)
	return repo
}

func (v *StorageRepo) GetFSType() string {
	return v.fs.GetType()
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

			if skipFSCheck || v.fs.Exists(folder) {
				mk := v.readVaultMarkers(folder)
				return makeVault(folder, mk)
			}
			return entity.Vault{}
		}

		folder := filepath.Join(v.root, vaultName)
		if skipFSCheck || v.fs.Exists(folder) {
			mk := v.readVaultMarkers(folder)
			return makeVault(folder, mk)
		}

		granularFolderPath := filepath.Join(v.granularFolder, vaultName)
		if v.fs.Exists(granularFolderPath) {
			mk := v.readVaultMarkers(granularFolderPath)
			vault := makeVault(granularFolderPath, mk)
			vault.IsGranular = true
			return vault
		}

		return entity.Vault{}
	}

	if len(vaultPath) > 0 {
		externalFolder := filepath.Join(v.externalRoot, vaultPath, vaultName)
		if skipFSCheck || v.fs.Exists(externalFolder) {
			mk := v.readVaultMarkers(externalFolder)
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
		TimeStamp:   v.createTime(v.basename(targetFolder)),
		IsEvictable: allowEviction,
		IsSharded:   isSharded,
	}

	return result, v.InitVault(result)
}

func (v *StorageRepo) Evict(vaultName string) error {
	return v.fs.RemoveAll(vaultName)
}

func (v *StorageRepo) createTime(folderName string) int64 {
	dateStr := folderName
	if idx := strings.LastIndex(folderName, "_"); idx >= 0 {
		dateStr = folderName[idx+1:]
	}
	if idx := strings.LastIndex(dateStr, "."); idx >= 0 {
		dateStr = dateStr[:idx]
	}
	// Parse in local timezone: vault names are formatted with time.Now() (local),
	// so UTC parsing would produce wrong timestamps on non-UTC hosts.
	t, err := time.ParseInLocation(VaultNameFormat, dateStr, time.Local)
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

func (v *StorageRepo) List(typeOfBackup string, storagePath string) ([]entity.Vault, error) {
	storageRootPath := v.root
	if len(storagePath) > 0 {
		storageRootPath = filepath.Join(v.externalRoot, storagePath)
	}

	if !v.fs.Exists(storageRootPath) {
		return []entity.Vault{}, ErrNoVaults
	}
	type dirEntry struct {
		name       string
		isGranular bool
	}

	var dirs []dirEntry

	if typeOfBackup == GRANULAR || typeOfBackup == ALL {
		files, err := v.fs.ListDir(filepath.Join(storageRootPath, GRANULAR))
		if err == nil {
			dirs = make([]dirEntry, 0, len(files))
			for _, name := range files {
				dirs = append(dirs, dirEntry{name: name, isGranular: true})
			}
		}
	}
	if typeOfBackup == FULL || typeOfBackup == ALL {
		files, err := v.fs.ListDir(storageRootPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read dir %s: %v", storageRootPath, err)
		}
		if dirs == nil {
			dirs = make([]dirEntry, 0, len(files))
		}
		for _, name := range files {
			dirs = append(dirs, dirEntry{name, false})
		}

	}
	vaults := make([]entity.Vault, 0, len(dirs))
	for _, dir := range dirs {
		lastPart := dir.name
		if idx := strings.LastIndex(dir.name, "_"); idx >= 0 {
			lastPart = dir.name[idx+1:]
		}
		if !v.vaultDirnameMatcher.MatchString(lastPart) {
			continue
		}

		vault := v.GetVault(dir.name, false, storageRootPath, "", true)
		if vault.Folder == "" {
			continue
		}
		if !v.skipLockCheck && vault.IsLocked {
			continue
		}

		if typeOfBackup == SHARDED && !vault.IsSharded {
			continue
		}
		vaults = append(vaults, vault)
	}
	if len(vaults) > 0 {
		sort.Slice(vaults, func(i, j int) bool {
			return vaults[i].TimeStamp < vaults[j].TimeStamp
		})
	}

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
	if err := v.fs.MkdirAll(vault.Folder); err != nil {
		return fmt.Errorf("failed to create vault dir %s: %w", vault.Folder, err)
	}

	if !vault.IsEvictable {
		evictLockPath := filepath.Join(vault.Folder, ".evictlock")
		if err := v.fs.TouchFile(evictLockPath); err != nil {
			return fmt.Errorf("failed to create .evictlock: %w", err)
		}
	}

	if vault.IsSharded {
		shardedPath := filepath.Join(vault.Folder, ".sharded")
		if err := v.fs.TouchFile(shardedPath); err != nil {
			return fmt.Errorf("failed to create .sharded: %w", err)
		}
	}

	return nil
}

func (v *StorageRepo) CloseVault(vault entity.Vault) error {
	lockPath := filepath.Join(vault.Folder, ".lock")
	if err := v.fs.Remove(lockPath); err != nil {
		return fmt.Errorf("failed to remove .lock: %w", err)
	}
	return nil
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

func (v *StorageRepo) GetNonEvictableVaults(typeOfBackup string) (map[int64]bool, error) {
	listVaults, err := v.List(typeOfBackup, "")
	if err != nil {
		if errors.Is(err, ErrNoVaults) {
			return map[int64]bool{}, nil
		}
		return nil, fmt.Errorf("error listing vaults: %v", err)
	}
	vaults := make(map[int64]bool, len(listVaults))
	for _, vault := range listVaults {
		if !vault.IsEvictable {
			vaults[vault.TimeStamp] = true
		}
	}
	return vaults, nil
}

// nolint
func (v *StorageRepo) isGranular(folder string) bool {
	return strings.Contains(folder, GRANULAR)
}

func (v *StorageRepo) HasCustomVars(vault entity.Vault) bool {
	if vault.CustomVarsFilePath == "" {
		return false
	}
	return v.fs.ExistsFile(vault.CustomVarsFilePath)
}

func (v *StorageRepo) LoadCustomVariables(vault entity.Vault) interface{} {
	data, err := v.fs.ReadFile(vault.CustomVarsFilePath)
	if err != nil {
		return map[string]interface{}{}
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return string(data)
	}
	return parsed
}

func (v *StorageRepo) LoadMetrics(vault entity.Vault) (map[string]interface{}, error) {
	metrics := make(map[string]interface{})
	if vault.MetricsFilePath == "" {
		return metrics, fmt.Errorf("metrics file not present")
	}
	data, err := v.fs.ReadFile(vault.MetricsFilePath)
	if err != nil {
		return metrics, fmt.Errorf("failed to read metrics file %s: %v", vault.MetricsFilePath, err)
	}
	if err = json.Unmarshal(data, &metrics); err != nil {
		return make(map[string]interface{}), fmt.Errorf("failed to unmarshal metrics file %s: %v", vault.MetricsFilePath, err)
	}
	return metrics, nil
}

// Health performs a lightweight accessibility check.
func (v *StorageRepo) Health() error {
	return v.fs.Health(v.root)
}

// readVaultMarkers reads the vault directory to detect marker files.// For local FS it reads the directory; for S3 it checks individual object keys.
func (v *StorageRepo) readVaultMarkers(folder string) vaultMarkers {
	var m vaultMarkers
	if v.fs.GetType() == "s3" {
		m.locked = v.fs.ExistsFile(filepath.Join(folder, ".lock"))
		m.evictLock = v.fs.ExistsFile(filepath.Join(folder, ".evictlock"))
		m.sharded = v.fs.ExistsFile(filepath.Join(folder, ".sharded"))
		return m
	}
	// Local filesystem: read directory entries once.
	entries, err := v.fs.ListDir(folder)
	if err != nil {
		return m
	}
	for _, name := range entries {
		switch name {
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
	return m
}
