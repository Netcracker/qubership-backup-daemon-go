package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/tasks"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/utils"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/repo"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const INCREMENTAL = "incremental"
const FULL = "full"
const GRANULAR = "granular"
const COMMONBACKUP = "backup"
const INCREMENTALBACKUP = "incremental backup"
const COMMONRESTORE = "restore"
const INCREMENTALRESTORE = "incremental restore"

var (
	ErrVaultNotFound    = errors.New("vault not found")
	ErrVaultLocked      = errors.New("vault locked")
	ErrBackupNotRunning = errors.New("backup not running")
	ErrIllegalState     = errors.New("illegal state")
)

//go:generate mockgen -source=backup-daemon.go -destination=../rest/mock.go -package=rest
type BackupDaemonUseCase interface {
	EnqueueBackup(ctx context.Context, request entity.BackupRequest) (entity.BackupResponse, error)
	RestoreBackup(ctx context.Context, request entity.RestoreRequest) (entity.RestoreResponse, error)
	EnqueueEviction(ctx context.Context, request entity.EvictRequest) error
	RemoveBackup(ctx context.Context, request entity.EvictByVaultRequest) error
	RemoveBackupV2(ctx context.Context, request entity.EvictByVaultV2Request) error
	RemoveRestoreV2(ctx context.Context, request entity.EvictByVaultV2Request) error
	GetJobStatus(ctx context.Context, request entity.JobStatusRequest) (entity.JobStatusResponse, error)
	CreateS3PresignedURL(ctx context.Context, request entity.S3PresignedURLRequest) (entity.S3PresignedURLResponse, error)
	ListBackups(ctx context.Context, procType string) ([]string, error)
	GetBackupStats(ctx context.Context, vaultName string, ts string, backupPath string, procType string) (result map[string]interface{}, err error)
	ListBackup(ctx context.Context, procType string, vaultPath string) (result map[string]interface{}, err error)
	GetHealth(ctx context.Context, procType string) (entity.HealthResponse, error)
	Find(ctx context.Context, request entity.FindRequest) (map[string]interface{}, error)
	UpdateEvictionPolicy(ctx context.Context, request entity.EvictionPolicyRequest) error
	TerminateBackup(ctx context.Context, request entity.TerminateRequest) error
	GetQueueSize() int
	DownloadBackup(ctx context.Context, backupID string) (string, error)
}
type BackupDaemon struct {
	storageRepo            repo.StorageRepository
	dbRepo                 repo.DBRepository
	taskPool               tasks.TaskPoolRepository
	s3Client               utils.S3ClientRepository
	executor               tasks.CommandExecutor
	s3Enable               bool
	logger                 *zap.SugaredLogger
	evictionPolicy         string
	granularEvictionPolicy string
}

func NewBackupDaemon(storageRepo repo.StorageRepository, dbRepo repo.DBRepository,
	taskPool tasks.TaskPoolRepository, s3Client utils.S3ClientRepository, executor tasks.CommandExecutor,
	s3Enable bool, logger *zap.SugaredLogger, evictionPolicy string, granularEvictionPolicy string) BackupDaemonUseCase {
	return &BackupDaemon{
		storageRepo:            storageRepo,
		dbRepo:                 dbRepo,
		taskPool:               taskPool,
		s3Client:               s3Client,
		executor:               executor,
		s3Enable:               s3Enable,
		logger:                 logger,
		evictionPolicy:         evictionPolicy,
		granularEvictionPolicy: granularEvictionPolicy,
	}
}

func (b *BackupDaemon) ListBackups(ctx context.Context, procType string) (backups []string, err error) {
	return b.storageRepo.ListVaultNames(false, repo.ALL, "")
}

func (b *BackupDaemon) ListBackup(ctx context.Context, procType string, vaultPath string) (result map[string]interface{}, err error) {
	return b.GetBackupStats(ctx, vaultPath, "", "", procType)
}

func LoadMetrics(v entity.Vault) (map[string]interface{}, error) {
	metrics := make(map[string]interface{})
	if v.MetricsFilePath == "" {
		return metrics, fmt.Errorf("metrics file not present")
	}

	data, err := os.ReadFile(v.MetricsFilePath)
	if err != nil {
		return metrics, fmt.Errorf("failed to read metrics file %s: %v", v.MetricsFilePath, err)
	}

	if err := json.Unmarshal(data, &metrics); err != nil {
		return make(map[string]interface{}), fmt.Errorf("failed to unmarshal metrics file %s: %v", v.MetricsFilePath, err)
	}

	return metrics, nil
}

func (b *BackupDaemon) GetBackupStats(ctx context.Context, vaultName string, ts string, backupPath string, procType string) (result map[string]interface{}, err error) {
	result = make(map[string]interface{})
	name := vaultName
	backupType := "all"

	if backupPath != "" {
		backupType = "full"
	}

	// Determine vault name if not provided
	if name != "" {
		listed, err := b.storageRepo.ListVaultNames(false, backupType, backupPath)
		if err != nil {
			return result, fmt.Errorf("failed to list backups: %v", err)
		}

		found := false
		for _, v := range listed {
			if v == name {
				found = true
				break
			}
		}

		if !found {
			return result, fmt.Errorf("backup %s not found", name)
		}

	} else if ts != "" {
		var err error
		name, err = b.storageRepo.FindByTS(ts, backupType, backupPath)
		if err != nil || name == "" {
			return result, fmt.Errorf("backup with ts %s or newer not found", ts)
		}

	} else {
		return result, fmt.Errorf("backup name or ts not found")
	}

	vaultObj := b.storageRepo.GetVault(name, backupPath != "", backupPath, "", false)
	var metricErr error
	vaultObj.Metrics, metricErr = LoadMetrics(vaultObj)
	if metricErr != nil {
		b.logger.Debugf(metricErr.Error())
	}

	result["is_granular"] = vaultObj.IsGranular

	if vaultObj.IsGranular {
		dbList, err := b.executor.GetBackupDBs(vaultObj.Folder)
		if err != nil {
			return result, fmt.Errorf("failed to get backup DBs: %v", err)
		}
		result["db_list"] = dbList
	} else {
		result["db_list"] = fmt.Sprintf("%s backup", procType)
	}

	result["id"] = b.storageRepo.GetName(vaultObj.Folder)
	result["failed"] = vaultObj.IsFailed
	result["locked"] = vaultObj.IsLocked
	result["sharded"] = vaultObj.IsSharded
	result["canceled"] = vaultObj.Canceled
	result["ts"] = vaultObj.TimeStamp

	for k, v := range vaultObj.Metrics {
		result[k] = v
	}

	if s, ok := result["size"]; ok {
		result["size"] = fmt.Sprintf("%vb", s)
	} else {
		result["size"] = "Unknown"
	}

	if t, ok := result["spent_time"]; ok {
		result["spent_time"] = fmt.Sprintf("%vms", t)
	} else {
		result["spent_time"] = "Unknown"
	}

	if _, ok := result["exit_code"]; !ok {
		result["exit_code"] = 0
	}

	failed, _ := result["failed"].(bool)
	locked, _ := result["locked"].(bool)
	_, hasException := result["exception"]

	result["valid"] = !failed && !locked && !hasException
	result["evictable"] = vaultObj.IsEvictable

	if HasCustomVars(vaultObj) {
		result["custom_vars"] = LoadCustomVariables(vaultObj)
	}

	b.logger.Debugf("Backup stats for backup %s: %+v", name, result)

	return result, nil
}

func HasCustomVars(v entity.Vault) bool {
	if v.CustomVarsFilePath == "" {
		return false
	}

	_, err := os.Stat(v.CustomVarsFilePath)
	return err == nil
}

func LoadCustomVariables(v entity.Vault) interface{} {
	data, err := os.ReadFile(v.CustomVarsFilePath)
	if err != nil {
		return map[string]interface{}{}
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return string(data)
	}
	return parsed
}

// TODO: worker pool, add task
func (b *BackupDaemon) EnqueueBackup(ctx context.Context, request entity.BackupRequest) (entity.BackupResponse, error) {
	dirType := repo.FULL
	if len(request.DBs) > 0 && len(request.ExternalBackupPath) == 0 {
		dirType = repo.GRANULAR
	}
	var commonTS []string
	var err error
	if request.ProcType == INCREMENTAL {
		if request.CustomVars == nil {
			request.CustomVars = make(map[string]string)
		}

		if len(request.ExternalBackupPath) == 0 {
			commonTS, err = b.storageRepo.ListVaultNames(true, repo.ALL, "")
			if err != nil {
				return entity.BackupResponse{}, fmt.Errorf("failed to list all backup err: %w", err)
			}
		} else {
			commonTS, err = b.storageRepo.ListVaultNames(false, dirType, "")
			if err != nil {
				return entity.BackupResponse{}, fmt.Errorf("failed to list %s backup err: %w", dirType, err)
			}
		}
	}

	// TODO change it can be done in sql
	sort.Slice(commonTS, func(i, j int) bool {
		return commonTS[i] > commonTS[j]
	})

	if request.CustomVars == nil {
		request.CustomVars = make(map[string]string)
	}
	if request.ProcType == INCREMENTAL && len(commonTS) == 0 {
		return entity.BackupResponse{}, fmt.Errorf(
			"existing backups not found, previous full or incremental backup must exist before doing incremental backup: %w",
			ErrIllegalState,
		)
	}
	if len(commonTS) > 0 {
		request.CustomVars["start_ts"] = commonTS[0]
	}
	var isGranular bool
	if len(request.DBs) > 0 {
		isGranular = true
	}
	action := getBackupAction(request.ProcType)
	var isExternal bool
	if len(request.ExternalBackupPath) > 0 {
		isExternal = true
	}
	blobPath := strings.TrimLeft(strings.TrimSpace(request.CustomVars["blob_path"]), "/")

	allowEviction := true
	if request.AllowEviction != "" {
		allowEviction, err = strconv.ParseBool(request.AllowEviction)
		if err != nil {
			return entity.BackupResponse{}, fmt.Errorf("failed to parse allow eviction err: %w", err)
		}
	}
	var vault entity.Vault

	if blobPath != "" {
		vault, err = b.storageRepo.OpenVault("", allowEviction, isGranular, request.Sharded, false, "", request.Prefix, blobPath)
	} else {
		vault, err = b.storageRepo.OpenVault(request.ExternalBackupPath, allowEviction, isGranular, request.Sharded, isExternal, request.ExternalBackupPath, request.Prefix, "")
	}

	if err != nil {
		return entity.BackupResponse{}, err
	}
	defer func(storageRepo repo.StorageRepository, vault entity.Vault) {
		err = storageRepo.CloseVault(vault)
		if err != nil {
			b.logger.Errorf("failed to close vault: %v", err)
		}
	}(b.storageRepo, vault)

	backupID := filepath.Base(vault.Folder)
	dbNames := make([]string, 0, len(request.DBs))
	for _, d := range request.DBs {
		if d.SimpleName != "" {
			dbNames = append(dbNames, d.SimpleName)
		}
	}
	dbsJSON, _ := json.Marshal(dbNames)

	creationTime := GetTimeCreationNow()
	job := entity.Job{TaskID: backupID, Type: action, Status: "Queued", Vault: backupID, Err: "", StorageName: request.CustomVars["storageName"], BlobPath: request.CustomVars["blob_path"], Databases: string(dbsJSON), CreationTime: creationTime}

	if err = b.dbRepo.UpdateJob(ctx, job); err != nil {
		return entity.BackupResponse{}, fmt.Errorf("failed to update job err: %w", err)
	}

	task := tasks.Task{
		Type:       "backup",
		ProcType:   request.ProcType,
		Vault:      vault,
		DBs:        request.DBs,
		CustomVars: request.CustomVars,
		Job:        job,
	}
	b.taskPool.EnqueueTask(task)

	return entity.BackupResponse{
		BackupID:     backupID,
		CreationTime: creationTime,
	}, nil
}

func (b *BackupDaemon) RestoreBackup(ctx context.Context, request entity.RestoreRequest) (entity.RestoreResponse, error) {
	if request.CustomVars == nil {
		request.CustomVars = make(map[string]string)
	}
	action := getRestoreAction(request.ProcType)
	taskID := uuid.New().String()
	dbNames := make([]string, 0, len(request.DBs))
	for _, d := range request.DBs {
		if d.SimpleName != "" {
			if newName, ok := request.ChangeDbNames[d.SimpleName]; ok {
				dbNames = append(dbNames, newName)
			} else {
				dbNames = append(dbNames, d.SimpleName)
			}
		}
	}
	dbsJSON, _ := json.Marshal(dbNames)

	storageName := request.CustomVars["storageName"]
	blobPath := request.CustomVars["blob_path"]
	dryRun := request.CustomVars["dryRun"] == "true"

	var restoreDBsJSON []byte
	if len(request.RestoreDBMaps) > 0 {
		var err error
		restoreDBsJSON, err = json.Marshal(request.RestoreDBMaps)
		if err != nil {
			b.logger.Errorf("Failed to marshal RestoreDBMaps for restore request: %v", err)
			return entity.RestoreResponse{}, fmt.Errorf("failed to marshal RestoreDBMaps: %w", err)
		}
	}

	creationTime := GetTimeCreationNow()

	if !dryRun {
		err := b.dbRepo.UpdateJob(ctx, entity.Job{
			TaskID:           taskID,
			Type:             action,
			Status:           "Queued",
			Vault:            "",
			Err:              "",
			StorageName:      storageName,
			BlobPath:         blobPath,
			Databases:        string(dbsJSON),
			CreationTime:     creationTime,
			RestoreDatabases: string(restoreDBsJSON),
		})
		if err != nil {
			return entity.RestoreResponse{}, fmt.Errorf("failed to update job err: %w", err)
		}
	} else {
		b.logger.Info("Dry run mode, skipping database update")
	}

	var external bool
	if len(request.ExternalBackupPath) > 0 {
		external = true
	}
	var vault entity.Vault
	if len(request.Vault) > 0 {
		vault = b.storageRepo.GetVault(request.Vault, external, request.ExternalBackupPath, "", false)
	} else {
		vaultName, err := b.storageRepo.FindByTS(request.TimeStamp, repo.ALL, "")
		if err != nil {
			return entity.RestoreResponse{}, fmt.Errorf("failed to find backup by ts %s err: %w", request.TimeStamp, err)
		}
		vault = b.storageRepo.GetVault(vaultName, external, request.ExternalBackupPath, "", false)
	}

	b.logger.Infof("Starting process from: %s, %s", request.ExternalBackupPath, vault.Folder)

	var vaultFolder string

	if blobPath != "" {
		s3Prefix := path.Join(blobPath, request.Vault)

		vaultFolder = filepath.Join(os.TempDir(), "backup-daemon", "restore", request.Vault)

		_ = os.RemoveAll(vaultFolder)
		if err := os.MkdirAll(vaultFolder, 0o755); err != nil {
			return entity.RestoreResponse{}, fmt.Errorf("failed to create restore dir %s: %w", vaultFolder, err)
		}

		if err := b.s3Client.DownloadFolder(ctx, s3Prefix, vaultFolder); err != nil {
			return entity.RestoreResponse{}, fmt.Errorf("failed to download backup from s3 prefix=%s err: %w", s3Prefix, err)
		}

		// Check if downloaded folder is empty (backup was deleted from S3)
		entries, err := os.ReadDir(vaultFolder)
		if err != nil || len(entries) == 0 {
			return entity.RestoreResponse{}, fmt.Errorf("backup %s not found in s3 at prefix %s: %w", request.Vault, s3Prefix, ErrVaultNotFound)
		}
	} else {
		var vault entity.Vault
		external := len(request.ExternalBackupPath) > 0

		if len(request.Vault) > 0 {
			vault = b.storageRepo.GetVault(request.Vault, external, request.ExternalBackupPath, "", false)
		} else {
			vaultName, err := b.storageRepo.FindByTS(request.TimeStamp, repo.ALL, "")
			if err != nil {
				return entity.RestoreResponse{}, fmt.Errorf("failed to find backup by ts %s err: %w", request.TimeStamp, err)
			}
			vault = b.storageRepo.GetVault(vaultName, external, request.ExternalBackupPath, "", false)
		}

		if reflect.DeepEqual(vault, entity.Vault{}) {
			return entity.RestoreResponse{}, fmt.Errorf("backup %s not found in storage: %w", request.Vault, ErrVaultNotFound)
		}

		vaultFolder = vault.Folder

		if b.s3Enable {
			if err := b.s3Client.DownloadFolder(ctx, vaultFolder, ""); err != nil {
				return entity.RestoreResponse{}, fmt.Errorf("failed to download backup err: %w", err)
			}
		}
	}

	if dryRun {
		b.logger.Info("Dry executed successfully")
		return entity.RestoreResponse{
			TaskID:       taskID,
			CreationTime: creationTime,
		}, nil
	}

	err := b.dbRepo.UpdateJob(ctx, entity.Job{
		TaskID:           taskID,
		Type:             action,
		Status:           "Queued",
		Vault:            filepath.Base(request.Vault),
		Err:              "",
		StorageName:      storageName,
		BlobPath:         blobPath,
		Databases:        string(dbsJSON),
		RestoreDatabases: string(restoreDBsJSON),
	})
	if err != nil {
		return entity.RestoreResponse{}, fmt.Errorf("failed to update job err: %w", err)
	}

	task := tasks.Task{
		Type:       "restore",
		ProcType:   request.ProcType,
		Vault:      vault,
		DBs:        request.DBs,
		DBMap:      request.ChangeDbNames,
		CustomVars: request.CustomVars,
		External:   external,
		Job: entity.Job{
			TaskID:           taskID,
			Type:             action,
			Status:           "Queued",
			Vault:            filepath.Base(request.Vault),
			Err:              "",
			StorageName:      storageName,
			BlobPath:         blobPath,
			Databases:        string(dbsJSON),
			RestoreDatabases: string(restoreDBsJSON),
		},
	}
	b.taskPool.EnqueueTask(task)

	return entity.RestoreResponse{
		TaskID:       taskID,
		CreationTime: creationTime,
	}, nil
}

func (b *BackupDaemon) EnqueueEviction(ctx context.Context, request entity.EvictRequest) error {
	excludedFiles, err := b.storageRepo.GetNonEvictableVaults(repo.ALL)
	if err != nil {
		return fmt.Errorf("failed to list all non evictable vaults err: %w", err)
	}

	fullVaults, err := b.storageRepo.List(repo.FULL, "")
	if err != nil {
		return fmt.Errorf("failed to list full vaults err: %w", err)
	}

	var obsoleteVaults []entity.Vault

	if b.evictionPolicy != "" {
		obsoleteFullVaults, err := b.evict(fullVaults, b.evictionPolicy, excludedFiles)
		if err != nil {
			return fmt.Errorf("failed to list evict full vaults err: %w", err)
		}
		obsoleteVaults = append(obsoleteVaults, obsoleteFullVaults...)
	}

	if b.granularEvictionPolicy != "" {
		granularVaults, err := b.storageRepo.List(repo.GRANULAR, "")
		if err != nil {
			return fmt.Errorf("failed to list granular vaults err: %w", err)
		}

		obsoleteGranularVaults, err := b.evict(granularVaults, b.granularEvictionPolicy, excludedFiles)
		if err != nil {
			return fmt.Errorf("failed to list evict granular vaults err: %w", err)
		}
		obsoleteVaults = append(obsoleteVaults, obsoleteGranularVaults...)
	}
	for _, obsoleteVault := range obsoleteVaults {
		err = b.storageRepo.Evict(obsoleteVault.Folder)
		if err != nil {
			return fmt.Errorf("failed to evict backup %s from storage err: %w", obsoleteVault.Folder, err)
		}
		err = b.dbRepo.RemoveVault(ctx, b.storageRepo.GetName(obsoleteVault.Folder))
		if err != nil && !errors.Is(err, repo.ErrNoVaults) {
			return fmt.Errorf("failed to remove backup %s from database err: %w", obsoleteVault.Folder, err)
		}
		err = b.executor.ExecuteEvictCmd(obsoleteVault.Folder)
		if err != nil {
			return fmt.Errorf("failed to evict backup from executor err: %w", err)
		}
	}
	return nil
}

func (b *BackupDaemon) RemoveBackup(ctx context.Context, request entity.EvictByVaultRequest) error {
	vaultObject := b.storageRepo.GetVault(request.Vault, false, "", "", false)
	if reflect.DeepEqual(vaultObject, entity.Vault{}) {
		return fmt.Errorf("backup vault %s not found in storage: %w", request.Vault, ErrVaultNotFound)
	}
	if vaultObject.IsLocked {
		return fmt.Errorf("backup vault %s is locked: %w", request.Vault, ErrVaultLocked)
	}
	job, err := b.dbRepo.SelectEverything(ctx, request.Vault)
	if err != nil && !errors.Is(err, repo.ErrNotFound) {
		return fmt.Errorf("failed to read backup metadata err: %w", err)
	}
	s3Prefix := ""
	if strings.TrimSpace(job.BlobPath) != "" {
		s3Prefix = path.Join(job.BlobPath, request.Vault)
	} else if b.s3Enable {
		// In legacy S3 mode (without blob_path), upload uses the vault folder path as key prefix.
		s3Prefix = strings.TrimLeft(filepath.ToSlash(vaultObject.Folder), "/")
	}
	if s3Prefix != "" {
		if err = b.s3Client.DeletePrefix(ctx, s3Prefix); err != nil {
			return fmt.Errorf("failed to delete backup from s3 prefix=%s err: %w", s3Prefix, err)
		}
	}
	if err := b.executor.ExecuteEvictCmd(vaultObject.Folder); err != nil {
		return fmt.Errorf("failed to evict backup from executor err: %w", err)
	}
	if err := b.storageRepo.Evict(vaultObject.Folder); err != nil {
		return fmt.Errorf("failed to evict backup err: %w", err)
	}
	if err := b.dbRepo.RemoveVault(ctx, request.Vault); err != nil && !errors.Is(err, repo.ErrNoVaults) {
		return fmt.Errorf("failed to remove backup from database err: %w", err)
	}

	return nil
}

func (b *BackupDaemon) RemoveBackupV2(ctx context.Context, request entity.EvictByVaultV2Request) error {
	backupID := strings.TrimSpace(request.Vault)
	if backupID == "" {
		return fmt.Errorf("vault is required")
	}

	job, err := b.dbRepo.SelectEverything(ctx, backupID)
	if err != nil {
		return err
	}

	blob := request.BlobPath
	if blob == "" {
		blob = job.BlobPath
	}

	if blob != "" {
		prefix := path.Join(blob, backupID)
		if err := b.s3Client.DeletePrefix(ctx, prefix); err != nil {
			return fmt.Errorf("failed to delete from s3 prefix=%s: %w", prefix, err)
		}
	}

	vaultObj := b.storageRepo.GetVault(backupID, false, "", blob, false)
	if !reflect.DeepEqual(vaultObj, entity.Vault{}) {
		if vaultObj.IsLocked {
			return fmt.Errorf("backup vault %s is locked", backupID)
		}
		if err := b.storageRepo.Evict(vaultObj.Folder); err != nil {
			return fmt.Errorf("failed to evict backup %s from storage: %w", vaultObj.Folder, err)
		}
		if err := b.executor.ExecuteEvictCmd(vaultObj.Folder); err != nil {
			return fmt.Errorf("failed to evict backup from executor: %w", err)
		}
	}

	if err = b.dbRepo.RemoveVault(ctx, backupID); err != nil && !errors.Is(err, repo.ErrNoVaults) {
		return fmt.Errorf("failed to remove backup %s from database: %w", backupID, err)
	}

	return nil
}

func (b BackupDaemon) RemoveRestoreV2(ctx context.Context, request entity.EvictByVaultV2Request) error {
	backupID := strings.TrimSpace(request.Vault)
	if backupID == "" {
		return fmt.Errorf("vault is required")
	}

	job, err := b.dbRepo.SelectEverything(ctx, request.TaskID)
	if err != nil {
		return err
	}

	blob := request.BlobPath
	if blob == "" {
		blob = job.BlobPath
	}

	if blob != "" {
		prefix := path.Join(blob, backupID, "restore_logs", request.TaskID)
		if err := b.s3Client.DeletePrefix(ctx, prefix); err != nil {
			return fmt.Errorf("failed to delete from s3 prefix=%s: %w", prefix, err)
		}
	}

	vaultObj := b.storageRepo.GetVault(backupID, false, "", blob, false)
	filePath := filepath.Join(vaultObj.Folder, "restore_logs", request.TaskID)
	if !reflect.DeepEqual(vaultObj, entity.Vault{}) {
		if vaultObj.IsLocked {
			return fmt.Errorf("backup vault %s is locked", backupID)
		}
		if err := b.storageRepo.Evict(filePath); err != nil {
			return fmt.Errorf("failed to evict restore logs %s from storage: %w", filePath, err)
		}
		if err := b.executor.ExecuteEvictCmd(filePath); err != nil {
			return fmt.Errorf("failed to evict restore logs from executor: %w", err)
		}
	}

	if err := b.dbRepo.RemoveJob(ctx, request.TaskID); err != nil {
		return fmt.Errorf("failed to remove restore %s from database: %w", request.TaskID, err)
	}

	return nil
}

func (b *BackupDaemon) GetJobStatus(ctx context.Context, request entity.JobStatusRequest) (entity.JobStatusResponse, error) {
	job, err := b.dbRepo.SelectEverything(ctx, request.TaskID)
	if err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			return entity.JobStatusResponse{
				StatusCode: http.StatusNotFound,
			}, nil
		}
		return entity.JobStatusResponse{}, fmt.Errorf("failed to select job err: %w", err)
	}
	var dbs []string
	if strings.TrimSpace(job.Databases) != "" {
		_ = json.Unmarshal([]byte(job.Databases), &dbs)
	}
	var restoreDBs []entity.RestoreDBMap
	if strings.TrimSpace(job.RestoreDatabases) != "" {
		if err := json.Unmarshal([]byte(job.RestoreDatabases), &restoreDBs); err != nil {
			b.logger.Errorf("Failed to unmarshal RestoreDatabases for job %s: %v", job.TaskID, err)
			return entity.JobStatusResponse{}, fmt.Errorf("failed to unmarshal RestoreDatabases: %w", err)
		}
	}
	response := entity.JobStatusResponse{
		TaskID:           job.TaskID,
		Status:           job.Status,
		Vault:            job.Vault,
		Error:            job.Err,
		Type:             job.Type,
		StorageName:      job.StorageName,
		BlobPath:         job.BlobPath,
		Databases:        dbs,
		CreationTime:     job.CreationTime,
		CompletionTime:   job.CompletionTime,
		RestoreDatabases: restoreDBs,
	}
	switch job.Status {
	case "Successful":
		response.StatusCode = http.StatusOK
	case "Failed":
		response.StatusCode = http.StatusInternalServerError
	default:
		response.StatusCode = http.StatusPartialContent
	}
	return response, nil
}

func (b *BackupDaemon) CreateS3PresignedURL(ctx context.Context, request entity.S3PresignedURLRequest) (entity.S3PresignedURLResponse, error) {
	vault := b.storageRepo.GetVault(request.BackupID, false, "", "", false)
	if reflect.DeepEqual(vault, entity.Vault{}) {
		return entity.S3PresignedURLResponse{}, fmt.Errorf("backup vault %s not found in storage", request.BackupID)
	}
	extensions := []string{".zip", ".tar", ".gz"}
	files, err := b.s3Client.ListFiles(ctx, vault.Folder)
	if err != nil {
		return entity.S3PresignedURLResponse{}, fmt.Errorf("failed to list files from s3 err: %w", err)
	}
	var urls []string
	for _, file := range files {
		for _, extension := range extensions {
			if strings.HasSuffix(file, extension) {
				url, err := b.s3Client.CreatePresignedUrl(ctx, file, request.Expiration)
				if err != nil {
					return entity.S3PresignedURLResponse{}, fmt.Errorf("failed to create presigned url err: %w", err)
				}
				urls = append(urls, url)
				break
			}
		}
	}
	return entity.S3PresignedURLResponse{Urls: urls}, nil
}

func (b *BackupDaemon) Find(ctx context.Context, request entity.FindRequest) (map[string]interface{}, error) {
	return b.GetBackupStats(ctx, "", request.TimeStamp, "", request.ProcType)
}

func (b *BackupDaemon) GetHealth(ctx context.Context, procType string) (entity.HealthResponse, error) {
	resp := entity.HealthResponse{
		Status:          "UP",
		BackupQueueSize: b.GetQueueSize(),
	}

	vaults, err := b.storageRepo.List(repo.ALL, "")
	if err != nil {
		return resp, nil
	}

	info := entity.StorageInfo{
		DumpCount: len(vaults),
	}

	if !b.s3Enable {
		storageRoot := b.storageRepo.(*repo.StorageRepo).GetRoot()
		info.TotalSpace, info.FreeSpace, info.Size, info.TotalInodes, info.FreeInodes, info.UsedInodes = utils.GetDiskUsage(storageRoot)
	}

	sort.Slice(vaults, func(i, j int) bool {
		return vaults[i].TimeStamp > vaults[j].TimeStamp
	})

	if len(vaults) > 0 {
		last := vaults[0]
		metrics, _ := LoadMetrics(last)
		exitCode, _ := metrics["exit_code"].(float64)
		spentTime, _ := metrics["spent_time"].(float64)
		size, _ := metrics["size"].(float64)
		info.Last = entity.BackupInfo{
			ID:        b.storageRepo.GetName(last.Folder),
			Failed:    last.IsFailed,
			Locked:    last.IsLocked,
			Sharded:   last.IsSharded,
			TimeStamp: last.TimeStamp,
			Metrics: entity.BackupMetrics{
				ExitCode:  int(exitCode),
				SpentTime: int(spentTime),
				Size:      int(size),
			},
		}

		// Match Python: if last backup failed → status is "Warning"
		if last.IsFailed {
			resp.Status = "Warning"
		}

		for _, v := range vaults {
			if !v.IsFailed && !v.IsLocked {
				m, _ := LoadMetrics(v)
				ec, _ := m["exit_code"].(float64)
				st, _ := m["spent_time"].(float64)
				sz, _ := m["size"].(float64)
				info.LastSuccessful = entity.BackupInfo{
					ID:        b.storageRepo.GetName(v.Folder),
					Failed:    v.IsFailed,
					Locked:    v.IsLocked,
					Sharded:   v.IsSharded,
					TimeStamp: v.TimeStamp,
					Metrics: entity.BackupMetrics{
						ExitCode:  int(ec),
						SpentTime: int(st),
						Size:      int(sz),
					},
				}
				break
			}
		}
	}

	resp.Storage = info
	return resp, nil
}

func (b *BackupDaemon) GetQueueSize() int {
	return b.taskPool.QueueSize()
}

func (b *BackupDaemon) UpdateEvictionPolicy(_ context.Context, request entity.EvictionPolicyRequest) error {
	if request.FullEvictionPolicy == "" {
		return fmt.Errorf("fullEvictionPolicy is required")
	}
	b.evictionPolicy = request.FullEvictionPolicy
	return nil
}

func (b *BackupDaemon) TerminateBackup(_ context.Context, request entity.TerminateRequest) error {
	vault := b.storageRepo.GetVault(request.BackupID, false, request.ExternalBackupPath, "", false)
	if reflect.DeepEqual(vault, entity.Vault{}) {
		return ErrVaultNotFound
	}
	if !vault.IsLocked {
		return fmt.Errorf("backup %s is not running (not locked): %w", request.BackupID, ErrBackupNotRunning)
	}
	return nil
}

func (b *BackupDaemon) DownloadBackup(_ context.Context, backupID string) (string, error) {
	vault := b.storageRepo.GetVault(backupID, false, "", "", false)
	if reflect.DeepEqual(vault, entity.Vault{}) {
		return "", ErrVaultNotFound
	}
	return vault.Folder, nil
}

func getBackupAction(procType string) string {
	if procType == INCREMENTAL {
		return INCREMENTALBACKUP
	}
	return COMMONBACKUP
}

func getRestoreAction(procType string) string {
	if procType == INCREMENTAL {
		return INCREMENTALRESTORE
	}
	return COMMONRESTORE
}

func (b *BackupDaemon) evict(items []entity.Vault, rules string, exclude map[int64]bool) ([]entity.Vault, error) {
	parsedRules, err := parseRules(rules)
	if err != nil {
		return nil, fmt.Errorf("failed to parse rules err: %w", err)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].TimeStamp > items[j].TimeStamp
	})
	rule := parsedRules[0]
	var eviction []entity.Vault

	switch rule.Type {
	case LimitType:
		limit := rule.First
		unique := uniqueVaults(items)
		sort.Slice(unique, func(i, j int) bool {
			return unique[i].TimeStamp > unique[j].TimeStamp
		})
		if limit < len(unique) {
			eviction = append(eviction, unique[limit:]...)
		}
		return eviction, nil
	case IntervalType:
		to := time.Now().Unix()
		for _, r := range parsedRules {
			var operateVersions []entity.Vault
			for _, x := range items {
				if x.TimeStamp <= to-int64(r.First) && !exclude[x.TimeStamp] {
					operateVersions = append(operateVersions, x)
				}
			}
			if r.Second == "delete" {
				eviction = append(eviction, operateVersions...)
			} else {
				interval := int64(r.Second.(int))
				thursday := int64(4 * 24 * 60 * 60)

				groups := make(map[int64][]entity.Vault)
				for _, x := range operateVersions {
					key := (x.TimeStamp - thursday) / interval
					groups[key] = append(groups[key], x)
				}
				for _, versions := range groups {
					sort.Slice(versions, func(i, j int) bool {
						return versions[i].TimeStamp < versions[j].TimeStamp
					})
					eviction = append(eviction, versions[:len(versions)-1]...)
				}
			}
		}
		return uniqueVaults(eviction), nil
	}
	return eviction, nil
}

func uniqueVaults(arr []entity.Vault) []entity.Vault {
	seen := make(map[int64]struct{})
	var res []entity.Vault
	for _, v := range arr {
		if _, ok := seen[v.TimeStamp]; !ok {
			seen[v.TimeStamp] = struct{}{}
			res = append(res, v)
		}
	}
	return res
}

func contains(list []string, item string) bool {
	for _, v := range list {
		if v == item {
			return true
		}
	}
	return false
}

func GetTimeCreationNow() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}
