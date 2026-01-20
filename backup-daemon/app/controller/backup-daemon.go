package controller

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/repo"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const INCREMENTAL = "incremental"
const FULL = "full"
const COMMONBACKUP = "backup"
const INCREMENTALBACKUP = "incremental backup"
const COMMONRESTORE = "restore"
const INCREMENTALRESTORE = "incremental restore"

//go:generate mockgen -source=backup-daemon.go -destination=../rest/mock.go -package=rest
type BackupDaemonUseCase interface {
	EnqueueBackup(ctx context.Context, request entity.BackupRequest) (entity.BackupResponse, error)
	RestoreBackup(ctx context.Context, request entity.RestoreRequest) (entity.RestoreResponse, error)
	EnqueueEviction(ctx context.Context, request entity.EvictRequest) error
	RemoveBackup(ctx context.Context, request entity.EvictByVaultRequest) error
	RemoveBackupV2(ctx context.Context, request entity.EvictByVaultV2Request) error
	GetJobStatus(ctx context.Context, request entity.JobStatusRequest) (entity.JobStatusResponse, error)
	CreateS3PresignedURL(ctx context.Context, request entity.S3PresignedURLRequest) (entity.S3PresignedURLResponse, error)
}

type BackupDaemon struct {
	storageRepo            repo.StorageRepository
	dbRepo                 repo.DBRepository
	scheduler              SchedulerRepository
	s3Client               S3ClientRepository
	executor               CommandExecutor
	s3Enable               bool
	logger                 *zap.SugaredLogger
	evictionPolicy         string
	granularEvictionPolicy string
}

func NewBackupDaemon(storageRepo repo.StorageRepository, dbRepo repo.DBRepository,
	scheduler SchedulerRepository, s3Client S3ClientRepository, executor CommandExecutor,
	s3Enable bool, logger *zap.SugaredLogger, evictionPolicy string, granularEvictionPolicy string) BackupDaemonUseCase {
	return &BackupDaemon{
		storageRepo:            storageRepo,
		dbRepo:                 dbRepo,
		scheduler:              scheduler,
		s3Client:               s3Client,
		executor:               executor,
		s3Enable:               s3Enable,
		logger:                 logger,
		evictionPolicy:         evictionPolicy,
		granularEvictionPolicy: granularEvictionPolicy,
	}
}

// TODO: worker pool, add task
func (b *BackupDaemon) EnqueueBackup(ctx context.Context, request entity.BackupRequest) (entity.BackupResponse, error) {
	dirType := repo.FULL
	if len(request.DBs) == 0 && len(request.ExternalBackupPath) == 0 {
		dirType = repo.GRANULAR
	}
	var commonTS []string
	var err error
	if request.ProcType == INCREMENTAL {
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

	var vault entity.Vault
	if blobPath != "" {
		vault = b.storageRepo.OpenVault("", request.AllowEviction, isGranular, request.Sharded, false, "", request.Prefix, blobPath)
	} else {
		vault = b.storageRepo.OpenVault(request.ExternalBackupPath, request.AllowEviction, isGranular, request.Sharded, isExternal, request.ExternalBackupPath, request.Prefix, "")
	}

	backupID := filepath.Base(vault.Folder)
	dbNames := make([]string, 0, len(request.DBs))
	for _, d := range request.DBs {
		if d.SimpleName != "" {
			dbNames = append(dbNames, d.SimpleName)
		}
	}
	dbsJSON, _ := json.Marshal(dbNames)

	job := entity.Job{TaskID: backupID, Type: action, Status: "Queued", Vault: backupID, Err: "", StorageName: request.CustomVars["storageName"], BlobPath: request.CustomVars["blob_path"], Databases: string(dbsJSON)}

	if err = b.dbRepo.UpdateJob(ctx, job); err != nil {
		return entity.BackupResponse{}, fmt.Errorf("failed to update job err: %w", err)
	}

	if err := b.executor.PerformBackup(vault, request.DBs, request.CustomVars); err != nil {
		tail, _ := b.tailConsole(vault.Folder, 5)
		job.Status = "Failed"
		job.Err = tail
		_ = b.dbRepo.UpdateJob(ctx, job)
		return entity.BackupResponse{}, err
	}

	// TODO
	//b.scheduler.EnqueueExecution()
	if b.s3Enable {
		blobPath := strings.Trim(strings.TrimSpace(request.CustomVars["blob_path"]), "/")

		if blobPath != "" {
			backupID := filepath.Base(vault.Folder)
			prefix := path.Join(blobPath, backupID)

			err = b.s3Client.UploadFolderWithPrefix(ctx, vault.Folder, prefix)
		} else {
			err = b.s3Client.UploadFolder(ctx, vault.Folder)
		}

		if err != nil {
			return entity.BackupResponse{}, fmt.Errorf("failed to upload folder to s3 err: %w", err)
		}
	}
	job.Status = "Successful"
	_ = b.dbRepo.UpdateJob(ctx, job)

	return entity.BackupResponse{
		BackupID: backupID,
	}, nil
}

func (b *BackupDaemon) RestoreBackup(ctx context.Context, request entity.RestoreRequest) (entity.RestoreResponse, error) {
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
	blobPath := strings.Trim(strings.TrimSpace(request.CustomVars["blob_path"]), "/")

	err := b.dbRepo.UpdateJob(ctx, entity.Job{
		TaskID:      taskID,
		Type:        action,
		Status:      "Queued",
		Vault:       "",
		Err:         "",
		StorageName: storageName,
		BlobPath:    blobPath,
		Databases:   string(dbsJSON),
	})
	if err != nil {
		return entity.RestoreResponse{}, fmt.Errorf("failed to update job err: %w", err)
	}
	// TODO
	//b.scheduler.EnqueueExecution()
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

	if b.s3Enable && blobPath != "" {
		s3Prefix := path.Join(blobPath, request.Vault)

		vaultFolder = filepath.Join(os.TempDir(), "backup-daemon", "restore", request.Vault)

		_ = os.RemoveAll(vaultFolder)
		if err := os.MkdirAll(vaultFolder, 0o755); err != nil {
			return entity.RestoreResponse{}, fmt.Errorf("failed to create restore dir %s: %w", vaultFolder, err)
		}

		if err := b.s3Client.DownloadFolder(ctx, s3Prefix, vaultFolder); err != nil {
			return entity.RestoreResponse{}, fmt.Errorf("failed to download backup from s3 prefix=%s err: %w", s3Prefix, err)
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

		vaultFolder = vault.Folder

		if b.s3Enable {
			if err := b.s3Client.DownloadFolder(ctx, vaultFolder, ""); err != nil {
				return entity.RestoreResponse{}, fmt.Errorf("failed to download backup err: %w", err)
			}
		}
	}
	if len(request.DBs) > 0 {
		backedDBs, err := b.executor.GetBackupDBs(vaultFolder)
		if err != nil {
			return entity.RestoreResponse{}, fmt.Errorf("failed to get backup dbs err: %w", err)
		}
		backed := make(map[string]bool, len(backedDBs))
		for _, db := range backedDBs {
			backed[db] = true
		}
		var wrong []string
		for _, db := range request.DBs {
			if db.SimpleName != "" {
				if !backed[db.SimpleName] {
					wrong = append(wrong, db.SimpleName)
				}
			} else if db.Object != nil {
				for k := range db.Object {
					if !backed[k] {
						wrong = append(wrong, k)
					}
				}
			}
		}
		if len(wrong) > 0 {
			err = b.dbRepo.UpdateJob(ctx, entity.Job{
				TaskID:      taskID,
				Type:        action,
				Status:      "Failed",
				Vault:       filepath.Base(request.Vault),
				Err:         fmt.Sprintf("Sorry, but databases %v do not exist in backup %s", wrong, vaultFolder),
				StorageName: storageName,
				BlobPath:    blobPath,
				Databases:   string(dbsJSON),
			})
			if err != nil {
				return entity.RestoreResponse{}, fmt.Errorf("failed to update job err: %w", err)
			}
			return entity.RestoreResponse{}, fmt.Errorf("sorry, but databases %v do not exist in backup %s", wrong, vaultFolder)
		}
		if len(request.ChangeDbNames) > 0 {
			for old := range request.ChangeDbNames {
				if !backed[old] {
					err = b.dbRepo.UpdateJob(ctx, entity.Job{
						TaskID:      taskID,
						Type:        action,
						Status:      "Failed",
						Vault:       filepath.Base(request.Vault),
						Err:         fmt.Sprintf("Sorry, but database name %s from dbmap does not exist in backup %s", old, vaultFolder),
						StorageName: storageName,
						BlobPath:    blobPath,
						Databases:   string(dbsJSON),
					})
					if err != nil {
						return entity.RestoreResponse{}, fmt.Errorf("failed to update job err: %w", err)
					}
					return entity.RestoreResponse{}, fmt.Errorf("sorry, but database name %s from dbmap does not exist in backup %s", old, vaultFolder)
				}
			}
		}
		// TODO python code
		//else:
		//if not configuration.config.enable_full_restore and not dbs:
		//error_message = \
		//"Sorry, but vault %s contains full backup of database, you can't restore it fully via REST API" \
		//% self.trim_storage_from_vault(vault_folder)
		//log.error(error_message)
		//self.db.update_job(task_id, action, "Failed", self.trim_storage_from_vault(vault_folder),
		//	error_message, login=True)
		//return
	}
	err = b.dbRepo.UpdateJob(ctx, entity.Job{
		TaskID:      taskID,
		Type:        action,
		Status:      "Processing",
		Vault:       filepath.Base(request.Vault),
		Err:         "",
		StorageName: storageName,
		BlobPath:    blobPath,
		Databases:   string(dbsJSON),
	})
	if err != nil {
		return entity.RestoreResponse{}, fmt.Errorf("failed to update job err: %w", err)
	}

	err = b.executor.PerformRestore(vaultFolder, request.DBs, request.ChangeDbNames, request.CustomVars, external, taskID)
	b.uploadRestoreLogsToS3(ctx, vaultFolder, request.CustomVars["blob_path"], request.Vault, taskID)

	if err != nil {
		lineNumber := 5
		tail, errTail := b.tailConsole(vaultFolder, lineNumber)
		if errTail != nil {
			return entity.RestoreResponse{}, fmt.Errorf("failed to tail err: %w", errTail)
		}
		if updateErr := b.dbRepo.UpdateJob(ctx, entity.Job{
			TaskID:      taskID,
			Type:        action,
			Status:      "Failed",
			Vault:       filepath.Base(request.Vault),
			Err:         tail,
			StorageName: storageName,
			BlobPath:    blobPath,
			Databases:   string(dbsJSON),
		}); updateErr != nil {
			return entity.RestoreResponse{}, fmt.Errorf("failed to update job: %w", updateErr)
		}
		return entity.RestoreResponse{}, err
	}

	err = b.dbRepo.UpdateJob(ctx, entity.Job{
		TaskID:      taskID,
		Type:        action,
		Status:      "Successful",
		Vault:       filepath.Base(request.Vault),
		Err:         "",
		StorageName: storageName,
		BlobPath:    blobPath,
		Databases:   string(dbsJSON),
	})
	if err != nil {
		return entity.RestoreResponse{}, fmt.Errorf("failed to update job err: %w", err)
	}

	return entity.RestoreResponse{
		TaskID: taskID,
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

	obsoleteFullVaults, err := b.evict(fullVaults, b.evictionPolicy, excludedFiles)
	if err != nil {
		return fmt.Errorf("failed to list evict full vaults err: %w", err)
	}

	granularVaults, err := b.storageRepo.List(repo.GRANULAR, "")
	if err != nil {
		return fmt.Errorf("failed to list granular vaults err: %w", err)
	}

	obsoleteGranularVaults, err := b.evict(granularVaults, b.granularEvictionPolicy, excludedFiles)
	if err != nil {
		return fmt.Errorf("failed to list evict granular vaults err: %w", err)
	}

	obsoleteVaults := append(obsoleteFullVaults, obsoleteGranularVaults...)
	for _, obsoleteVault := range obsoleteVaults {
		err = b.storageRepo.Evict(obsoleteVault.Folder)
		if err != nil {
			return fmt.Errorf("failed to evict backup %s from storage err: %w", obsoleteVault.Folder, err)
		}
		err = b.dbRepo.RemoveVault(ctx, b.storageRepo.GetName(obsoleteVault.Folder))
		if err != nil {
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
	vaultNames, err := b.storageRepo.ListVaultNames(true, repo.ALL, "")
	if err != nil {
		return fmt.Errorf("failed to list all backup by timestamp err: %w", err)
	}
	if !contains(vaultNames, request.Vault) {
		return fmt.Errorf("backup vault %s not found in storage", request.Vault)
	}
	vaultObject := b.storageRepo.GetVault(request.Vault, false, "", "", false)
	if reflect.DeepEqual(vaultObject, entity.Vault{}) {
		return fmt.Errorf("backup vault %s not found in storage", request.Vault)
	}
	if vaultObject.IsLocked {
		return fmt.Errorf("backup vault %s is locked", request.Vault)
	}
	err = b.storageRepo.Evict(vaultObject.Folder)
	if err != nil {
		return fmt.Errorf("failed to evict backup err: %w", err)
	}
	err = b.dbRepo.RemoveVault(ctx, request.Vault)
	if err != nil {
		return fmt.Errorf("failed to remove backup from database err: %w", err)
	}
	err = b.executor.ExecuteEvictCmd(vaultObject.Folder)
	if err != nil {
		return fmt.Errorf("failed to evict backup from executor err: %w", err)
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
		return fmt.Errorf("failed to select job %s: %w", backupID, err)
	}

	normalizeBlobPath := func(p string) string {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, `"'`)
		p = strings.TrimLeft(p, "/")
		return p
	}

	blob := strings.Trim(normalizeBlobPath(request.BlobPath), "/")
	if blob == "" {
		blob = strings.Trim(normalizeBlobPath(job.BlobPath), "/")
	}

	if b.s3Enable && blob != "" {
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
		_ = b.storageRepo.Evict(vaultObj.Folder)
		_ = b.executor.ExecuteEvictCmd(vaultObj.Folder)
	}

	if err := b.dbRepo.RemoveVault(ctx, backupID); err != nil {
		return fmt.Errorf("failed to remove backup %s from database: %w", backupID, err)
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
	response := entity.JobStatusResponse{
		TaskID:      job.TaskID,
		Status:      job.Status,
		Vault:       job.Vault,
		Error:       job.Err,
		Type:        job.Type,
		StorageName: job.StorageName,
		BlobPath:    job.BlobPath,
		Databases:   dbs,
	}
	if job.Status == "Successful" {
		response.StatusCode = http.StatusOK
	} else if job.Status == "Failed" {
		response.StatusCode = http.StatusInternalServerError
	} else {
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

//func (b *BackupDaemon) Find(ctx context.Context, request entity.FindRequest) (entity.FindResponse, error) {
//	vaultName, err := b.storageRepo.FindByTS(request.TimeStamp, repo.ALL, "")
//	if err != nil {
//		return entity.FindResponse{}, fmt.Errorf("failed to find backup by timestamp err: %w", err)
//	}
//	vaultObject := b.storageRepo.GetVault(vaultName, false, "", false)
//	if vaultObject.IsGranular {
//
//	}
//}

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
				interval := r.Second.(int64)
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

func (b *BackupDaemon) tailConsole(folder string, num int) (string, error) {
	filePath := filepath.Join(folder, ".console")

	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer func() {
		errFile := file.Close()
		if errFile != nil {
			err = errFile
		}
	}()

	// Read all lines
	scanner := bufio.NewScanner(file)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}

	if len(lines) < num {
		num = len(lines)
	}
	lastLines := lines[len(lines)-num:]

	return strings.Join(lastLines, " "), nil
}

func (b *BackupDaemon) uploadRestoreLogsToS3(ctx context.Context, vaultFolder, blobPath, backupID, taskID string) {
	if !b.s3Enable {
		return
	}
	blobPath = strings.Trim(strings.TrimSpace(blobPath), "/")
	if blobPath == "" {
		return
	}

	logsDir := filepath.Join(vaultFolder, "restore_logs")
	if _, err := os.Stat(logsDir); err != nil {
		return
	}

	prefix := path.Join(blobPath, backupID, "restore_logs")

	if err := b.s3Client.UploadFolderWithPrefix(ctx, logsDir, prefix); err != nil {
		b.logger.Warnf("failed to upload restore logs to s3 prefix=%s err=%v", prefix, err)
	}
}
