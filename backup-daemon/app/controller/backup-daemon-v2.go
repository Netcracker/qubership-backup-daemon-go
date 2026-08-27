package controller

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/repo"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/tasks"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/utils"
	"go.uber.org/zap"
)

type BackupDaemonV2 struct {
	BackupDaemon
}

func (d *BackupDaemonV2) resolveRestoreVault(ctx context.Context, request entity.RestoreRequest, external bool) (restoreVaultResult, error) {
	blobPath := request.CustomVars["blob_path"]
	storageName := request.CustomVars["storageName"]
	if storageName == "" {
		storageName = defaultStorageName
	}

	s3Prefix := path.Join(blobPath, request.Vault)
	vaultFolder := filepath.Join(d.storageRepo.GetRoot(), repo.S3_PROCESSING, request.Vault)
	d.logger.Debug("Local temporary Vault folder", zap.String("vaultFolder", vaultFolder))
	_ = os.RemoveAll(vaultFolder)

	if err := os.MkdirAll(vaultFolder, 0o755); err != nil {
		return restoreVaultResult{}, fmt.Errorf("failed to create restore dir %s: %w", vaultFolder, err)
	}
	s3c, err := d.s3Registry.Get(storageName)
	if err != nil {
		return restoreVaultResult{}, fmt.Errorf("failed to resolve s3 client for storage %q: %w", storageName, err)
	}
	if err := s3c.DownloadFolder(ctx, s3Prefix, vaultFolder); err != nil {
		return restoreVaultResult{}, fmt.Errorf("failed to download backup from s3 prefix=%s err: %w", s3Prefix, err)
	}

	vault := d.storageRepo.GetVault(request.Vault, external, request.ExternalBackupPath, blobPath, false)
	return restoreVaultResult{
		vault:              vault,
		vaultFolder:        vaultFolder,
		needsDownloadCheck: true,
	}, nil
}

func (b *BackupDaemonV2) GetBackupStats(ctx context.Context, vaultName string, ts string, backupPath string, procType string) (result map[string]interface{}, err error) {
	result = make(map[string]interface{})
	name := vaultName

	if name == "" && ts != "" {
		name, err = b.storageRepo.FindByTS(ts, repo.ALL, backupPath)
		if err != nil || name == "" {
			return result, fmt.Errorf("backup with ts %s or newer not found", ts)
		}
	} else if name == "" {
		return result, fmt.Errorf("backup name or ts not found")
	}

	job, dbErr := b.dbRepo.SelectEverything(ctx, name)
	if dbErr != nil {
		return result, fmt.Errorf("backup %s not found", name)
	}
	blob := job.BlobPath

	vaultObj := b.storageRepo.GetVault(name, false, "", blob, false)
	var metricErr error
	vaultObj.Metrics, metricErr = b.storageRepo.LoadMetrics(vaultObj)
	if metricErr != nil {
		b.logger.Debugf(metricErr.Error())
	}

	if vaultObj.IsGranular {
		dbList, execErr := b.executor.GetBackupDBs(vaultObj.Folder)
		if execErr != nil {
			s3c, s3Err := b.resolveS3Client(job.StorageName)
			if s3Err != nil {
				b.logger.Warnf("failed to list granular DBs: %v", execErr)
			} else {
				s3Path := path.Join(blob, name)
				parent := strings.TrimRight(s3Path, "/") + "/"
				prefixes, listErr := s3c.ListCommonPrefixes(ctx, s3Path)
				if listErr != nil {
					b.logger.Warnf("failed to list granular DBs from S3: %v", listErr)
				} else {
					for _, p := range prefixes {
						dbName := strings.TrimSuffix(strings.TrimPrefix(p, parent), "/")
						if dbName != "" {
							dbList = append(dbList, dbName)
						}
					}
				}
			}
		}
		result["db_list"] = dbList
	} else {
		result["db_list"] = fmt.Sprintf("%s backup", procType)
	}

	for k, v := range vaultObj.ToMap() {
		result[k] = v
	}
	for k, v := range vaultObj.Metrics {
		result[k] = v
	}
	result["id"] = name
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

	failed := b.convertInterfaceToBool(result["failed"])
	locked := b.convertInterfaceToBool(result["locked"])
	_, hasException := result["exception"]

	result["valid"] = !failed && !locked && !hasException
	result["evictable"] = vaultObj.IsEvictable

	if b.storageRepo.HasCustomVars(vaultObj) {
		result["custom_vars"] = b.storageRepo.LoadCustomVariables(vaultObj)
	}

	b.logger.Debugf("Backup stats for backup %s: %+v", name, result)
	return result, nil
}

func (b *BackupDaemonV2) CreateS3PresignedURL(ctx context.Context, request entity.S3PresignedURLRequest) (entity.S3PresignedURLResponse, error) {
	job, err := b.dbRepo.SelectEverything(ctx, request.BackupID)
	if err != nil {
		return entity.S3PresignedURLResponse{}, fmt.Errorf("backup vault %s not found", request.BackupID)
	}

	s3c, err := b.resolveS3Client(job.StorageName)
	if err != nil {
		return entity.S3PresignedURLResponse{}, fmt.Errorf("failed to resolve s3 client: %w", err)
	}

	s3Path := path.Join(job.BlobPath, request.BackupID)
	files, err := s3c.ListFiles(ctx, s3Path)
	if err != nil {
		return entity.S3PresignedURLResponse{}, fmt.Errorf("failed to list files from s3 err: %w", err)
	}

	extensions := []string{".zip", ".tar", ".gz"}
	var urls []string
	for _, file := range files {
		for _, extension := range extensions {
			if strings.HasSuffix(file, extension) {
				url, err := s3c.CreatePresignedUrl(ctx, file, request.Expiration)
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

func NewBackupDaemonV2(storageRepo repo.StorageRepository, dbRepo repo.DBRepository,
	taskPool tasks.TaskPoolRepository, s3Registry utils.S3AliasRegistry, executor tasks.CommandExecutor,
	logger *zap.SugaredLogger) BackupDaemonUseCase {
	d := &BackupDaemonV2{
		BackupDaemon: BackupDaemon{
			storageRepo: storageRepo,
			dbRepo:      dbRepo,
			taskPool:    taskPool,
			s3Registry:  s3Registry,
			executor:    executor,
			logger:      logger,
		},
	}
	d.BackupDaemon.resolveRestoreVault = d.resolveRestoreVault
	return d
}
