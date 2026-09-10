package controller

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"

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
