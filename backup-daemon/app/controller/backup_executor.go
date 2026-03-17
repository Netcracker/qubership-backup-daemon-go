package controller

import (
	"context"
	"sort"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/repo"
)

// BackupExecutor routes use-case calls to either the full or incremental BackupDaemon
// based on ProcType, mirroring the Python BackupExecutor pattern.
type BackupExecutor struct {
	full        BackupDaemonUseCase
	incremental BackupDaemonUseCase
	fullStorage repo.StorageRepository
	incrStorage repo.StorageRepository
}

func NewBackupExecutor(
	full BackupDaemonUseCase,
	incremental BackupDaemonUseCase,
	fullStorage repo.StorageRepository,
	incrStorage repo.StorageRepository,
) BackupDaemonUseCase {
	return &BackupExecutor{
		full:        full,
		incremental: incremental,
		fullStorage: fullStorage,
		incrStorage: incrStorage,
	}
}

func (e *BackupExecutor) processor(procType string) BackupDaemonUseCase {
	if procType == INCREMENTAL {
		return e.incremental
	}
	return e.full
}

// EnqueueBackup routes to the correct processor.
// For incremental backups it merges timestamps from both storages to resolve start_ts,
// mirroring the Python BackupExecutor.enqueue_backup logic.
func (e *BackupExecutor) EnqueueBackup(ctx context.Context, request entity.BackupRequest) (entity.BackupResponse, error) {
	if request.ProcType == INCREMENTAL {
		if request.CustomVars == nil {
			request.CustomVars = make(map[string]string)
		}

		// Only resolve start_ts when not using external backup path
		if request.ExternalBackupPath == "" {
			fullTS, err := e.fullStorage.ListVaultNames(true, repo.ALL, "")
			if err != nil {
				fullTS = []string{}
			}
			incrTS, err := e.incrStorage.ListVaultNames(true, repo.ALL, "")
			if err != nil {
				incrTS = []string{}
			}

			commonTS := append(fullTS, incrTS...)
			sort.Slice(commonTS, func(i, j int) bool {
				return commonTS[i] > commonTS[j]
			})

			if len(commonTS) > 0 {
				request.CustomVars["start_ts"] = commonTS[0]
			}
		}

		return e.incremental.EnqueueBackup(ctx, request)
	}
	return e.full.EnqueueBackup(ctx, request)
}

func (e *BackupExecutor) RestoreBackup(ctx context.Context, request entity.RestoreRequest) (entity.RestoreResponse, error) {
	return e.processor(request.ProcType).RestoreBackup(ctx, request)
}

func (e *BackupExecutor) EnqueueEviction(ctx context.Context, request entity.EvictRequest) error {
	return e.processor(request.ProcType).EnqueueEviction(ctx, request)
}

func (e *BackupExecutor) RemoveBackup(ctx context.Context, request entity.EvictByVaultRequest) error {
	return e.processor(request.ProcType).RemoveBackup(ctx, request)
}

func (e *BackupExecutor) RemoveBackupV2(ctx context.Context, request entity.EvictByVaultV2Request) error {
	return e.full.RemoveBackupV2(ctx, request)
}

func (e *BackupExecutor) RemoveRestoreV2(ctx context.Context, request entity.EvictByVaultV2Request) error {
	return e.full.RemoveRestoreV2(ctx, request)
}

func (e *BackupExecutor) GetJobStatus(ctx context.Context, request entity.JobStatusRequest) (entity.JobStatusResponse, error) {
	return e.full.GetJobStatus(ctx, request)
}

func (e *BackupExecutor) CreateS3PresignedURL(ctx context.Context, request entity.S3PresignedURLRequest) (entity.S3PresignedURLResponse, error) {
	return e.processor(request.ProcType).CreateS3PresignedURL(ctx, request)
}

func (e *BackupExecutor) ListBackups(ctx context.Context, procType string) ([]string, error) {
	return e.processor(procType).ListBackups(ctx, procType)
}

func (e *BackupExecutor) GetBackupStats(ctx context.Context, vaultName string, ts string, backupPath string, procType string) (map[string]interface{}, error) {
	return e.processor(procType).GetBackupStats(ctx, vaultName, ts, backupPath, procType)
}

func (e *BackupExecutor) ListBackup(ctx context.Context, procType string, vaultPath string) (map[string]interface{}, error) {
	return e.processor(procType).ListBackup(ctx, procType, vaultPath)
}

// GetHealth aggregates health from both processors.
func (e *BackupExecutor) GetHealth(ctx context.Context, procType string) (entity.HealthResponse, error) {
	if procType == INCREMENTAL {
		return e.incremental.GetHealth(ctx, procType)
	}
	return e.full.GetHealth(ctx, procType)
}

func (e *BackupExecutor) Find(ctx context.Context, request entity.FindRequest) (map[string]interface{}, error) {
	return e.processor(request.ProcType).Find(ctx, request)
}

func (e *BackupExecutor) UpdateEvictionPolicy(ctx context.Context, request entity.EvictionPolicyRequest) error {
	return e.full.UpdateEvictionPolicy(ctx, request)
}

func (e *BackupExecutor) TerminateBackup(ctx context.Context, request entity.TerminateRequest) error {
	return e.full.TerminateBackup(ctx, request)
}

func (e *BackupExecutor) GetQueueSize() int {
	return e.full.GetQueueSize() + e.incremental.GetQueueSize()
}

func (e *BackupExecutor) DownloadBackup(ctx context.Context, backupID string) (string, error) {
	return e.full.DownloadBackup(ctx, backupID)
}
