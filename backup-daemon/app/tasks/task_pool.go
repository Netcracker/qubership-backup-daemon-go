package tasks

import (
	"context"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/controller"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/repo"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/utils"
	"go.uber.org/zap"
	"os"
	"path"
	"path/filepath"
)

type Task struct {
	Type       string
	Vault      entity.Vault
	DBs        []entity.DBEntry
	DBMap      map[string]string
	CustomVars map[string]string
	External   bool
	Job        entity.Job
}

// TaskPoolRepository is the only interface that BackupDaemon and Scheduler need
// in order to submit tasks. TaskExecutor reads from the same channel and executes them.
type TaskPoolRepository interface {
	EnqueueTask(task Task)
	QueueSize() int
}

// TaskPool holds the buffered channel and exposes EnqueueTask / QueueSize.
// On creation it immediately starts a single TaskExecutor goroutine.
type TaskPool struct {
	tasks  chan Task
	logger *zap.SugaredLogger
}

// NewTaskPool creates a TaskPool with the given buffer size and starts a TaskExecutor
// that will process tasks until ctx is cancelled.
func NewTaskPool(
	ctx context.Context,
	bufSize int,
	storageRepo repo.StorageRepository,
	executor CommandExecutor,
	dbRepo repo.DBRepository,
	s3Client utils.S3ClientRepository,
	s3Enable bool,
	logger *zap.SugaredLogger,
) TaskPoolRepository {
	tp := &TaskPool{
		tasks:  make(chan Task, bufSize),
		logger: logger,
	}

	te := &TaskExecutor{
		tasks:       tp.tasks,
		storageRepo: storageRepo,
		executor:    executor,
		dbRepo:      dbRepo,
		s3Client:    s3Client,
		s3Enable:    s3Enable,
		logger:      logger,
	}

	go te.run(ctx)

	return tp
}

func (tp *TaskPool) EnqueueTask(task Task) {
	select {
	case tp.tasks <- task:
		tp.logger.Info("Task enqueued successfully",
			zap.String("type", task.Type),
			zap.String("vault", task.Job.Vault),
			zap.Int("queueSize", len(tp.tasks)))
	default:
		tp.logger.Error("Task queue is full, dropping task",
			zap.String("type", task.Type),
			zap.String("vault", task.Job.Vault))
	}
}

func (tp *TaskPool) QueueSize() int {
	return len(tp.tasks)
}

// ---------------------------------------------------------------------------
// TaskExecutor — single goroutine that processes tasks from the channel.
// ---------------------------------------------------------------------------

type TaskExecutor struct {
	tasks       <-chan Task
	storageRepo repo.StorageRepository
	executor    CommandExecutor
	dbRepo      repo.DBRepository
	s3Client    utils.S3ClientRepository
	s3Enable    bool
	logger      *zap.SugaredLogger
}

func (te *TaskExecutor) run(ctx context.Context) {
	te.logger.Info("TaskExecutor started")
	for {
		select {
		case <-ctx.Done():
			te.logger.Info("TaskExecutor stopped: context cancelled")
			return
		case task, ok := <-te.tasks:
			if !ok {
				te.logger.Info("TaskExecutor stopped: channel closed")
				return
			}
			te.process(ctx, task)
		}
	}
}

func (te *TaskExecutor) process(ctx context.Context, task Task) {
	te.logger.Info("Processing task",
		zap.String("type", task.Type),
		zap.String("vault", task.Job.Vault),
		zap.Int("remainingQueue", len(te.tasks)))

	var err error
	switch task.Type {
	case "backup":
		err = te.executor.PerformBackup(task.Vault, task.DBs, task.CustomVars)
		if err == nil {
			te.logger.Info("Backup completed successfully", zap.String("vault", task.Job.Vault))
			err = te.uploadBackupToS3(ctx, task)
		} else {
			te.logger.Error("Backup failed", zap.Error(err), zap.String("vault", task.Job.Vault))
		}

	case "restore":
		err = te.executor.PerformRestore(task.Vault.Folder, task.DBs, task.DBMap, task.CustomVars, task.External, task.Job.TaskID)
		if err == nil {
			te.logger.Info("Restore completed successfully", zap.String("vault", task.Job.Vault))
			if task.CustomVars["blob_path"] != "" {
				te.uploadRestoreLogsToS3(ctx, task.Vault.Folder, task.CustomVars["blob_path"], task.Job.Vault, task.Job.TaskID)
			}
		} else {
			te.logger.Error("Restore failed", zap.Error(err), zap.String("vault", task.Job.Vault))
		}
	}

	status := "Successful"
	if err != nil {
		status = "Failed"
	}

	task.Job.Status = status
	task.Job.Err = ""
	if err != nil {
		task.Job.Err = err.Error()
	}
	task.Job.CompletionTime = controller.GetTimeCreationNow()

	if updateErr := te.dbRepo.UpdateJob(ctx, task.Job); updateErr != nil {
		te.logger.Error("Failed to update job status",
			zap.Error(updateErr),
			zap.String("vault", task.Job.Vault))
	} else {
		te.logger.Info("Job status updated",
			zap.String("vault", task.Job.Vault),
			zap.String("status", status))
	}
}

func (te *TaskExecutor) uploadBackupToS3(ctx context.Context, task Task) error {
	blobPath := task.CustomVars["blob_path"]
	if blobPath != "" {
		backupID := filepath.Base(task.Vault.Folder)
		prefix := path.Join(blobPath, backupID)
		if err := te.s3Client.UploadFolderWithPrefix(ctx, task.Vault.Folder, prefix); err != nil {
			te.logger.Error("S3 upload failed", zap.Error(err), zap.String("vault", task.Job.Vault))
			return err
		}
		te.logger.Info("S3 upload completed", zap.String("vault", task.Job.Vault), zap.String("prefix", prefix))
		return nil
	}
	if te.s3Enable {
		if err := te.s3Client.UploadFolder(ctx, task.Vault.Folder); err != nil {
			te.logger.Error("S3 upload failed", zap.Error(err), zap.String("vault", task.Job.Vault))
			return err
		}
		te.logger.Info("S3 upload completed", zap.String("vault", task.Job.Vault))
	}
	return nil
}

func (te *TaskExecutor) uploadRestoreLogsToS3(ctx context.Context, vaultFolder, blobPath, backupID, taskID string) {
	logsDir := filepath.Join(vaultFolder, "restore_logs")
	if _, err := os.Stat(logsDir); err != nil {
		return
	}
	prefix := path.Join(blobPath, backupID, "restore_logs")
	if err := te.s3Client.UploadFolderWithPrefix(ctx, logsDir, prefix); err != nil {
		te.logger.Warnf("failed to upload restore logs to s3 prefix=%s taskID=%s err=%v", prefix, taskID, err)
	}
}
