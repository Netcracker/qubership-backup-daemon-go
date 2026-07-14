package tasks

import (
	"context"
	"errors"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/repo"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/utils"
	"go.uber.org/zap"
)

const ProcTypeFull = "full"
const ProcTypeIncremental = "incremental"

type Task struct {
	Type        string
	ProcType    string // "full" or "incremental" — selects which executor runs the task
	Vault       entity.Vault
	DBs         []entity.DBEntry
	DBMap       map[string]string
	CustomVars  map[string]string
	External    bool
	Job         entity.Job
	IsScheduled bool
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

// NewTaskPool creates a TaskPool with the given buffer size and starts a single TaskExecutor
// that routes tasks to fullExecutor or incrExecutor based on Task.ProcType.
func NewTaskPool(
	ctx context.Context,
	bufSize int,
	fullExecutor CommandExecutor,
	incrExecutor CommandExecutor,
	dbRepo repo.DBRepository,
	s3Client utils.S3ClientRepository,
	s3Enable bool,
	s3Registry utils.S3AliasRegistry,
	logger *zap.SugaredLogger,
) TaskPoolRepository {
	tp := &TaskPool{
		tasks:  make(chan Task, bufSize),
		logger: logger,
	}

	te := &TaskExecutor{
		tasks:        tp.tasks,
		fullExecutor: fullExecutor,
		incrExecutor: incrExecutor,
		dbRepo:       dbRepo,
		s3Client:     s3Client,
		s3Enable:     s3Enable,
		s3Registry:   s3Registry,
		logger:       logger,
	}

	go te.run(ctx)

	return tp
}

func (tp *TaskPool) EnqueueTask(task Task) {
	select {
	case tp.tasks <- task:
		tp.logger.Info("Task enqueued successfully",
			zap.String("type", task.Type),
			zap.String("procType", task.ProcType),
			zap.String("vault", task.Job.Vault),
			zap.Int("queueSize", len(tp.tasks)))
	default:
		tp.logger.Error("Task queue is full, dropping task",
			zap.String("type", task.Type),
			zap.String("procType", task.ProcType),
			zap.String("vault", task.Job.Vault))
	}
}

func (tp *TaskPool) QueueSize() int {
	return len(tp.tasks)
}

// NewTaskPoolForTest creates a TaskPool with given buffer and returns both the pool
// and the underlying channel — for use in tests only.
func NewTaskPoolForTest(bufSize int, logger *zap.SugaredLogger) (*TaskPool, chan Task) {
	ch := make(chan Task, bufSize)
	return &TaskPool{tasks: ch, logger: logger}, ch
}

// NewTaskExecutorForTest creates a TaskExecutor wired to the given channel — for use in tests only.
// Both fullExecutor and incrExecutor are set to the same executor for simplicity.
func NewTaskExecutorForTest(
	tasksCh chan Task,
	executor CommandExecutor,
	dbRepo repo.DBRepository,
	s3Client utils.S3ClientRepository,
	s3Enable bool,
	logger *zap.SugaredLogger,
) *TaskExecutor {
	return &TaskExecutor{
		tasks:        tasksCh,
		fullExecutor: executor,
		incrExecutor: executor,
		dbRepo:       dbRepo,
		s3Client:     s3Client,
		s3Enable:     s3Enable,
		logger:       logger,
	}
}

// ---------------------------------------------------------------------------
// TaskExecutor — single goroutine that processes tasks from the channel.
// Routes to fullExecutor or incrExecutor based on Task.ProcType.
// ---------------------------------------------------------------------------

type TaskExecutor struct {
	tasks        <-chan Task
	fullExecutor CommandExecutor
	incrExecutor CommandExecutor
	dbRepo       repo.DBRepository
	s3Client     utils.S3ClientRepository
	s3Registry   utils.S3AliasRegistry
	s3Enable     bool
	logger       *zap.SugaredLogger
}

func (te *TaskExecutor) resolveS3Client(storageName string) utils.S3ClientRepository {
	if te.s3Registry != nil && storageName != "" {
		if client, err := te.s3Registry.Get(storageName); err == nil {
			return client
		}
		te.logger.Warnf("s3 alias %q not found, falling back to default client", storageName)
	}
	return te.s3Client
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
			te.Process(ctx, task)
		}
	}
}

// selectExecutor returns the appropriate executor based on Task.ProcType.
func (te *TaskExecutor) selectExecutor(task Task) CommandExecutor {
	if task.ProcType == ProcTypeIncremental {
		return te.incrExecutor
	}
	return te.fullExecutor
}

// Process handles a single task — exported for testing.
func (te *TaskExecutor) Process(ctx context.Context, task Task) {
	executor := te.selectExecutor(task)

	te.logger.Debug("Processing task",
		zap.String("type", task.Type),
		zap.String("procType", task.ProcType),
		zap.String("vault", task.Job.Vault),
		zap.Int("remainingQueue", len(te.tasks)))

	task.Job.Status = "Processing"
	if updateErr := te.dbRepo.UpdateJob(ctx, task.Job); updateErr != nil {
		te.logger.Error("Failed to update job status",
			zap.Error(updateErr),
			zap.String("vault", task.Job.Vault))
	}

	var err error
	switch task.Type {
	case "backup":
		err = executor.PerformBackup(task.Vault, task.DBs, task.CustomVars)
		if err == nil {
			te.logger.Debug("Backup completed successfully", zap.String("vault", task.Job.Vault))
			if te.s3Enable || task.CustomVars["blob_path"] != "" {
				err = te.moveBackupToS3(ctx, task)
				if err != nil {
					te.logger.Errorf("Failed to move backup to S3: %v", err)
				}
			}
			if err == nil {
				// This runs for EVERY successful backup, whether manual or scheduled
				if evictErr := executor.PerformEviction(ctx); evictErr != nil {
					te.logger.Warn("Automatic eviction failed after backup",
						zap.Error(evictErr), zap.String("vault", task.Job.Vault))
				}
			}
		} else {
			te.logger.Error("Backup failed", zap.Error(err), zap.String("vault", task.Job.Vault))
		}

	case "restore":
		err = executor.PerformRestore(task.Vault.Folder, task.DBs, task.DBMap, task.CustomVars, task.External, task.Job.TaskID)
		if err == nil {
			te.logger.Debug("Restore completed successfully", zap.String("vault", task.Job.Vault))
			if task.CustomVars["blob_path"] != "" {
				te.moveRestoreLogsToS3(ctx, task.Vault.Folder, task.CustomVars["blob_path"], task.Job.Vault, task.Job.TaskID, task.CustomVars["storageName"])
			}
		} else {
			te.logger.Error("Restore failed", zap.Error(err), zap.String("vault", task.Job.Vault))
		}
	default:
		te.logger.Error("Unknown task type", zap.String("type", task.Type))
		err = errors.New("unknown task type")
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
	task.Job.CompletionTime = getTimeCreationNow()

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

func getTimeCreationNow() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func (te *TaskExecutor) moveBackupToS3(ctx context.Context, task Task) error {
	s3c := te.resolveS3Client(task.CustomVars["storageName"])

	blobPath := task.CustomVars["blob_path"]
	if blobPath != "" {
		backupID := filepath.Base(task.Vault.Folder)
		prefix := path.Join(blobPath, backupID)
		if err := s3c.UploadFolderWithPrefix(ctx, task.Vault.Folder, prefix); err != nil {
			te.logger.Error("S3 upload failed", zap.Error(err), zap.String("vault", task.Job.Vault))
			return err
		}

		te.logger.Info("S3 upload completed", zap.String("vault", task.Job.Vault), zap.String("prefix", prefix))
		return nil
	}
	if te.s3Enable {
		if err := s3c.UploadFolder(ctx, task.Vault.Folder); err != nil {
			te.logger.Error("S3 upload failed", zap.Error(err), zap.String("vault", task.Job.Vault))
			return err
		}

		te.logger.Info("S3 upload completed", zap.String("vault", task.Job.Vault))
	}

	te.logger.Debug("Folder will be removed", zap.String("folder", task.Vault.Folder))
	err := os.RemoveAll(task.Vault.Folder)
	if err != nil {
		te.logger.Warnf("Failed to remove local vault folder %s err=%v", task.Vault.Folder, err)
	}
	return nil
}

func (te *TaskExecutor) moveRestoreLogsToS3(ctx context.Context, vaultFolder, blobPath, backupID, taskID, storageName string) {
	logsDir := filepath.Join(vaultFolder, "restore_logs")
	if _, err := os.Stat(logsDir); err != nil {
		return
	}
	s3c := te.resolveS3Client(storageName)
	prefix := path.Join(blobPath, backupID, "restore_logs")
	if err := s3c.UploadFolderWithPrefix(ctx, logsDir, prefix); err != nil {
		te.logger.Warnf("failed to upload restore logs to s3 prefix=%s taskID=%s err=%v", prefix, taskID, err)
	}
	te.logger.Debug("Folder will be removed", zap.String("folder", vaultFolder))
	err := os.RemoveAll(vaultFolder)
	if err != nil {
		te.logger.Warnf("Failed to remove local vault folder %s err=%v", vaultFolder, err)
	}
}
