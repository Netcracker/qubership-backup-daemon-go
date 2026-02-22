package controller

import (
	"context"
	"os"
	"path"
	"path/filepath"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/repo"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
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

type SchedulerRepository interface {
	EnqueueTask(task Task)
	SetBackupDaemon(backupDaemon BackupDaemonUseCase)
}

type Scheduler struct {
	tasks            chan Task
	storageRepo      repo.StorageRepository
	executor         CommandExecutor
	dbRepo           repo.DBRepository
	s3Client         S3ClientRepository
	s3Enable         bool
	logger           *zap.SugaredLogger
	cron             *cron.Cron
	backupDaemon     BackupDaemonUseCase
	customVars       map[string]string
	scheduledDBs     []string
	schedule         string
	granularSchedule string
}

func NewScheduler(storageRepo repo.StorageRepository, executor CommandExecutor, dbRepo repo.DBRepository, s3Client S3ClientRepository, s3Enable bool, logger *zap.SugaredLogger, numWorkers int, schedule string, granularSchedule string, scheduledDBs []string, customVars map[string]string) SchedulerRepository {
	s := &Scheduler{
		tasks:            make(chan Task, 100),
		storageRepo:      storageRepo,
		executor:         executor,
		dbRepo:           dbRepo,
		s3Client:         s3Client,
		s3Enable:         s3Enable,
		logger:           logger,
		cron:             cron.New(),
		customVars:       customVars,
		scheduledDBs:     scheduledDBs,
		schedule:         schedule,
		granularSchedule: granularSchedule,
	}
	for i := 0; i < numWorkers; i++ {
		go s.worker()
	}

	s.logger.Info("Scheduler initialized",
		zap.Int("numWorkers", numWorkers),
		zap.String("schedule", schedule),
		zap.String("granularSchedule", granularSchedule),
		zap.Strings("scheduledDBs", scheduledDBs),
		zap.Int("taskQueueCapacity", cap(s.tasks)))

	if s.schedule != "" {
		s.cron.AddFunc(s.schedule, func() { s.enqueueCronBackup(false) })
		s.logger.Info("Added full backup cron job", zap.String("schedule", s.schedule))
	}
	if s.granularSchedule != "" && len(s.scheduledDBs) > 0 {
		s.cron.AddFunc(s.granularSchedule, func() { s.enqueueCronBackup(true) })
		s.logger.Info("Added granular backup cron job",
			zap.String("schedule", s.granularSchedule),
			zap.Strings("databases", s.scheduledDBs))
	}
	s.cron.Start()
	s.logger.Info("Scheduler cron jobs started")
	return s
}

func (s *Scheduler) EnqueueTask(task Task) {
	select {
	case s.tasks <- task:
		s.logger.Info("Task enqueued successfully",
			zap.String("type", task.Type),
			zap.String("vault", task.Job.Vault),
			zap.Int("queueSize", len(s.tasks)))
	default:
		s.logger.Error("Task queue is full, dropping task",
			zap.String("type", task.Type),
			zap.String("vault", task.Job.Vault))
	}
}

func (s *Scheduler) worker() {
	s.logger.Info("Scheduler worker started")
	for task := range s.tasks {
		s.logger.Info("Processing task",
			zap.String("type", task.Type),
			zap.String("vault", task.Job.Vault),
			zap.Int("remainingQueue", len(s.tasks)))
		var err error
		if task.Type == "backup" {
			err = s.executor.PerformBackup(task.Vault, task.DBs, task.CustomVars)
			if err == nil {
				s.logger.Info("Backup completed successfully", zap.String("vault", task.Job.Vault))
				ctx := context.Background()
				blobPath := task.CustomVars["blob_path"]
				if blobPath != "" {
					backupID := filepath.Base(task.Vault.Folder)
					prefix := path.Join(blobPath, backupID)
					uploadErr := s.s3Client.UploadFolderWithPrefix(ctx, task.Vault.Folder, prefix)
					if uploadErr != nil {
						err = uploadErr
						s.logger.Error("S3 upload failed", zap.Error(uploadErr), zap.String("vault", task.Job.Vault))
					} else {
						s.logger.Info("S3 upload completed", zap.String("vault", task.Job.Vault), zap.String("prefix", prefix))
					}
				} else if s.s3Enable {
					uploadErr := s.s3Client.UploadFolder(ctx, task.Vault.Folder)
					if uploadErr != nil {
						err = uploadErr
						s.logger.Error("S3 upload failed", zap.Error(uploadErr), zap.String("vault", task.Job.Vault))
					} else {
						s.logger.Info("S3 upload completed", zap.String("vault", task.Job.Vault))
					}
				}
			} else {
				s.logger.Error("Backup failed", zap.Error(err), zap.String("vault", task.Job.Vault))
			}
		} else if task.Type == "restore" {
			err = s.executor.PerformRestore(task.Vault.Folder, task.DBs, task.DBMap, task.CustomVars, task.External, task.Job.TaskID)
			if err == nil {
				s.logger.Info("Restore completed successfully", zap.String("vault", task.Job.Vault))
				if task.CustomVars["blob_path"] != "" {
					s.uploadRestoreLogsToS3(context.Background(), task.Vault.Folder, task.CustomVars["blob_path"], task.Job.Vault, task.Job.TaskID)
				}
			} else {
				s.logger.Error("Restore failed", zap.Error(err), zap.String("vault", task.Job.Vault))
			}
		}

		status := "Successful"
		if err != nil {
			status = "Failed"
		}

		// Update job status
		task.Job.Status = status
		task.Job.Err = ""
		if err != nil {
			task.Job.Err = err.Error()
		}
		if updateErr := s.dbRepo.UpdateJob(context.Background(), task.Job); updateErr != nil {
			s.logger.Error("Failed to update job status", zap.Error(updateErr), zap.String("vault", task.Job.Vault))
		} else {
			s.logger.Info("Job status updated",
				zap.String("vault", task.Job.Vault),
				zap.String("status", status))
		}
	}
	s.logger.Info("Scheduler worker stopped")
}

func (s *Scheduler) uploadRestoreLogsToS3(ctx context.Context, vaultFolder, blobPath, backupID, taskID string) {
	logsDir := filepath.Join(vaultFolder, "restore_logs")
	if _, err := os.Stat(logsDir); err != nil {
		return
	}

	prefix := path.Join(blobPath, backupID, "restore_logs")

	if err := s.s3Client.UploadFolderWithPrefix(ctx, logsDir, prefix); err != nil {
		s.logger.Warnf("failed to upload restore logs to s3 prefix=%s err=%v", prefix, err)
	}
}

func (s *Scheduler) SetBackupDaemon(backupDaemon BackupDaemonUseCase) {
	s.backupDaemon = backupDaemon
	s.logger.Info("BackupDaemon set for scheduler - cron jobs now active")
}

func (s *Scheduler) enqueueCronBackup(isGranular bool) {
	s.logger.Info("Cron backup triggered", zap.Bool("isGranular", isGranular))

	if s.backupDaemon == nil {
		s.logger.Warn("BackupDaemon not set, skipping cron backup")
		return
	}
	ctx := context.Background()
	request := entity.BackupRequest{
		AllowEviction: "true",
		CustomVars:    s.customVars,
	}
	if isGranular {
		for _, dbName := range s.scheduledDBs {
			request.DBs = append(request.DBs, entity.DBEntry{Name: dbName})
		}
		s.logger.Info("Enqueuing granular cron backup",
			zap.Strings("databases", s.scheduledDBs),
			zap.Int("dbCount", len(request.DBs)))
	} else {
		s.logger.Info("Enqueuing full cron backup")
	}

	_, err := s.backupDaemon.EnqueueBackup(ctx, request)
	if err != nil {
		s.logger.Error("Failed to enqueue cron backup", zap.Error(err), zap.Bool("isGranular", isGranular))
	} else {
		s.logger.Info("Successfully enqueued cron backup", zap.Bool("isGranular", isGranular))
	}
}
