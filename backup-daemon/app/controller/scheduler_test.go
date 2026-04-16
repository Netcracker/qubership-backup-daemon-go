package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/tasks"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/utils"
	"github.com/robfig/cron/v3"
	gomock "go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func newTestTaskExecutor(t *testing.T, executor *MockCommandExecutor, dbRepo *MockDBRepository,
	s3Client *utils.MockS3ClientRepository, s3Enable bool) (*tasks.TaskExecutor, chan tasks.Task) {
	t.Helper()
	ch := make(chan tasks.Task, 10)
	return tasks.NewTaskExecutorForTest(ch, executor, dbRepo, s3Client, s3Enable, zap.NewNop().Sugar()), ch
}

// --- TaskExecutor process tests ---

func TestWorker_BackupSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	executor := NewMockCommandExecutor(ctrl)
	dbRepo := NewMockDBRepository(ctrl)
	s3Client := utils.NewMockS3ClientRepository(ctrl)

	te, _ := newTestTaskExecutor(t, executor, dbRepo, s3Client, false)

	task := tasks.Task{
		Type: "backup",
		Vault: entity.Vault{
			Folder: "/tmp/test-vault",
		},
		DBs:        nil,
		CustomVars: map[string]string{},
		Job: entity.Job{
			TaskID: "backup-1",
			Type:   "backup",
			Status: "Queued",
			Vault:  "test-vault",
		},
	}

	executor.EXPECT().PerformBackup(task.Vault, task.DBs, task.CustomVars).Return(nil)
	gomock.InOrder(
		dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, job entity.Job) error {
				if job.Status != "Processing" {
					t.Errorf("expected status Processing, got %s", job.Status)
				}
				return nil
			}),
		dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, job entity.Job) error {
				if job.Status != "Successful" {
					t.Errorf("expected status Successful, got %s", job.Status)
				}
				if job.Err != "" {
					t.Errorf("expected empty error, got %s", job.Err)
				}
				if job.CompletionTime == "" {
					t.Error("expected CompletionTime to be set")
				}
				return nil
			}),
	)

	te.Process(context.Background(), task)
}

func TestWorker_BackupFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	executor := NewMockCommandExecutor(ctrl)
	dbRepo := NewMockDBRepository(ctrl)
	s3Client := utils.NewMockS3ClientRepository(ctrl)

	te, _ := newTestTaskExecutor(t, executor, dbRepo, s3Client, false)

	task := tasks.Task{
		Type: "backup",
		Vault: entity.Vault{
			Folder: "/tmp/test-vault",
		},
		CustomVars: map[string]string{},
		Job: entity.Job{
			TaskID: "backup-fail-1",
			Type:   "backup",
			Status: "Queued",
			Vault:  "test-vault",
		},
	}

	backupErr := errors.New("disk full")
	executor.EXPECT().PerformBackup(task.Vault, task.DBs, task.CustomVars).Return(backupErr)
	gomock.InOrder(
		dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, job entity.Job) error {
				if job.Status != "Processing" {
					t.Errorf("expected status Processing, got %s", job.Status)
				}
				return nil
			}),
		dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, job entity.Job) error {
				if job.Status != "Failed" {
					t.Errorf("expected status Failed, got %s", job.Status)
				}
				if job.Err != "disk full" {
					t.Errorf("expected error 'disk full', got %s", job.Err)
				}
				if job.CompletionTime == "" {
					t.Error("expected CompletionTime to be set")
				}
				return nil
			}),
	)

	te.Process(context.Background(), task)
}

func TestWorker_BackupWithS3Upload(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	executor := NewMockCommandExecutor(ctrl)
	dbRepo := NewMockDBRepository(ctrl)
	s3Client := utils.NewMockS3ClientRepository(ctrl)

	te, _ := newTestTaskExecutor(t, executor, dbRepo, s3Client, true)

	task := tasks.Task{
		Type: "backup",
		Vault: entity.Vault{
			Folder: "/tmp/test-vault",
		},
		CustomVars: map[string]string{},
		Job: entity.Job{
			TaskID: "backup-s3-1",
			Type:   "backup",
			Status: "Queued",
			Vault:  "test-vault",
		},
	}

	executor.EXPECT().PerformBackup(task.Vault, task.DBs, task.CustomVars).Return(nil)
	s3Client.EXPECT().UploadFolder(gomock.Any(), "/tmp/test-vault").Return(nil)
	gomock.InOrder(
		dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, job entity.Job) error {
				if job.Status != "Processing" {
					t.Errorf("expected status Processing, got %s", job.Status)
				}
				return nil
			}),
		dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, job entity.Job) error {
				if job.Status != "Successful" {
					t.Errorf("expected status Successful, got %s", job.Status)
				}
				return nil
			}),
	)

	te.Process(context.Background(), task)
}

func TestWorker_BackupWithBlobPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	executor := NewMockCommandExecutor(ctrl)
	dbRepo := NewMockDBRepository(ctrl)
	s3Client := utils.NewMockS3ClientRepository(ctrl)

	te, _ := newTestTaskExecutor(t, executor, dbRepo, s3Client, false)

	task := tasks.Task{
		Type: "backup",
		Vault: entity.Vault{
			Folder: "/tmp/test-vault",
		},
		CustomVars: map[string]string{"blob_path": "my-bucket/backups"},
		Job: entity.Job{
			TaskID: "backup-blob-1",
			Type:   "backup",
			Status: "Queued",
			Vault:  "test-vault",
		},
	}

	executor.EXPECT().PerformBackup(task.Vault, task.DBs, task.CustomVars).Return(nil)
	s3Client.EXPECT().UploadFolderWithPrefix(gomock.Any(), "/tmp/test-vault", "my-bucket/backups/test-vault").Return(nil)
	gomock.InOrder(
		dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, job entity.Job) error {
				if job.Status != "Processing" {
					t.Errorf("expected status Processing, got %s", job.Status)
				}
				return nil
			}),
		dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, job entity.Job) error {
				if job.Status != "Successful" {
					t.Errorf("expected status Successful, got %s", job.Status)
				}
				return nil
			}),
	)

	te.Process(context.Background(), task)
}

func TestWorker_BackupS3UploadFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	executor := NewMockCommandExecutor(ctrl)
	dbRepo := NewMockDBRepository(ctrl)
	s3Client := utils.NewMockS3ClientRepository(ctrl)

	te, _ := newTestTaskExecutor(t, executor, dbRepo, s3Client, true)

	task := tasks.Task{
		Type: "backup",
		Vault: entity.Vault{
			Folder: "/tmp/test-vault",
		},
		CustomVars: map[string]string{},
		Job: entity.Job{
			TaskID: "backup-s3fail-1",
			Type:   "backup",
			Status: "Queued",
			Vault:  "test-vault",
		},
	}

	executor.EXPECT().PerformBackup(task.Vault, task.DBs, task.CustomVars).Return(nil)
	s3Client.EXPECT().UploadFolder(gomock.Any(), "/tmp/test-vault").Return(errors.New("s3 timeout"))
	gomock.InOrder(
		dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, job entity.Job) error {
				if job.Status != "Processing" {
					t.Errorf("expected status Processing, got %s", job.Status)
				}
				return nil
			}),
		dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, job entity.Job) error {
				if job.Status != "Failed" {
					t.Errorf("expected status Failed after S3 upload failure, got %s", job.Status)
				}
				if !strings.Contains(job.Err, "s3 timeout") {
					t.Errorf("expected error to contain 's3 timeout', got %s", job.Err)
				}
				return nil
			}),
	)

	te.Process(context.Background(), task)
}

func TestWorker_RestoreSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	executor := NewMockCommandExecutor(ctrl)
	dbRepo := NewMockDBRepository(ctrl)
	s3Client := utils.NewMockS3ClientRepository(ctrl)

	te, _ := newTestTaskExecutor(t, executor, dbRepo, s3Client, false)

	task := tasks.Task{
		Type: "restore",
		Vault: entity.Vault{
			Folder: "/tmp/test-vault",
		},
		DBs:        []entity.DBEntry{{SimpleName: "mydb"}},
		DBMap:      nil,
		CustomVars: map[string]string{},
		External:   false,
		Job: entity.Job{
			TaskID: "restore-1",
			Type:   "restore",
			Status: "Queued",
			Vault:  "test-vault",
		},
	}

	executor.EXPECT().PerformRestore("/tmp/test-vault", task.DBs, task.DBMap, task.CustomVars, false, "restore-1").Return(nil)
	gomock.InOrder(
		dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, job entity.Job) error {
				if job.Status != "Processing" {
					t.Errorf("expected status Processing, got %s", job.Status)
				}
				return nil
			}),
		dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, job entity.Job) error {
				if job.Status != "Successful" {
					t.Errorf("expected status Successful, got %s", job.Status)
				}
				if job.CompletionTime == "" {
					t.Error("expected CompletionTime to be set")
				}
				return nil
			}),
	)

	te.Process(context.Background(), task)
}

func TestWorker_RestoreFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	executor := NewMockCommandExecutor(ctrl)
	dbRepo := NewMockDBRepository(ctrl)
	s3Client := utils.NewMockS3ClientRepository(ctrl)

	te, _ := newTestTaskExecutor(t, executor, dbRepo, s3Client, false)

	task := tasks.Task{
		Type: "restore",
		Vault: entity.Vault{
			Folder: "/tmp/test-vault",
		},
		CustomVars: map[string]string{},
		Job: entity.Job{
			TaskID: "restore-fail-1",
			Type:   "restore",
			Status: "Queued",
			Vault:  "test-vault",
		},
	}

	executor.EXPECT().PerformRestore(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.New("restore script crashed"))
	gomock.InOrder(
		dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, job entity.Job) error {
				if job.Status != "Processing" {
					t.Errorf("expected status Processing, got %s", job.Status)
				}
				return nil
			}),
		dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, job entity.Job) error {
				if job.Status != "Failed" {
					t.Errorf("expected status Failed, got %s", job.Status)
				}
				if !strings.Contains(job.Err, "restore script crashed") {
					t.Errorf("expected error to contain 'restore script crashed', got %s", job.Err)
				}
				return nil
			}),
	)

	te.Process(context.Background(), task)
}

func TestWorker_CompletionTimeIsSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	executor := NewMockCommandExecutor(ctrl)
	dbRepo := NewMockDBRepository(ctrl)
	s3Client := utils.NewMockS3ClientRepository(ctrl)

	te, _ := newTestTaskExecutor(t, executor, dbRepo, s3Client, false)

	task := tasks.Task{
		Type: "backup",
		Vault: entity.Vault{
			Folder: "/tmp/test-vault",
		},
		CustomVars: map[string]string{},
		Job: entity.Job{
			TaskID:       "backup-time-1",
			Type:         "backup",
			Status:       "Queued",
			Vault:        "test-vault",
			CreationTime: "2025-01-01T00:00:00Z",
		},
	}

	before := time.Now().UTC()

	executor.EXPECT().PerformBackup(task.Vault, task.DBs, task.CustomVars).Return(nil)
	gomock.InOrder(
		dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, job entity.Job) error {
				if job.Status != "Processing" {
					t.Errorf("expected status Processing, got %s", job.Status)
				}
				return nil
			}),
		dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, job entity.Job) error {
				ct, err := time.Parse(time.RFC3339Nano, job.CompletionTime)
				if err != nil {
					t.Fatalf("failed to parse CompletionTime: %v", err)
				}
				if ct.Before(before) {
					t.Errorf("CompletionTime %v should be after test start %v", ct, before)
				}
				return nil
			}),
	)

	te.Process(context.Background(), task)
}

// --- TaskPool EnqueueTask tests ---

func TestEnqueueTask_Success(t *testing.T) {
	tp, ch := tasks.NewTaskPoolForTest(2, zap.NewNop().Sugar())

	task := tasks.Task{
		Type: "backup",
		Job:  entity.Job{TaskID: "t-1", Vault: "v1"},
	}
	tp.EnqueueTask(task)

	if tp.QueueSize() != 1 {
		t.Fatalf("expected 1 task in queue, got %d", tp.QueueSize())
	}

	got := <-ch
	if got.Job.TaskID != "t-1" {
		t.Fatalf("expected task t-1, got %s", got.Job.TaskID)
	}
}

func TestEnqueueTask_QueueFull(t *testing.T) {
	tp, _ := tasks.NewTaskPoolForTest(1, zap.NewNop().Sugar())

	tp.EnqueueTask(tasks.Task{Type: "backup", Job: entity.Job{TaskID: "t-1", Vault: "v1"}})
	tp.EnqueueTask(tasks.Task{Type: "backup", Job: entity.Job{TaskID: "t-2", Vault: "v2"}})

	if tp.QueueSize() != 1 {
		t.Fatalf("expected 1 task in queue (second should be dropped), got %d", tp.QueueSize())
	}
}

func TestEnqueueCronBackup_Full(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBD := NewMockBackupDaemonUseCase(ctrl)
	mockBD.EXPECT().EnqueueBackup(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req entity.BackupRequest) (entity.BackupResponse, error) {
			if req.ProcType != "" {
				t.Errorf("expected empty ProcType for full backup, got %s", req.ProcType)
			}
			if len(req.DBs) != 0 {
				t.Errorf("expected no DBs for full backup, got %d", len(req.DBs))
			}
			return entity.BackupResponse{BackupID: "full-1"}, nil
		})

	s := &Scheduler{
		logger:       zap.NewNop().Sugar(),
		backupDaemon: mockBD,
		customVars:   map[string]string{"storageName": "test"},
	}

	s.enqueueCronBackup(context.TODO(), FULL)
}

func TestEnqueueCronBackup_Granular(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBD := NewMockBackupDaemonUseCase(ctrl)
	mockBD.EXPECT().EnqueueBackup(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req entity.BackupRequest) (entity.BackupResponse, error) {
			if len(req.DBs) != 2 {
				t.Errorf("expected 2 DBs for granular backup, got %d", len(req.DBs))
			}
			return entity.BackupResponse{BackupID: "gran-1"}, nil
		})

	s := &Scheduler{
		logger:       zap.NewNop().Sugar(),
		backupDaemon: mockBD,
		scheduledDBs: []string{"db1", "db2"},
		customVars:   map[string]string{},
	}

	s.enqueueCronBackup(context.TODO(), GRANULAR)
}

func TestEnqueueCronBackup_Incremental(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBD := NewMockBackupDaemonUseCase(ctrl)
	mockBD.EXPECT().EnqueueBackup(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req entity.BackupRequest) (entity.BackupResponse, error) {
			if req.ProcType != INCREMENTAL {
				t.Errorf("expected ProcType '%s', got '%s'", INCREMENTAL, req.ProcType)
			}
			return entity.BackupResponse{BackupID: "inc-1"}, nil
		})

	s := &Scheduler{
		logger:       zap.NewNop().Sugar(),
		backupDaemon: mockBD,
		customVars:   map[string]string{},
	}

	s.enqueueCronBackup(context.TODO(), INCREMENTAL)
}

func TestEnqueueCronBackup_NilDaemon(t *testing.T) {
	s := &Scheduler{
		logger:       zap.NewNop().Sugar(),
		backupDaemon: nil,
		customVars:   map[string]string{},
	}

	s.enqueueCronBackup(context.TODO(), FULL)
}

func TestSetBackupDaemon(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBD := NewMockBackupDaemonUseCase(ctrl)

	s := &Scheduler{
		logger: zap.NewNop().Sugar(),
		cron:   cron.New(),
	}

	s.SetBackupDaemon(mockBD)
	if s.backupDaemon == nil {
		t.Error("expected backupDaemon to be set")
	}
}
