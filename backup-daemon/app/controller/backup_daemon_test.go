package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/repo"
	gomock "go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func newTestBackupDaemon(t *testing.T, ctrl *gomock.Controller, s3Enable bool) (
	*BackupDaemon,
	*MockStorageRepository,
	*MockDBRepository,
	*MockSchedulerRepository,
	*MockS3ClientRepository,
	*MockCommandExecutor,
) {
	t.Helper()
	storageRepo := NewMockStorageRepository(ctrl)
	dbRepo := NewMockDBRepository(ctrl)
	scheduler := NewMockSchedulerRepository(ctrl)
	s3Client := NewMockS3ClientRepository(ctrl)
	executor := NewMockCommandExecutor(ctrl)
	logger := zap.NewNop().Sugar()

	bd := &BackupDaemon{
		storageRepo:            storageRepo,
		dbRepo:                 dbRepo,
		scheduler:              scheduler,
		s3Client:               s3Client,
		executor:               executor,
		s3Enable:               s3Enable,
		logger:                 logger,
		evictionPolicy:         "3",
		granularEvictionPolicy: "3",
	}

	return bd, storageRepo, dbRepo, scheduler, s3Client, executor
}

// --- EnqueueBackup tests ---

func TestEnqueueBackup_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, storageRepo, dbRepo, scheduler, _, _ := newTestBackupDaemon(t, ctrl, false)

	storageRepo.EXPECT().OpenVault("", true, false, false, false, "", "", "mybucket/path").
		Return(entity.Vault{Folder: "/storage/20250101T000000"})
	dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).Return(nil)
	scheduler.EXPECT().EnqueueTask(gomock.Any())

	resp, err := bd.EnqueueBackup(context.Background(), entity.BackupRequest{
		AllowEviction: "true",
		CustomVars:    map[string]string{"storageName": "s3", "blob_path": "mybucket/path"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.BackupID != "20250101T000000" {
		t.Errorf("expected backupID '20250101T000000', got '%s'", resp.BackupID)
	}
	if resp.CreationTime == "" {
		t.Error("expected CreationTime to be set")
	}
}

func TestEnqueueBackup_WithDBs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, storageRepo, dbRepo, scheduler, _, _ := newTestBackupDaemon(t, ctrl, false)

	storageRepo.EXPECT().OpenVault(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(entity.Vault{Folder: "/storage/granular/20250101T000000"})
	dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, job entity.Job) error {
			var dbs []string
			_ = json.Unmarshal([]byte(job.Databases), &dbs)
			if len(dbs) != 2 {
				t.Errorf("expected 2 databases in job, got %d", len(dbs))
			}
			return nil
		})
	scheduler.EXPECT().EnqueueTask(gomock.Any())

	_, err := bd.EnqueueBackup(context.Background(), entity.BackupRequest{
		DBs:           []entity.DBEntry{{SimpleName: "db1"}, {SimpleName: "db2"}},
		AllowEviction: "true",
		CustomVars:    map[string]string{"storageName": "s3", "blob_path": "mybucket"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnqueueBackup_InvalidAllowEviction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, _, _, _, _, _ := newTestBackupDaemon(t, ctrl, false)

	_, err := bd.EnqueueBackup(context.Background(), entity.BackupRequest{
		AllowEviction: "invalid",
		CustomVars:    map[string]string{"blob_path": ""},
	})
	if err == nil {
		t.Fatal("expected error for invalid allow eviction")
	}
}

func TestEnqueueBackup_DBUpdateFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, storageRepo, dbRepo, _, _, _ := newTestBackupDaemon(t, ctrl, false)

	storageRepo.EXPECT().OpenVault(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(entity.Vault{Folder: "/storage/20250101T000000"})
	dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).Return(errors.New("db error"))

	_, err := bd.EnqueueBackup(context.Background(), entity.BackupRequest{
		AllowEviction: "true",
		CustomVars:    map[string]string{"blob_path": "mybucket"},
	})
	if err == nil {
		t.Fatal("expected error when DB update fails")
	}
}

// --- GetJobStatus tests ---

func TestGetJobStatus_Successful(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, _, dbRepo, _, _, _ := newTestBackupDaemon(t, ctrl, false)

	dbRepo.EXPECT().SelectEverything(gomock.Any(), "task-1").Return(entity.Job{
		TaskID:         "task-1",
		Type:           "backup",
		Status:         "Successful",
		Vault:          "v1",
		Err:            "",
		StorageName:    "s3",
		BlobPath:       "bucket/path",
		Databases:      `["db1","db2"]`,
		CreationTime:   "2025-01-01T00:00:00Z",
		CompletionTime: "2025-01-01T00:05:00Z",
	}, nil)

	resp, err := bd.GetJobStatus(context.Background(), entity.JobStatusRequest{TaskID: "task-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
	if resp.Status != "Successful" {
		t.Errorf("expected Successful, got %s", resp.Status)
	}
	if len(resp.Databases) != 2 {
		t.Errorf("expected 2 databases, got %d", len(resp.Databases))
	}
	if resp.CompletionTime != "2025-01-01T00:05:00Z" {
		t.Errorf("expected CompletionTime '2025-01-01T00:05:00Z', got '%s'", resp.CompletionTime)
	}
}

func TestGetJobStatus_Failed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, _, dbRepo, _, _, _ := newTestBackupDaemon(t, ctrl, false)

	dbRepo.EXPECT().SelectEverything(gomock.Any(), "task-2").Return(entity.Job{
		TaskID: "task-2",
		Type:   "restore",
		Status: "Failed",
		Err:    "restore script crashed",
	}, nil)

	resp, err := bd.GetJobStatus(context.Background(), entity.JobStatusRequest{TaskID: "task-2"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", resp.StatusCode)
	}
	if resp.Error != "restore script crashed" {
		t.Errorf("expected error message, got %s", resp.Error)
	}
}

func TestGetJobStatus_NotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, _, dbRepo, _, _, _ := newTestBackupDaemon(t, ctrl, false)

	dbRepo.EXPECT().SelectEverything(gomock.Any(), "missing-task").Return(entity.Job{}, repo.ErrNotFound)

	resp, err := bd.GetJobStatus(context.Background(), entity.JobStatusRequest{TaskID: "missing-task"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", resp.StatusCode)
	}
}

func TestGetJobStatus_DBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, _, dbRepo, _, _, _ := newTestBackupDaemon(t, ctrl, false)

	dbRepo.EXPECT().SelectEverything(gomock.Any(), "task-err").Return(entity.Job{}, errors.New("connection lost"))

	_, err := bd.GetJobStatus(context.Background(), entity.JobStatusRequest{TaskID: "task-err"})
	if err == nil {
		t.Fatal("expected error when DB fails")
	}
}

func TestGetJobStatus_WithRestoreDatabases(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, _, dbRepo, _, _, _ := newTestBackupDaemon(t, ctrl, false)

	restoreDBs := []entity.RestoreDBMap{
		{PreviousDatabaseName: "old_db", DatabaseName: "new_db"},
	}
	restoreDBsJSON, _ := json.Marshal(restoreDBs)

	dbRepo.EXPECT().SelectEverything(gomock.Any(), "task-restore").Return(entity.Job{
		TaskID:           "task-restore",
		Type:             "restore",
		Status:           "Successful",
		Databases:        `["new_db"]`,
		RestoreDatabases: string(restoreDBsJSON),
		CompletionTime:   "2025-01-01T01:00:00Z",
	}, nil)

	resp, err := bd.GetJobStatus(context.Background(), entity.JobStatusRequest{TaskID: "task-restore"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.RestoreDatabases) != 1 {
		t.Fatalf("expected 1 RestoreDBMap, got %d", len(resp.RestoreDatabases))
	}
	if resp.CompletionTime != "2025-01-01T01:00:00Z" {
		t.Errorf("expected CompletionTime, got '%s'", resp.CompletionTime)
	}
}

func TestGetJobStatus_Queued(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, _, dbRepo, _, _, _ := newTestBackupDaemon(t, ctrl, false)

	dbRepo.EXPECT().SelectEverything(gomock.Any(), "task-q").Return(entity.Job{
		TaskID: "task-q",
		Type:   "backup",
		Status: "Queued",
	}, nil)

	resp, err := bd.GetJobStatus(context.Background(), entity.JobStatusRequest{TaskID: "task-q"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusPartialContent {
		t.Errorf("expected status 206, got %d", resp.StatusCode)
	}
}

// --- Helper function tests ---

func TestGetBackupAction(t *testing.T) {
	if got := getBackupAction(INCREMENTAL); got != INCREMENTALBACKUP {
		t.Errorf("expected '%s', got '%s'", INCREMENTALBACKUP, got)
	}
	if got := getBackupAction(""); got != COMMONBACKUP {
		t.Errorf("expected '%s', got '%s'", COMMONBACKUP, got)
	}
	if got := getBackupAction(FULL); got != COMMONBACKUP {
		t.Errorf("expected '%s', got '%s'", COMMONBACKUP, got)
	}
}

func TestGetRestoreAction(t *testing.T) {
	if got := getRestoreAction(INCREMENTAL); got != INCREMENTALRESTORE {
		t.Errorf("expected '%s', got '%s'", INCREMENTALRESTORE, got)
	}
	if got := getRestoreAction(""); got != COMMONRESTORE {
		t.Errorf("expected '%s', got '%s'", COMMONRESTORE, got)
	}
}

func TestContains(t *testing.T) {
	list := []string{"a", "b", "c"}
	if !contains(list, "b") {
		t.Error("expected contains to return true for 'b'")
	}
	if contains(list, "d") {
		t.Error("expected contains to return false for 'd'")
	}
	if contains(nil, "a") {
		t.Error("expected contains to return false for nil list")
	}
}

func TestGetTimeCreationNow(t *testing.T) {
	ts := GetTimeCreationNow()
	if ts == "" {
		t.Error("expected non-empty timestamp")
	}
	// Should be parseable as RFC3339Nano
	_, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		t.Errorf("expected valid RFC3339Nano timestamp, got %s: %v", ts, err)
	}
}

// --- RemoveBackup tests ---

func TestRemoveBackup_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, storageRepo, dbRepo, _, _, executor := newTestBackupDaemon(t, ctrl, false)

	storageRepo.EXPECT().GetVault("vault-1", false, "", "", false).Return(entity.Vault{
		Folder:   "/storage/vault-1",
		IsLocked: false,
	})
	dbRepo.EXPECT().SelectEverything(gomock.Any(), "vault-1").Return(entity.Job{}, repo.ErrNotFound)
	executor.EXPECT().ExecuteEvictCmd("/storage/vault-1").Return(nil)
	storageRepo.EXPECT().Evict("/storage/vault-1").Return(nil)
	dbRepo.EXPECT().RemoveVault(gomock.Any(), "vault-1").Return(nil)

	err := bd.RemoveBackup(context.Background(), entity.EvictByVaultRequest{Vault: "vault-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRemoveBackup_SuccessWithS3BlobPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, storageRepo, dbRepo, _, s3Client, executor := newTestBackupDaemon(t, ctrl, false)

	storageRepo.EXPECT().GetVault("vault-1", false, "", "", false).Return(entity.Vault{
		Folder:   "/storage/vault-1",
		IsLocked: false,
	})
	dbRepo.EXPECT().SelectEverything(gomock.Any(), "vault-1").Return(entity.Job{
		TaskID:   "vault-1",
		Vault:    "vault-1",
		BlobPath: "backup-storage/granular",
	}, nil)
	s3Client.EXPECT().DeletePrefix(gomock.Any(), "backup-storage/granular/vault-1").Return(nil)
	executor.EXPECT().ExecuteEvictCmd("/storage/vault-1").Return(nil)
	storageRepo.EXPECT().Evict("/storage/vault-1").Return(nil)
	dbRepo.EXPECT().RemoveVault(gomock.Any(), "vault-1").Return(nil)

	err := bd.RemoveBackup(context.Background(), entity.EvictByVaultRequest{Vault: "vault-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRemoveBackup_SelectMetadataError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, storageRepo, dbRepo, _, _, _ := newTestBackupDaemon(t, ctrl, false)

	storageRepo.EXPECT().GetVault("vault-1", false, "", "", false).Return(entity.Vault{
		Folder:   "/storage/vault-1",
		IsLocked: false,
	})
	dbRepo.EXPECT().SelectEverything(gomock.Any(), "vault-1").Return(entity.Job{}, errors.New("db failure"))

	err := bd.RemoveBackup(context.Background(), entity.EvictByVaultRequest{Vault: "vault-1"})
	if err == nil {
		t.Fatal("expected error when selecting backup metadata fails")
	}
}

func TestRemoveBackup_NotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, storageRepo, _, _, _, _ := newTestBackupDaemon(t, ctrl, false)

	storageRepo.EXPECT().GetVault("missing", false, "", "", false).Return(entity.Vault{})

	err := bd.RemoveBackup(context.Background(), entity.EvictByVaultRequest{Vault: "missing"})
	if err == nil {
		t.Fatal("expected error for missing vault")
	}
}

func TestRemoveBackup_Locked(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, storageRepo, _, _, _, _ := newTestBackupDaemon(t, ctrl, false)

	storageRepo.EXPECT().GetVault("locked-vault", false, "", "", false).Return(entity.Vault{
		Folder:   "/storage/locked-vault",
		IsLocked: true,
	})

	err := bd.RemoveBackup(context.Background(), entity.EvictByVaultRequest{Vault: "locked-vault"})
	if err == nil {
		t.Fatal("expected error for locked vault")
	}
}
