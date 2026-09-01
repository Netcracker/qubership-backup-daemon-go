package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/tasks"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/utils"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/repo"

	gomock "go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func newTestBackupDaemon(t *testing.T, ctrl *gomock.Controller, s3Enable bool) (
	*BackupDaemon,
	*repo.MockStorageRepository,
	*MockDBRepository,
	*MockTaskPoolRepository,
	*utils.MockS3ClientRepository,
	*tasks.MockCommandExecutor,
) {
	t.Helper()
	storageRepo := repo.NewMockStorageRepository(ctrl)
	dbRepo := NewMockDBRepository(ctrl)
	taskPool := NewMockTaskPoolRepository(ctrl)
	s3Client := utils.NewMockS3ClientRepository(ctrl)
	executor := tasks.NewMockCommandExecutor(ctrl)
	logger := zap.NewNop().Sugar()

	bd := &BackupDaemon{
		storageRepo: storageRepo,
		dbRepo:      dbRepo,
		taskPool:    taskPool,
		s3Client:    s3Client,
		executor:    executor,
		s3Enable:    s3Enable,
		logger:      logger,
	}
	bd.resolveRestoreVault = bd.resolveRestoreVaultDefault

	return bd, storageRepo, dbRepo, taskPool, s3Client, executor
}

// --- EnqueueBackup tests ---

func TestEnqueueBackup_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, storageRepo, dbRepo, taskPool, _, _ := newTestBackupDaemon(t, ctrl, false)

	storageRepo.EXPECT().OpenVault("", true, false, false, false, "", "", "mybucket/path").
		Return(entity.Vault{Folder: "/storage/20250101T000000"}, nil)
	dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).Return(nil)
	taskPool.EXPECT().EnqueueTask(gomock.Any())

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

	bd, storageRepo, dbRepo, taskPool, _, _ := newTestBackupDaemon(t, ctrl, false)

	storageRepo.EXPECT().OpenVault(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(entity.Vault{Folder: "/storage/granular/20250101T000000"}, nil)
	dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, job entity.Job) error {
			var dbs []string
			_ = json.Unmarshal([]byte(job.Databases), &dbs)
			if len(dbs) != 2 {
				t.Errorf("expected 2 databases in job, got %d", len(dbs))
			}
			return nil
		})
	taskPool.EXPECT().EnqueueTask(gomock.Any())

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
		Return(entity.Vault{Folder: "/storage/20250101T000000"}, nil)
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

// TestRemoveBackup_MultipleAliases_RoutesToCorrectClient guards against alias
// cross-contamination: when several S3 aliases are configured, evicting a
// vault must use the S3 client for the alias that vault was actually
// uploaded to (persisted as job.StorageName), never a different alias's
// client. Each alias's mock client here has an EXPECT() set for its own
// vault only, so a misrouted DeletePrefix call fails the test via gomock's
// unexpected-call panic, not just a wrong-value assertion.
func TestRemoveBackup_MultipleAliases_RoutesToCorrectClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, storageRepo, dbRepo, _, _, executor := newTestBackupDaemon(t, ctrl, true)

	fullBackupsClient := utils.NewMockS3ClientRepository(ctrl)
	archiveClient := utils.NewMockS3ClientRepository(ctrl)
	bd.s3Registry = utils.NewS3AliasRegistry(map[string]utils.S3ClientRepository{
		"full-backups": fullBackupsClient,
		"archive":      archiveClient,
	})

	cases := []struct {
		vault       string
		storageName string
		client      *utils.MockS3ClientRepository
	}{
		{vault: "vault-full-1", storageName: "full-backups", client: fullBackupsClient},
		{vault: "vault-archive-1", storageName: "archive", client: archiveClient},
	}

	for _, tc := range cases {
		folder := "/storage/" + tc.vault
		storageRepo.EXPECT().GetVault(tc.vault, false, "", "", false).Return(entity.Vault{Folder: folder})
		dbRepo.EXPECT().SelectEverything(gomock.Any(), tc.vault).Return(entity.Job{
			Vault:       tc.vault,
			StorageName: tc.storageName,
			BlobPath:    "backup-storage/granular",
		}, nil)
		tc.client.EXPECT().DeletePrefix(gomock.Any(), "backup-storage/granular/"+tc.vault).Return(nil)
		executor.EXPECT().ExecuteEvictCmd(folder).Return(nil)
		storageRepo.EXPECT().Evict(folder).Return(nil)
		dbRepo.EXPECT().RemoveVault(gomock.Any(), tc.vault).Return(nil)

		if err := bd.RemoveBackup(context.Background(), entity.EvictByVaultRequest{Vault: tc.vault}); err != nil {
			t.Fatalf("RemoveBackup(%s): unexpected error: %v", tc.vault, err)
		}
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

func TestGetBackupStats_GranularS3Fallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, storageRepo, dbRepo, _, s3Client, executor := newTestBackupDaemon(t, ctrl, true)

	const vaultName = "20260730T061234"
	const vaultFolder = "backup-storage/granular/" + vaultName

	storageRepo.EXPECT().ListVaultNames(false, "all", "").Return([]string{vaultName}, nil)
	storageRepo.EXPECT().GetVault(vaultName, false, "", "", false).Return(entity.Vault{
		Folder:     vaultFolder,
		IsGranular: true,
	})
	storageRepo.EXPECT().LoadMetrics(gomock.Any()).Return(map[string]interface{}{}, nil)
	executor.EXPECT().GetBackupDBs(vaultFolder).Return(nil, errors.New("exit status 1"))
	// Best-effort DB lookup: no BlobPath → legacy S3 mode uses vault folder as prefix.
	dbRepo.EXPECT().SelectEverything(gomock.Any(), vaultName).Return(entity.Job{}, nil)
	s3Client.EXPECT().ListCommonPrefixes(gomock.Any(), vaultFolder).Return([]string{
		vaultFolder + "/db1/",
		vaultFolder + "/db2/",
	}, nil)
	storageRepo.EXPECT().HasCustomVars(gomock.Any()).Return(false)

	result, err := bd.GetBackupStats(context.Background(), vaultName, "", "", "granular")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dbList, ok := result["db_list"]
	if !ok {
		t.Fatal("expected db_list in result")
	}
	names, ok := dbList.([]string)
	if !ok {
		t.Fatalf("expected db_list to be []string, got %T", dbList)
	}
	if len(names) != 2 || names[0] != "db1" || names[1] != "db2" {
		t.Fatalf("unexpected db_list: %v", names)
	}
}

// --- S3 support tests for old '/' API ---

func TestDownloadBackup_Local_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, storageRepo, dbRepo, _, _, _ := newTestBackupDaemon(t, ctrl, false)

	storageRepo.EXPECT().GetVault("vault-1", false, "", "", false).Return(entity.Vault{Folder: "/storage/vault-1"})
	dbRepo.EXPECT().SelectEverything(gomock.Any(), "vault-1").Return(entity.Job{}, repo.ErrNotFound)

	folder, cleanup, err := bd.DownloadBackup(context.Background(), "vault-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cleanup()
	if folder != "/storage/vault-1" {
		t.Fatalf("expected /storage/vault-1, got %s", folder)
	}
}

func TestDownloadBackup_S3Legacy_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, storageRepo, dbRepo, _, s3Client, _ := newTestBackupDaemon(t, ctrl, true)

	storageRepo.EXPECT().GetVault("vault-1", false, "", "", false).Return(entity.Vault{Folder: "/storage/vault-1"})
	dbRepo.EXPECT().SelectEverything(gomock.Any(), "vault-1").Return(entity.Job{}, repo.ErrNotFound)
	// Legacy S3: prefix is the vault folder path with leading slash stripped.
	s3Client.EXPECT().DownloadFolder(gomock.Any(), "storage/vault-1", gomock.Any()).Return(nil)

	folder, cleanup, err := bd.DownloadBackup(context.Background(), "vault-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cleanup()
	if folder == "" || folder == "/storage/vault-1" {
		t.Fatalf("expected a temp dir, got %q", folder)
	}
}

func TestDownloadBackup_S3BlobPath_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, storageRepo, dbRepo, _, s3Client, _ := newTestBackupDaemon(t, ctrl, false)

	storageRepo.EXPECT().GetVault("vault-1", false, "", "", false).Return(entity.Vault{Folder: "/storage/vault-1"})
	dbRepo.EXPECT().SelectEverything(gomock.Any(), "vault-1").Return(entity.Job{
		BlobPath: "backup-storage/granular",
	}, nil)
	// blob_path mode: prefix is path.Join(blobPath, backupID).
	s3Client.EXPECT().DownloadFolder(gomock.Any(), "backup-storage/granular/vault-1", gomock.Any()).Return(nil)

	folder, cleanup, err := bd.DownloadBackup(context.Background(), "vault-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cleanup()
	if folder == "" || folder == "/storage/vault-1" {
		t.Fatalf("expected a temp dir, got %q", folder)
	}
}

func TestDownloadBackup_NotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, storageRepo, _, _, _, _ := newTestBackupDaemon(t, ctrl, false)
	storageRepo.EXPECT().GetVault("missing", false, "", "", false).Return(entity.Vault{})

	_, _, err := bd.DownloadBackup(context.Background(), "missing")
	if !errors.Is(err, ErrVaultNotFound) {
		t.Fatalf("expected ErrVaultNotFound, got %v", err)
	}
}

func TestCreateS3PresignedURL_LegacyS3(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, storageRepo, dbRepo, _, s3Client, _ := newTestBackupDaemon(t, ctrl, true)

	storageRepo.EXPECT().GetVault("vault-1", false, "", "", false).Return(entity.Vault{Folder: "/storage/vault-1"})
	dbRepo.EXPECT().SelectEverything(gomock.Any(), "vault-1").Return(entity.Job{}, repo.ErrNotFound)
	// Legacy S3: prefix is stripped vault folder.
	s3Client.EXPECT().ListFiles(gomock.Any(), "storage/vault-1").Return([]string{"storage/vault-1/db.tar.gz"}, nil)
	s3Client.EXPECT().CreatePresignedUrl(gomock.Any(), "storage/vault-1/db.tar.gz", gomock.Any()).Return("https://example.com/presigned", nil)

	resp, err := bd.CreateS3PresignedURL(context.Background(), entity.S3PresignedURLRequest{
		BackupID:   "vault-1",
		Expiration: 3600,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Urls) != 1 || resp.Urls[0] != "https://example.com/presigned" {
		t.Fatalf("unexpected urls: %v", resp.Urls)
	}
}

func TestRemoveBackup_S3Enable_NoBlobPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, storageRepo, dbRepo, _, s3Client, executor := newTestBackupDaemon(t, ctrl, true)

	storageRepo.EXPECT().GetVault("vault-1", false, "", "", false).Return(entity.Vault{
		Folder:   "/storage/vault-1",
		IsLocked: false,
	})
	dbRepo.EXPECT().SelectEverything(gomock.Any(), "vault-1").Return(entity.Job{}, repo.ErrNotFound)
	// Legacy S3: prefix is stripped vault folder path.
	s3Client.EXPECT().DeletePrefix(gomock.Any(), "storage/vault-1").Return(nil)
	executor.EXPECT().ExecuteEvictCmd("/storage/vault-1").Return(nil)
	storageRepo.EXPECT().Evict("/storage/vault-1").Return(nil)
	dbRepo.EXPECT().RemoveVault(gomock.Any(), "vault-1").Return(nil)

	err := bd.RemoveBackup(context.Background(), entity.EvictByVaultRequest{Vault: "vault-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRestoreBackup_S3Legacy_Downloads(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, storageRepo, dbRepo, taskPool, s3Client, _ := newTestBackupDaemon(t, ctrl, true)

	// Use a real temp dir so os.ReadDir succeeds after the mock "download".
	vaultFolder := t.TempDir()
	storageRepo.EXPECT().GetVault("vault-1", false, "", "", false).Return(entity.Vault{Folder: vaultFolder})

	// Legacy S3: DownloadFolder is called with (vaultFolder, "") and must create a file so ReadDir passes.
	s3Client.EXPECT().DownloadFolder(gomock.Any(), vaultFolder, "").
		DoAndReturn(func(_ context.Context, _, _ string) error {
			return os.WriteFile(vaultFolder+"/db.tar.gz", []byte("data"), 0o644)
		})

	// RestoreBackup calls UpdateJob twice: before vault resolution and after.
	dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).Return(nil).Times(2)
	taskPool.EXPECT().EnqueueTask(gomock.Any())

	_, err := bd.RestoreBackup(context.Background(), entity.RestoreRequest{
		Vault: "vault-1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRestoreBackup_S3BlobPath_Downloads(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storageRoot := t.TempDir()
	bd, storageRepo, dbRepo, taskPool, s3Client, _ := newTestBackupDaemon(t, ctrl, false)
	storageRepo.EXPECT().GetRoot().Return(storageRoot)

	storageRepo.EXPECT().GetVault("vault-1", false, "", "bkp/granular", false).Return(entity.Vault{
		Folder: storageRoot + "/s3-processing/vault-1",
	})

	// blob_path mode: DownloadFolder downloads to the s3-processing staging dir.
	// The mock writes a placeholder file so os.ReadDir passes.
	s3Client.EXPECT().DownloadFolder(gomock.Any(), "bkp/granular/vault-1", gomock.Any()).
		DoAndReturn(func(_ context.Context, _, localDir string) error {
			return os.WriteFile(localDir+"/db.tar.gz", []byte("data"), 0o644)
		})

	// RestoreBackup calls UpdateJob twice: before vault resolution and after.
	dbRepo.EXPECT().UpdateJob(gomock.Any(), gomock.Any()).Return(nil).Times(2)
	taskPool.EXPECT().EnqueueTask(gomock.Any())

	_, err := bd.RestoreBackup(context.Background(), entity.RestoreRequest{
		Vault: "vault-1",
		CustomVars: map[string]string{
			"blob_path": "bkp/granular",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetBackupStats_GranularS3_BlobPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bd, storageRepo, dbRepo, _, s3Client, executor := newTestBackupDaemon(t, ctrl, false)

	const vaultName = "20260730T061234"
	const blobPath = "backup-storage/granular"
	const s3Prefix = blobPath + "/" + vaultName

	storageRepo.EXPECT().ListVaultNames(false, "all", "").Return([]string{vaultName}, nil)
	storageRepo.EXPECT().GetVault(vaultName, false, "", "", false).Return(entity.Vault{
		Folder:     "/storage/" + vaultName,
		IsGranular: true,
	})
	storageRepo.EXPECT().LoadMetrics(gomock.Any()).Return(map[string]interface{}{}, nil)
	executor.EXPECT().GetBackupDBs("/storage/" + vaultName).Return(nil, errors.New("not found"))
	dbRepo.EXPECT().SelectEverything(gomock.Any(), vaultName).Return(entity.Job{
		BlobPath: blobPath,
	}, nil)
	s3Client.EXPECT().ListCommonPrefixes(gomock.Any(), s3Prefix).Return([]string{
		s3Prefix + "/db1/",
		s3Prefix + "/db2/",
	}, nil)
	storageRepo.EXPECT().HasCustomVars(gomock.Any()).Return(false)

	result, err := bd.GetBackupStats(context.Background(), vaultName, "", "", "granular")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	names, ok := result["db_list"].([]string)
	if !ok || len(names) != 2 || names[0] != "db1" || names[1] != "db2" {
		t.Fatalf("unexpected db_list: %v", result["db_list"])
	}
}
