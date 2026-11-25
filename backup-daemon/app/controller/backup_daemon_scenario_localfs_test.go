package controller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/db"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/repo"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/tasks"
	"go.uber.org/zap"
)

// localScenarioEnv wires a BackupDaemon the way production does when S3
// storage is disabled: StorageRepository is backed by the plain local
// filesystem, and no S3 client is involved at all (s3Enable=false, s3Client
// is nil, same as when app.go skips S3 client construction entirely).
type localScenarioEnv struct {
	daemon        BackupDaemonUseCase
	storageRoot   string
	fixtureData   string
	restoreTarget string
}

func newLocalScenarioEnv(t *testing.T) *localScenarioEnv {
	t.Helper()

	storageRoot := filepath.Join(t.TempDir(), "storage")
	storageRepo := repo.NewStorageRepoWithFS(storageRoot, "", "", false, repo.NewLocalFileSystem())

	dbPath := filepath.Join(t.TempDir(), "database.db")
	dbConn, err := db.NewConnection(dbPath)
	if err != nil {
		t.Fatalf("db.NewConnection: %v", err)
	}
	t.Cleanup(func() { _ = dbConn.Close() })
	dbRepo := repo.NewDBRepo(dbConn)

	logger := zap.NewNop().Sugar()

	fixtureData := "payload-" + safeName(t.Name())
	fixtureFile := filepath.Join(t.TempDir(), "seed.txt")
	if err := os.WriteFile(fixtureFile, []byte(fixtureData), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	restoreTarget := filepath.Join(t.TempDir(), "restored.txt")

	backupCmd := fmt.Sprintf("cp %s {{.data_folder}}/dump.txt", fixtureFile)
	restoreCmd := fmt.Sprintf("cp {{.data_folder}}/dump.txt %s", restoreTarget)

	executor, err := tasks.NewExecutor(
		"",        // evictCmdTemplate
		backupCmd,
		restoreCmd,
		"",                  // dbListCmdTemplate (not needed for a full/non-granular backup)
		map[string]string{}, // customVars
		"-d",                // databasesKey
		"-m",                // dbmapKey
		storageRepo,
		dbRepo,
		"", // evictionPolicy: no automatic eviction so the fresh backup survives PerformEviction()
		"", // granularEvictionPolicy
		logger,
		"", // markerSetCmdTemplate
		"", // markerGetCmdTemplate
	)
	if err != nil {
		t.Fatalf("NewExecutor: %v", err)
	}

	tpCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// s3Client is nil and s3Enable is false: no S3 code path should ever be exercised.
	taskPool := tasks.NewTaskPool(tpCtx, 10, executor, executor, dbRepo, nil, false, nil, logger)

	daemon := NewBackupDaemon(storageRepo, dbRepo, taskPool, nil, executor, false, logger)

	return &localScenarioEnv{
		daemon:        daemon,
		storageRoot:   storageRoot,
		fixtureData:   fixtureData,
		restoreTarget: restoreTarget,
	}
}

// TestLocalFS_FullBackupRestoreEvictLifecycle mirrors
// TestIntegration_FullBackupRestoreEvictLifecycle but with S3Enable=false,
// confirming the same backup/list/restore/evict lifecycle works against plain
// local disk storage — no Docker/MinIO required, runs as a normal unit test.
func TestLocalFS_FullBackupRestoreEvictLifecycle(t *testing.T) {
	env := newLocalScenarioEnv(t)
	ctx := context.Background()

	// 1. Create a backup and wait for the async task pool to finish it.
	backupResp, err := env.daemon.EnqueueBackup(ctx, entity.BackupRequest{ProcType: "full"})
	if err != nil {
		t.Fatalf("EnqueueBackup: %v", err)
	}
	backupID := backupResp.BackupID
	if backupID == "" {
		t.Fatalf("EnqueueBackup returned empty BackupID")
	}

	backupStatus := waitForJob(t, env.daemon, backupID)
	if backupStatus.Status != "Successful" {
		t.Fatalf("backup job did not succeed: status=%s err=%s", backupStatus.Status, backupStatus.Error)
	}

	// With S3 disabled, the backup must stay on local disk (no upload, no removal).
	vaultDir := filepath.Join(env.storageRoot, backupID)
	if _, err := os.Stat(filepath.Join(vaultDir, "dump.txt")); err != nil {
		t.Fatalf("expected backup data on local disk at %s: %v", vaultDir, err)
	}

	// 2. listBackups: the new backup must show up, both in the name list and
	// in the per-vault stats endpoint.
	names, err := env.daemon.ListBackups(ctx, "full")
	if err != nil {
		t.Fatalf("ListBackups: %v", err)
	}
	if !containsStr(names, backupID) {
		t.Fatalf("expected ListBackups to contain %s, got %v", backupID, names)
	}

	stats, err := env.daemon.ListBackup(ctx, "full", backupID)
	if err != nil {
		t.Fatalf("ListBackup: %v", err)
	}
	if valid, _ := stats["valid"].(bool); !valid {
		t.Fatalf("expected backup %s to be valid, got stats=%+v", backupID, stats)
	}
	if evictable, _ := stats["evictable"].(bool); !evictable {
		t.Fatalf("expected backup %s to be evictable, got stats=%+v", backupID, stats)
	}

	// 3. Restore the backup and verify the restore command ran against the
	// vault folder directly (no S3 download needed/performed).
	restoreResp, err := env.daemon.RestoreBackup(ctx, entity.RestoreRequest{Vault: backupID, ProcType: "full"})
	if err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}

	restoreStatus := waitForJob(t, env.daemon, restoreResp.TaskID)
	if restoreStatus.Status != "Successful" {
		t.Fatalf("restore job did not succeed: status=%s err=%s", restoreStatus.Status, restoreStatus.Error)
	}

	restoredBytes, err := os.ReadFile(env.restoreTarget)
	if err != nil {
		t.Fatalf("reading restored file: %v", err)
	}
	if string(restoredBytes) != env.fixtureData {
		t.Fatalf("restored content = %q, want %q", string(restoredBytes), env.fixtureData)
	}

	// 4. Evict the backup and verify it disappears both from the daemon's view
	// and from local disk.
	if err := env.daemon.RemoveBackupV2(ctx, entity.EvictByVaultV2Request{Vault: backupID}); err != nil {
		t.Fatalf("RemoveBackupV2: %v", err)
	}

	names, err = env.daemon.ListBackups(ctx, "full")
	if err != nil {
		t.Fatalf("ListBackups after evict: %v", err)
	}
	if containsStr(names, backupID) {
		t.Fatalf("expected %s to be evicted, still present in %v", backupID, names)
	}

	if _, err := os.Stat(vaultDir); !os.IsNotExist(err) {
		t.Fatalf("expected local vault dir %s to be removed, stat err=%v", vaultDir, err)
	}

	// Restoring an evicted backup must fail cleanly.
	if _, err := env.daemon.RestoreBackup(ctx, entity.RestoreRequest{Vault: backupID, ProcType: "full"}); !errors.Is(err, ErrVaultNotFound) {
		t.Fatalf("expected ErrVaultNotFound restoring evicted backup, got: %v", err)
	}
}
