//go:build integration

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
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/utils"
	"go.uber.org/zap"
)

// scenarioEnv wires a BackupDaemon exactly like production does when S3 storage
// is enabled (see app/app/app.go): StorageRepository is backed directly by
// S3FileSystem pointed at MinIO, backup/restore commands run for real via the
// Executor, and jobs are processed asynchronously by a real TaskPool.
type scenarioEnv struct {
	daemon        BackupDaemonUseCase
	s3Client      utils.S3ClientRepository
	storageRoot   string
	fixtureData   string
	restoreTarget string
}

func scenarioGetenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// newScenarioEnv builds a fresh BackupDaemon backed by a real MinIO bucket
// (configured via MINIO_* env vars, same as scripts/test-s3-integration.sh)
// and a real (temp-file) SQLite job database.
func newScenarioEnv(t *testing.T) *scenarioEnv {
	t.Helper()
	ctx := context.Background()

	url := scenarioGetenv("MINIO_URL", "http://localhost:9000")
	key := scenarioGetenv("MINIO_ACCESS_KEY", "minioadmin")
	secret := scenarioGetenv("MINIO_SECRET_KEY", "minioadmin")
	bucket := scenarioGetenv("MINIO_BUCKET", "test-bucket")
	region := scenarioGetenv("MINIO_REGION", "us-east-1")

	s3Client, err := utils.NewS3Client(ctx, url, key, secret, bucket, region, true, "")
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}

	// storageRoot doubles as both the real local path used while commands run
	// and (with the leading "/" trimmed) the S3 key prefix under which the
	// backup ends up living — this mirrors how S3-enabled production works.
	storageRoot := filepath.Join(t.TempDir(), "storage", "integration-tests", safeName(t.Name()))
	fs := repo.NewS3FileSystem(ctx, s3Client, bucket)
	storageRepo := repo.NewStorageRepoWithFS(storageRoot, "", "", false, fs)
	t.Cleanup(func() {
		_ = s3Client.DeletePrefix(ctx, storageRoot)
	})

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

	// Commands are split on whitespace (no shell), so keep them argv-simple:
	// "cp <src> <dst>" with no embedded quoting/redirection.
	backupCmd := fmt.Sprintf("cp %s {{.data_folder}}/dump.txt", fixtureFile)
	restoreCmd := fmt.Sprintf("cp {{.data_folder}}/dump.txt %s", restoreTarget)

	executor, err := tasks.NewExecutor(
		"",        // evictCmdTemplate
		backupCmd,
		restoreCmd,
		"",        // dbListCmdTemplate (not needed for a full/non-granular backup)
		map[string]string{}, // customVars
		"-d",      // databasesKey
		"-m",      // dbmapKey
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
	taskPool := tasks.NewTaskPool(tpCtx, 10, executor, executor, dbRepo, s3Client, true, nil, logger)

	daemon := NewBackupDaemon(storageRepo, dbRepo, taskPool, s3Client, executor, true, logger)

	return &scenarioEnv{
		daemon:        daemon,
		s3Client:      s3Client,
		storageRoot:   storageRoot,
		fixtureData:   fixtureData,
		restoreTarget: restoreTarget,
	}
}

// TestIntegration_FullBackupRestoreEvictLifecycle exercises the whole daemon
// lifecycle — backup creation, listing, restore, and eviction — against a real
// MinIO bucket the same way the daemon is wired in S3-enabled production mode.
func TestIntegration_FullBackupRestoreEvictLifecycle(t *testing.T) {
	env := newScenarioEnv(t)
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

	// The uploaded data must be visible directly in MinIO, proving the backup
	// really left local disk and landed in S3 (moveBackupToS3 runs before the
	// job is marked Successful).
	files, err := env.s3Client.ListFiles(ctx, filepath.Join(env.storageRoot, backupID))
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("expected backup files in S3 under %s, got none", filepath.Join(env.storageRoot, backupID))
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

	// 3. Restore the backup and verify the restore command actually ran
	// against data downloaded back from S3.
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
	// and from MinIO itself.
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

	exists, err := env.s3Client.PrefixExists(ctx, filepath.Join(env.storageRoot, backupID))
	if err != nil {
		t.Fatalf("PrefixExists after evict: %v", err)
	}
	if exists {
		t.Fatalf("expected backup %s to be removed from S3, prefix still exists", backupID)
	}

	// Restoring an evicted backup must fail cleanly.
	if _, err := env.daemon.RestoreBackup(ctx, entity.RestoreRequest{Vault: backupID, ProcType: "full"}); !errors.Is(err, ErrVaultNotFound) {
		t.Fatalf("expected ErrVaultNotFound restoring evicted backup, got: %v", err)
	}
}

// TestIntegration_GranularBackupWithS3AliasesUsesDefaultAliasClient
// reproduces the CPCAP-11911 production bug end-to-end against a real MinIO
// endpoint, so the actual AWS SDK credential-resolution code path (the one
// that produced "static credentials are empty" in production) is exercised
// instead of a mock.
//
// It mirrors app.go's wiring whenever S3 aliases are configured
// (app/app/app.go:118-137): the alias registry holds only alias-name keys
// (here just "default", with real working credentials) and never an explicit
// "" entry, while the top-level (non-alias) s3Client is built with
// deliberately blank credentials -- exactly what happens in an alias-only
// deployment where cfg.AccessKeyID/AccessKeySecret are left unset because
// real credentials live solely in the alias config.
//
// The backup request carries no CustomVars["storageName"], matching an
// internally-scheduled granular backup (rest/helperV2.go's
// normalizeStorageName, which defaults empty storageName to "default", only
// runs for REST-driven requests). Before the resolveS3Client fix this task
// would fall back to the blank-credential top-level client and fail; it must
// now resolve through the registry and land in S3 via the "default" alias.
func TestIntegration_GranularBackupWithS3AliasesUsesDefaultAliasClient(t *testing.T) {
	ctx := context.Background()

	url := scenarioGetenv("MINIO_URL", "http://localhost:9000")
	bucket := scenarioGetenv("MINIO_BUCKET", "test-bucket")
	region := scenarioGetenv("MINIO_REGION", "us-east-1")

	// The "default" alias: real, working credentials -- mirrors the per-alias
	// client app.go builds from cfg.S3Aliases["default"].
	defaultAliasClient, err := utils.NewS3Client(ctx, url,
		scenarioGetenv("MINIO_ACCESS_KEY", "minioadmin"),
		scenarioGetenv("MINIO_SECRET_KEY", "minioadmin"),
		bucket, region, true, "")
	if err != nil {
		t.Fatalf("NewS3Client (default alias): %v", err)
	}

	// The top-level client: deliberately blank credentials, mirroring an
	// alias-only deployment where cfg.AccessKeyID/AccessKeySecret are unset.
	brokenTopLevelClient, err := utils.NewS3Client(ctx, url, "", "", bucket, region, true, "")
	if err != nil {
		t.Fatalf("NewS3Client (broken top-level): %v", err)
	}

	// Mirrors app.go:118-130: the registry holds only alias-name keys, never
	// an explicit "" entry.
	registry := utils.NewS3AliasRegistry(map[string]utils.S3ClientRepository{
		"default": defaultAliasClient,
	})

	storageRoot := filepath.Join(t.TempDir(), "storage", "integration-tests", safeName(t.Name()))
	storageRepo := repo.NewStorageRepo(storageRoot, "", "", false)
	t.Cleanup(func() {
		_ = defaultAliasClient.DeletePrefix(ctx, storageRoot)
	})

	dbPath := filepath.Join(t.TempDir(), "database.db")
	dbConn, err := db.NewConnection(dbPath)
	if err != nil {
		t.Fatalf("db.NewConnection: %v", err)
	}
	t.Cleanup(func() { _ = dbConn.Close() })
	dbRepo := repo.NewDBRepo(dbConn)

	logger := zap.NewNop().Sugar()

	fixtureFile := filepath.Join(t.TempDir(), "seed.txt")
	if err := os.WriteFile(fixtureFile, []byte("payload"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	backupCmd := fmt.Sprintf("cp %s {{.data_folder}}/dump.txt", fixtureFile)

	executor, err := tasks.NewExecutor(
		"",        // evictCmdTemplate
		backupCmd,
		"",        // restoreCmdTemplate (not needed for this test)
		"",        // dbListCmdTemplate
		map[string]string{}, // customVars
		"-d",      // databasesKey
		"-m",      // dbmapKey
		storageRepo,
		dbRepo,
		"", // evictionPolicy
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
	// s3Client is the broken top-level client; s3Registry only knows "default".
	// A granular/scheduled backup (no storageName) must resolve through the
	// registry, not fall back to the broken client.
	taskPool := tasks.NewTaskPool(tpCtx, 10, executor, executor, dbRepo, brokenTopLevelClient, true, registry, logger)

	// Built directly (rather than via NewBackupDaemon, which has no parameter
	// for s3Registry) so the daemon's own resolveS3Client also sees the
	// alias-only registry, matching how app.go wires BackupDaemon in
	// production.
	daemonImpl := &BackupDaemon{
		storageRepo: storageRepo,
		dbRepo:      dbRepo,
		taskPool:    taskPool,
		s3Client:    brokenTopLevelClient,
		s3Registry:  registry,
		executor:    executor,
		s3Enable:    true,
		logger:      logger,
	}
	daemonImpl.resolveRestoreVault = daemonImpl.resolveRestoreVaultDefault
	var daemon BackupDaemonUseCase = daemonImpl

	// EnqueueBackup with no CustomVars mirrors the internal/scheduled granular
	// backup path, which never goes through handlerV2.normalizeStorageName.
	backupResp, err := daemon.EnqueueBackup(ctx, entity.BackupRequest{ProcType: "full"})
	if err != nil {
		t.Fatalf("EnqueueBackup: %v", err)
	}
	backupID := backupResp.BackupID
	if backupID == "" {
		t.Fatalf("EnqueueBackup returned empty BackupID")
	}

	status := waitForJob(t, daemon, backupID)
	if status.Status != "Successful" {
		t.Fatalf("backup job did not succeed (would fail with \"static credentials are empty\" "+
			"if resolveS3Client fell back to the broken top-level client): status=%s err=%s",
			status.Status, status.Error)
	}

	// Confirm the upload actually landed via the "default" alias client.
	files, err := defaultAliasClient.ListFiles(ctx, storageRoot)
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("expected backup files in S3 under %s via the default alias, found none", storageRoot)
	}
}
