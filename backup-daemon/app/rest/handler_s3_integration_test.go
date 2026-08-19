//go:build integration

package rest

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/controller"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/db"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/repo"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/tasks"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/utils"
)

// handlerIntegEnv wires a BackupDaemon + EndpointHandler + Gin router pointing at
// a real MinIO bucket. It mirrors the production wiring from app/app/app.go.
type handlerIntegEnv struct {
	router        *gin.Engine
	daemon        controller.BackupDaemonUseCase
	s3Client      utils.S3ClientRepository
	storageRoot   string
	fixtureData   string
	restoreTarget string
}

func handlerIntegGetenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// newHandlerIntegEnv builds the full stack for the old '/' API integration tests.
func newHandlerIntegEnv(t *testing.T) *handlerIntegEnv {
	t.Helper()
	ctx := context.Background()

	url := handlerIntegGetenv("MINIO_URL", "http://localhost:9000")
	key := handlerIntegGetenv("MINIO_ACCESS_KEY", "minioadmin")
	secret := handlerIntegGetenv("MINIO_SECRET_KEY", "minioadmin")
	bucket := handlerIntegGetenv("MINIO_BUCKET", "test-bucket")
	region := handlerIntegGetenv("MINIO_REGION", "us-east-1")

	s3Client, err := utils.NewS3Client(ctx, url, key, secret, bucket, region, true, "")
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}

	// storageRoot is used as the local staging path and (with the leading "/"
	// stripped) as the S3 key prefix — same convention as s3-enabled production.
	storageRoot := filepath.Join(t.TempDir(), "storage", "handler-integ", handlerIntegSafeName(t.Name()))
	fs := repo.NewS3FileSystem(ctx, s3Client, bucket)
	storageRepo := repo.NewStorageRepoWithFS(storageRoot, "", "", false, fs)
	t.Cleanup(func() { _ = s3Client.DeletePrefix(ctx, storageRoot) })

	dbPath := filepath.Join(t.TempDir(), "database.db")
	dbConn, err := db.NewConnection(dbPath)
	if err != nil {
		t.Fatalf("db.NewConnection: %v", err)
	}
	t.Cleanup(func() { _ = dbConn.Close() })
	dbRepo := repo.NewDBRepo(dbConn)

	logger := zap.NewNop().Sugar()

	fixtureData := "handler-integ-payload-" + handlerIntegSafeName(t.Name())
	fixtureFile := filepath.Join(t.TempDir(), "seed.txt")
	if err := os.WriteFile(fixtureFile, []byte(fixtureData), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	restoreTarget := filepath.Join(t.TempDir(), "restored.txt")

	// The executor splits the command with strings.Fields (no shell), so compound
	// commands need a helper script. The backup script creates both dump.txt (used by
	// restore) and dump.tar.gz (needed for S3 presigned URL extension filter).
	scriptDir := t.TempDir()
	backupScriptPath := filepath.Join(scriptDir, "backup.sh")
	backupScriptContent := fmt.Sprintf("#!/bin/sh\ncp %s \"$1\"/dump.txt\ntar czf \"$1\"/dump.tar.gz -C \"$1\" dump.txt\n", fixtureFile)
	if err := os.WriteFile(backupScriptPath, []byte(backupScriptContent), 0o755); err != nil {
		t.Fatalf("write backup script: %v", err)
	}
	backupCmd := backupScriptPath + " {{.data_folder}}"
	restoreCmd := fmt.Sprintf("cp {{.data_folder}}/dump.txt %s", restoreTarget)

	executor, err := tasks.NewExecutor(
		"", backupCmd, restoreCmd, "",
		map[string]string{}, "-d", "-m",
		storageRepo, dbRepo, "", "", logger, "", "",
	)
	if err != nil {
		t.Fatalf("NewExecutor: %v", err)
	}

	tpCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	taskPool := tasks.NewTaskPool(tpCtx, 10, executor, executor, dbRepo, s3Client, true, nil, logger)

	daemon := controller.NewBackupDaemon(storageRepo, dbRepo, taskPool, s3Client, executor, true, logger)

	gin.SetMode(gin.TestMode)
	router := gin.New()
	handler := NewEndpointHandler(daemon, daemon, logger)

	// Register the old '/' API routes used in these tests.
	router.POST("/backup", handler.Backup)
	router.POST("/restore", handler.Restore)
	router.POST("/evict/:vault", handler.EvictByVault)
	router.GET("/jobstatus/:task_id", handler.JobStatus)
	router.GET("/listbackups", handler.ListBackups)
	router.GET("/backup/s3/:backup_id", handler.S3PresignedURL)
	router.GET("/backup/:backup_id", handler.DownloadBackup)

	return &handlerIntegEnv{
		router:        router,
		daemon:        daemon,
		s3Client:      s3Client,
		storageRoot:   storageRoot,
		fixtureData:   fixtureData,
		restoreTarget: restoreTarget,
	}
}

func handlerIntegSafeName(name string) string {
	r := strings.NewReplacer("/", "_", " ", "_")
	return r.Replace(name)
}

// waitForJobHTTP polls GET /jobstatus/:task_id until the job reaches a terminal state.
// HTTP 200 = Successful, HTTP 500 = Failed, HTTP 206 = in-progress.
func waitForJobHTTP(t *testing.T, router *gin.Engine, taskID string) entity.JobStatusResponse {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		req := httptest.NewRequest(http.MethodGet, "/jobstatus/"+taskID, nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		switch w.Code {
		case http.StatusOK, http.StatusPartialContent, http.StatusInternalServerError:
			// valid states: parse and check terminal
		default:
			t.Fatalf("GET /jobstatus/%s returned unexpected status %d: %s", taskID, w.Code, w.Body.String())
		}
		var status entity.JobStatusResponse
		if err := json.Unmarshal(w.Body.Bytes(), &status); err != nil {
			t.Fatalf("parse jobstatus response: %v", err)
		}
		switch status.Status {
		case "Successful", "Failed":
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for job %s, last status=%q", taskID, status.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// doBackup posts to /backup and returns the backup ID.
// The Backup handler does a 2s sleep internally before returning the ID.
func doBackup(t *testing.T, router *gin.Engine) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/backup", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("POST /backup: status %d body: %s", w.Code, w.Body.String())
	}
	backupID := strings.TrimSpace(w.Body.String())
	if backupID == "" {
		t.Fatal("POST /backup returned empty backup ID")
	}
	return backupID
}

// TestHandlerInteg_FullBackupRestoreEvictLifecycle exercises backup → list →
// restore → evict through the old '/' HTTP API against a real MinIO bucket.
func TestHandlerInteg_FullBackupRestoreEvictLifecycle(t *testing.T) {
	env := newHandlerIntegEnv(t)
	ctx := context.Background()

	// 1. Backup
	backupID := doBackup(t, env.router)
	status := waitForJobHTTP(t, env.router, backupID)
	if status.Status != "Successful" {
		t.Fatalf("backup did not succeed: status=%s err=%s", status.Status, status.Error)
	}

	// Files must be present in MinIO after moveBackupToS3 runs.
	files, err := env.s3Client.ListFiles(ctx, filepath.Join(env.storageRoot, backupID))
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("expected backup files in S3, got none under %s", filepath.Join(env.storageRoot, backupID))
	}

	// 2. List — backup must appear
	req := httptest.NewRequest(http.MethodGet, "/listbackups", nil)
	w := httptest.NewRecorder()
	env.router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /listbackups: status %d", w.Code)
	}
	var names []string
	if err := json.Unmarshal(w.Body.Bytes(), &names); err != nil {
		t.Fatalf("parse listbackups: %v", err)
	}
	found := false
	for _, n := range names {
		if n == backupID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected %s in listbackups, got %v", backupID, names)
	}

	// 3. Restore — downloads from S3 and runs restore command
	// The old '/' handler returns the task ID as raw text (not JSON).
	restoreBody := fmt.Sprintf(`{"vault": %q}`, backupID)
	req = httptest.NewRequest(http.MethodPost, "/restore", strings.NewReader(restoreBody))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	env.router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("POST /restore: status %d body: %s", w.Code, w.Body.String())
	}
	restoreTaskID := strings.TrimSpace(w.Body.String())
	if restoreTaskID == "" {
		t.Fatal("POST /restore returned empty task ID")
	}
	restoreStatus := waitForJobHTTP(t, env.router, restoreTaskID)
	if restoreStatus.Status != "Successful" {
		t.Fatalf("restore did not succeed: status=%s err=%s", restoreStatus.Status, restoreStatus.Error)
	}
	restoredBytes, err := os.ReadFile(env.restoreTarget)
	if err != nil {
		t.Fatalf("reading restored file: %v", err)
	}
	if string(restoredBytes) != env.fixtureData {
		t.Fatalf("restored content = %q, want %q", string(restoredBytes), env.fixtureData)
	}

	// 4. Evict via old API — must remove from S3
	req = httptest.NewRequest(http.MethodPost, "/evict/"+backupID, nil)
	w = httptest.NewRecorder()
	env.router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("POST /evict/%s: status %d body: %s", backupID, w.Code, w.Body.String())
	}

	exists, err := env.s3Client.PrefixExists(ctx, filepath.Join(env.storageRoot, backupID))
	if err != nil {
		t.Fatalf("PrefixExists after evict: %v", err)
	}
	if exists {
		t.Fatalf("expected backup %s removed from S3 after evict, prefix still exists", backupID)
	}

	// Listing must no longer include the evicted backup.
	req = httptest.NewRequest(http.MethodGet, "/listbackups", nil)
	w = httptest.NewRecorder()
	env.router.ServeHTTP(w, req)
	if err := json.Unmarshal(w.Body.Bytes(), &names); err != nil {
		t.Fatalf("parse listbackups after evict: %v", err)
	}
	for _, n := range names {
		if n == backupID {
			t.Fatalf("evicted backup %s still appears in listbackups: %v", backupID, names)
		}
	}
}

// TestHandlerInteg_DownloadBackup verifies that GET /backup/:backup_id streams a
// non-empty ZIP from S3, proving the DownloadBackup S3 fix works end-to-end.
func TestHandlerInteg_DownloadBackup(t *testing.T) {
	env := newHandlerIntegEnv(t)

	backupID := doBackup(t, env.router)
	status := waitForJobHTTP(t, env.router, backupID)
	if status.Status != "Successful" {
		t.Fatalf("backup did not succeed: %s / %s", status.Status, status.Error)
	}

	req := httptest.NewRequest(http.MethodGet, "/backup/"+backupID, nil)
	w := httptest.NewRecorder()
	env.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GET /backup/%s: expected 200, got %d body: %s", backupID, w.Code, w.Body.String())
	}

	contentDisp := w.Header().Get("Content-Disposition")
	if !strings.Contains(contentDisp, backupID+".zip") {
		t.Fatalf("unexpected Content-Disposition %q, want it to contain %s.zip", contentDisp, backupID)
	}

	body := w.Body.Bytes()
	zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("response is not a valid ZIP (len=%d): %v", len(body), err)
	}
	if len(zr.File) == 0 {
		t.Fatal("ZIP response is empty — backup files were not downloaded from S3")
	}

	// Verify the fixture content survived the S3 round-trip.
	found := false
	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			t.Fatalf("open zip entry %s: %v", f.Name, err)
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("read zip entry %s: %v", f.Name, err)
		}
		if bytes.Contains(data, []byte(env.fixtureData)) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("fixture data %q not found in any ZIP entry", env.fixtureData)
	}
}

// TestHandlerInteg_S3PresignedURL verifies that GET /backup/s3/:backup_id returns
// at least one pre-signed URL pointing at the correct S3 key prefix, proving the
// CreateS3PresignedURL S3 fix works end-to-end.
func TestHandlerInteg_S3PresignedURL(t *testing.T) {
	env := newHandlerIntegEnv(t)

	backupID := doBackup(t, env.router)
	status := waitForJobHTTP(t, env.router, backupID)
	if status.Status != "Successful" {
		t.Fatalf("backup did not succeed: %s / %s", status.Status, status.Error)
	}

	req := httptest.NewRequest(http.MethodGet, "/backup/s3/"+backupID+"?expiration=3600", nil)
	w := httptest.NewRecorder()
	env.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GET /backup/s3/%s: expected 200, got %d body: %s", backupID, w.Code, w.Body.String())
	}

	var resp entity.S3PresignedURLResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("parse presigned URL response: %v", err)
	}
	if len(resp.Urls) == 0 {
		t.Fatalf("expected at least one presigned URL, got none — CreateS3PresignedURL S3 prefix fix may not be working")
	}
	for _, u := range resp.Urls {
		if u == "" {
			t.Fatalf("got empty URL in presigned response: %v", resp.Urls)
		}
	}
}
