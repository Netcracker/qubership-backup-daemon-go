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

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/controller"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/db"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/repo"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/tasks"
)

// handlerLocalFSEnv wires a BackupDaemon + EndpointHandler + Gin router using
// only the local filesystem (s3Enable=false, no S3 client).
type handlerLocalFSEnv struct {
	router        *gin.Engine
	storageRoot   string
	fixtureData   string
	restoreTarget string
}

func newHandlerLocalFSEnv(t *testing.T) *handlerLocalFSEnv {
	t.Helper()

	storageRoot := t.TempDir()
	storageRepo := repo.NewStorageRepo(storageRoot, "", "", false)

	dbPath := filepath.Join(t.TempDir(), "database.db")
	dbConn, err := db.NewConnection(dbPath)
	if err != nil {
		t.Fatalf("db.NewConnection: %v", err)
	}
	t.Cleanup(func() { _ = dbConn.Close() })
	dbRepo := repo.NewDBRepo(dbConn)

	logger := zap.NewNop().Sugar()

	fixtureData := "local-fs-payload-" + handlerIntegSafeName(t.Name())
	fixtureFile := filepath.Join(t.TempDir(), "seed.txt")
	if err := os.WriteFile(fixtureFile, []byte(fixtureData), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	restoreTarget := filepath.Join(t.TempDir(), "restored.txt")

	backupCmd := fmt.Sprintf("cp %s {{.data_folder}}/dump.txt", fixtureFile)
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
	// s3Enable=false, s3Client=nil — local FS only.
	taskPool := tasks.NewTaskPool(tpCtx, 10, executor, executor, dbRepo, nil, false, nil, logger)

	daemon := controller.NewBackupDaemon(storageRepo, dbRepo, taskPool, nil, executor, false, logger)

	gin.SetMode(gin.TestMode)
	router := gin.New()
	handler := NewEndpointHandler(daemon, daemon, logger)

	router.POST("/backup", handler.Backup)
	router.POST("/restore", handler.Restore)
	router.POST("/evict/:vault", handler.EvictByVault)
	router.GET("/jobstatus/:task_id", handler.JobStatus)
	router.GET("/listbackups", handler.ListBackups)
	router.GET("/backup/:backup_id", handler.DownloadBackup)

	return &handlerLocalFSEnv{
		router:        router,
		storageRoot:   storageRoot,
		fixtureData:   fixtureData,
		restoreTarget: restoreTarget,
	}
}

// doBackupLocal posts to /backup and returns the backup ID (raw text response).
func doBackupLocal(t *testing.T, router *gin.Engine) string {
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

// TestHandlerLocalFS_FullBackupRestoreEvictLifecycle verifies backup → list →
// restore → evict through the old '/' HTTP API with local filesystem storage.
func TestHandlerLocalFS_FullBackupRestoreEvictLifecycle(t *testing.T) {
	env := newHandlerLocalFSEnv(t)

	// 1. Backup
	backupID := doBackupLocal(t, env.router)
	status := waitForJobHTTP(t, env.router, backupID)
	if status.Status != "Successful" {
		t.Fatalf("backup did not succeed: status=%s err=%s", status.Status, status.Error)
	}

	// Backup folder must exist on local FS.
	backupFolder := filepath.Join(env.storageRoot, backupID)
	if _, err := os.Stat(backupFolder); err != nil {
		t.Fatalf("expected backup folder %s to exist: %v", backupFolder, err)
	}
	if _, err := os.Stat(filepath.Join(backupFolder, "dump.txt")); err != nil {
		t.Fatalf("expected dump.txt in backup folder: %v", err)
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

	// 3. Restore — copies from local FS and runs restore command
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

	// 4. Evict — must remove from local FS
	req = httptest.NewRequest(http.MethodPost, "/evict/"+backupID, nil)
	w = httptest.NewRecorder()
	env.router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("POST /evict/%s: status %d body: %s", backupID, w.Code, w.Body.String())
	}
	if _, err := os.Stat(backupFolder); !os.IsNotExist(err) {
		t.Fatalf("expected backup folder %s removed after evict, err=%v", backupFolder, err)
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

// TestHandlerLocalFS_DownloadBackup verifies that GET /backup/:backup_id streams a
// ZIP directly from local FS, containing the expected fixture data.
func TestHandlerLocalFS_DownloadBackup(t *testing.T) {
	env := newHandlerLocalFSEnv(t)

	backupID := doBackupLocal(t, env.router)
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
		t.Fatal("ZIP response is empty")
	}

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
