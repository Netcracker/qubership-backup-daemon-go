package rest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/controller"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/gin-gonic/gin"
	gomock "go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

// --- BackupV2 tests ---

func TestBackupV2_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockBackupDaemonUseCase(ctrl)
	mock.EXPECT().EnqueueBackup(gomock.Any(), gomock.Any()).Return(entity.BackupResponse{
		BackupID:     "20250101T000000",
		CreationTime: "2025-01-01T00:00:00Z",
	}, nil)

	handler := NewEndpointHandler(mock, mock, zap.NewNop().Sugar())
	r := gin.Default()
	r.POST("/api/v1/backup", handler.BackupV2)

	body := `{"storageName":"s3","blobPath":"bucket/path","databases":["db1","db2"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/backup", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp entity.BackupV2Response
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if resp.BackupID != "20250101T000000" {
		t.Errorf("expected backupId '20250101T000000', got '%s'", resp.BackupID)
	}
	if resp.Status != NotStarted {
		t.Errorf("expected status '%s', got '%s'", NotStarted, resp.Status)
	}
	if len(resp.Databases) != 2 {
		t.Errorf("expected 2 databases, got %d", len(resp.Databases))
	}
}

func TestBackupV2_EmptyBlobPath(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockBackupDaemonUseCase(ctrl)
	handler := NewEndpointHandler(mock, mock, zap.NewNop().Sugar())
	r := gin.Default()
	r.POST("/api/v1/backup", handler.BackupV2)

	body := `{"storageName":"s3","blobPath":"","databases":["db1"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/backup", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestBackupV2_InternalError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockBackupDaemonUseCase(ctrl)
	mock.EXPECT().EnqueueBackup(gomock.Any(), gomock.Any()).Return(entity.BackupResponse{}, errors.New("internal"))

	handler := NewEndpointHandler(mock, mock, zap.NewNop().Sugar())
	r := gin.Default()
	r.POST("/api/v1/backup", handler.BackupV2)

	body := `{"storageName":"s3","blobPath":"bucket/path","databases":[]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/backup", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

// --- BackupV2Status tests ---

func TestBackupV2Status_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockBackupDaemonUseCase(ctrl)
	mock.EXPECT().GetJobStatus(gomock.Any(), gomock.Any()).Return(entity.JobStatusResponse{
		StatusCode:     http.StatusOK,
		TaskID:         "20250101T000000",
		Status:         "Successful",
		StorageName:    "s3",
		BlobPath:       "bucket/path",
		Databases:      []string{"db1"},
		CreationTime:   "2025-01-01T00:00:00Z",
		CompletionTime: "2025-01-01T00:05:00Z",
	}, nil)

	handler := NewEndpointHandler(mock, mock, zap.NewNop().Sugar())
	r := gin.Default()
	r.GET("/api/v1/backup/:backup_id", handler.BackupV2Status)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/backup/20250101T000000", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp entity.BackupV2Response
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Status != Completed {
		t.Errorf("expected status '%s', got '%s'", Completed, resp.Status)
	}
	if resp.ErrorMessage != "" {
		t.Errorf("expected empty errorMessage, got '%s'", resp.ErrorMessage)
	}
	if resp.CompletionTime != "2025-01-01T00:05:00Z" {
		t.Errorf("expected CompletionTime '2025-01-01T00:05:00Z', got '%s'", resp.CompletionTime)
	}
	if len(resp.Databases) != 1 {
		t.Fatalf("expected 1 database, got %d", len(resp.Databases))
	}
	if resp.Databases[0].CreationTime != "2025-01-01T00:00:00Z" {
		t.Errorf("expected db CreationTime '2025-01-01T00:00:00Z', got '%s'", resp.Databases[0].CreationTime)
	}
}

func TestBackupV2Status_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockBackupDaemonUseCase(ctrl)
	mock.EXPECT().GetJobStatus(gomock.Any(), gomock.Any()).Return(entity.JobStatusResponse{
		StatusCode: http.StatusNotFound,
	}, nil)

	handler := NewEndpointHandler(mock, mock, zap.NewNop().Sugar())
	r := gin.Default()
	r.GET("/api/v1/backup/:backup_id", handler.BackupV2Status)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/backup/missing", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

// --- RestoreV2 tests ---

func TestRestoreV2_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockBackupDaemonUseCase(ctrl)
	mock.EXPECT().RestoreBackup(gomock.Any(), gomock.Any()).Return(entity.RestoreResponse{
		TaskID:       "restore-uuid-1",
		CreationTime: "2025-01-01T00:00:00Z",
	}, nil)

	handler := NewEndpointHandler(mock, mock, zap.NewNop().Sugar())
	r := gin.Default()
	r.POST("/api/v1/restore/:backup_id", handler.RestoreV2)

	body := `{"storageName":"s3","blobPath":"bucket/path","databases":[{"previousDatabaseName":"old_db","databaseName":"new_db"}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/restore/20250101T000000", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp entity.RestoreV2Response
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.RestoreID != "restore-uuid-1" {
		t.Errorf("expected restoreId 'restore-uuid-1', got '%s'", resp.RestoreID)
	}
	if resp.Status != NotStarted {
		t.Errorf("expected status '%s', got '%s'", NotStarted, resp.Status)
	}
	if len(resp.Databases) != 1 {
		t.Errorf("expected 1 database, got %d", len(resp.Databases))
	}
}

func TestRestoreV2_DryRun(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockBackupDaemonUseCase(ctrl)
	mock.EXPECT().RestoreBackup(gomock.Any(), gomock.Any()).Return(entity.RestoreResponse{
		TaskID:       "restore-dry-1",
		CreationTime: "2025-01-01T00:00:00Z",
	}, nil)

	handler := NewEndpointHandler(mock, mock, zap.NewNop().Sugar())
	r := gin.Default()
	r.POST("/api/v1/restore/:backup_id", handler.RestoreV2)

	body := `{"storageName":"s3","blobPath":"bucket/path","databases":[{"previousDatabaseName":"db1","databaseName":"db1"}],"dryRun":true}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/restore/20250101T000000", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp entity.RestoreV2Response
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Status != Completed {
		t.Errorf("expected status '%s' for dry run, got '%s'", Completed, resp.Status)
	}
}

func TestRestoreV2_VaultNotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockBackupDaemonUseCase(ctrl)
	mock.EXPECT().RestoreBackup(gomock.Any(), gomock.Any()).Return(
		entity.RestoreResponse{},
		fmt.Errorf("backup not found: %w", controller.ErrVaultNotFound),
	)

	handler := NewEndpointHandler(mock, mock, zap.NewNop().Sugar())
	r := gin.Default()
	r.POST("/api/v1/restore/:backup_id", handler.RestoreV2)

	body := `{"storageName":"s3","blobPath":"bucket/path","databases":[{"previousDatabaseName":"db1","databaseName":"db1"}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/restore/20250101T000000", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestRestoreV2_EmptyBlobPath(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockBackupDaemonUseCase(ctrl)
	handler := NewEndpointHandler(mock, mock, zap.NewNop().Sugar())
	r := gin.Default()
	r.POST("/api/v1/restore/:backup_id", handler.RestoreV2)

	body := `{"storageName":"s3","blobPath":"","databases":[{"previousDatabaseName":"db1","databaseName":"db1"}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/restore/20250101T000000", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestRestoreV2_MissingDatabaseName(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockBackupDaemonUseCase(ctrl)
	handler := NewEndpointHandler(mock, mock, zap.NewNop().Sugar())
	r := gin.Default()
	r.POST("/api/v1/restore/:backup_id", handler.RestoreV2)

	body := `{"storageName":"s3","blobPath":"bucket/path","databases":[{"previousDatabaseName":"db1","databaseName":""}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/restore/20250101T000000", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

// --- RestoreV2Status tests ---

func TestRestoreV2Status_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockBackupDaemonUseCase(ctrl)
	mock.EXPECT().GetJobStatus(gomock.Any(), gomock.Any()).Return(entity.JobStatusResponse{
		StatusCode:     http.StatusOK,
		TaskID:         "restore-uuid-1",
		Status:         "Successful",
		Type:           "restore",
		StorageName:    "s3",
		BlobPath:       "bucket/path",
		Databases:      []string{"db1"},
		CreationTime:   "2025-01-01T00:00:00Z",
		CompletionTime: "2025-01-01T00:05:00Z",
		RestoreDatabases: []entity.RestoreDBMap{
			{PreviousDatabaseName: "old_db", DatabaseName: "new_db"},
		},
	}, nil)

	handler := NewEndpointHandler(mock, mock, zap.NewNop().Sugar())
	r := gin.Default()
	r.GET("/api/v1/restore/:restore_id", handler.RestoreV2Status)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/restore/restore-uuid-1", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp entity.RestoreV2Response
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Status != Completed {
		t.Errorf("expected status 'completed', got '%s'", resp.Status)
	}
	if resp.CompletionTime != "2025-01-01T00:05:00Z" {
		t.Errorf("expected CompletionTime '2025-01-01T00:05:00Z', got '%s'", resp.CompletionTime)
	}
	if len(resp.Databases) != 1 {
		t.Fatalf("expected 1 database, got %d", len(resp.Databases))
	}
	if resp.Databases[0].DatabaseName != "new_db" {
		t.Errorf("expected DatabaseName 'new_db', got '%s'", resp.Databases[0].DatabaseName)
	}
}

func TestRestoreV2Status_Failed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockBackupDaemonUseCase(ctrl)
	mock.EXPECT().GetJobStatus(gomock.Any(), gomock.Any()).Return(entity.JobStatusResponse{
		StatusCode:   http.StatusInternalServerError,
		TaskID:       "restore-fail-1",
		Status:       "Failed",
		Type:         "restore",
		Error:        "restore script crashed",
		CreationTime: "2025-01-01T00:00:00Z",
		Databases:    []string{"db1"},
	}, nil)

	handler := NewEndpointHandler(mock, mock, zap.NewNop().Sugar())
	r := gin.Default()
	r.GET("/api/v1/restore/:restore_id", handler.RestoreV2Status)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/restore/restore-fail-1", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp entity.RestoreV2Response
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Status != Failed {
		t.Errorf("expected status 'failed', got '%s'", resp.Status)
	}
	if resp.ErrorMessage != "restore script crashed" {
		t.Errorf("expected error message 'restore script crashed', got '%s'", resp.ErrorMessage)
	}
}

func TestRestoreV2Status_NotRestore(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockBackupDaemonUseCase(ctrl)
	mock.EXPECT().GetJobStatus(gomock.Any(), gomock.Any()).Return(entity.JobStatusResponse{
		StatusCode: http.StatusOK,
		TaskID:     "backup-1",
		Type:       "backup",
		Status:     "Successful",
	}, nil)

	handler := NewEndpointHandler(mock, mock, zap.NewNop().Sugar())
	r := gin.Default()
	r.GET("/api/v1/restore/:restore_id", handler.RestoreV2Status)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/restore/backup-1", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for non-restore job, got %d: %s", w.Code, w.Body.String())
	}
}

func TestRestoreV2Status_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockBackupDaemonUseCase(ctrl)
	mock.EXPECT().GetJobStatus(gomock.Any(), gomock.Any()).Return(entity.JobStatusResponse{
		StatusCode: http.StatusNotFound,
		Type:       "restore",
	}, nil)

	handler := NewEndpointHandler(mock, mock, zap.NewNop().Sugar())
	r := gin.Default()
	r.GET("/api/v1/restore/:restore_id", handler.RestoreV2Status)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/restore/missing", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestRestoreV2Status_FallbackToNames(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockBackupDaemonUseCase(ctrl)
	mock.EXPECT().GetJobStatus(gomock.Any(), gomock.Any()).Return(entity.JobStatusResponse{
		StatusCode:       http.StatusOK,
		TaskID:           "old-restore",
		Status:           "Successful",
		Type:             "restore",
		Databases:        []string{"db1", "db2"},
		CreationTime:     "2025-01-01T00:00:00Z",
		CompletionTime:   "2025-01-01T00:05:00Z",
		RestoreDatabases: nil, // no rich data stored (older job)
	}, nil)

	handler := NewEndpointHandler(mock, mock, zap.NewNop().Sugar())
	r := gin.Default()
	r.GET("/api/v1/restore/:restore_id", handler.RestoreV2Status)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/restore/old-restore", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp entity.RestoreV2Response
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Databases) != 2 {
		t.Fatalf("expected 2 databases from name fallback, got %d", len(resp.Databases))
	}
	if resp.Databases[0].DatabaseName != "db1" {
		t.Errorf("expected 'db1', got '%s'", resp.Databases[0].DatabaseName)
	}
}

// --- BackupV2Delete tests ---

func TestBackupV2Delete_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockBackupDaemonUseCase(ctrl)
	mock.EXPECT().RemoveBackupV2(gomock.Any(), gomock.Any()).Return(nil)

	handler := NewEndpointHandler(mock, mock, zap.NewNop().Sugar())
	r := gin.Default()
	r.DELETE("/api/v1/backup/:backup_id", handler.BackupV2Delete)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/backup/20250101T000000?blobPath=bucket/path", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestBackupV2Delete_MissingBlobPath(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockBackupDaemonUseCase(ctrl)
	handler := NewEndpointHandler(mock, mock, zap.NewNop().Sugar())
	r := gin.Default()
	r.DELETE("/api/v1/backup/:backup_id", handler.BackupV2Delete)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/backup/20250101T000000", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestBackupV2Delete_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockBackupDaemonUseCase(ctrl)
	mock.EXPECT().RemoveBackupV2(gomock.Any(), gomock.Any()).Return(errors.New("no job found for vault"))

	handler := NewEndpointHandler(mock, mock, zap.NewNop().Sugar())
	r := gin.Default()
	r.DELETE("/api/v1/backup/:backup_id", handler.BackupV2Delete)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/backup/missing?blobPath=bucket/path", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

// --- RestoreV2Delete tests ---

func TestRestoreV2Delete_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockBackupDaemonUseCase(ctrl)
	mock.EXPECT().GetJobStatus(gomock.Any(), gomock.Any()).Return(entity.JobStatusResponse{
		StatusCode: http.StatusOK,
		TaskID:     "restore-1",
		Vault:      "20250101T000000",
		BlobPath:   "bucket/path",
	}, nil)
	mock.EXPECT().RemoveRestoreV2(gomock.Any(), gomock.Any()).Return(nil)

	handler := NewEndpointHandler(mock, mock, zap.NewNop().Sugar())
	r := gin.Default()
	r.DELETE("/api/v1/restore/:restore_id", handler.RestoreV2Delete)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/restore/restore-1?blobPath=bucket/path", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestRestoreV2Delete_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mock := NewMockBackupDaemonUseCase(ctrl)
	mock.EXPECT().GetJobStatus(gomock.Any(), gomock.Any()).Return(entity.JobStatusResponse{
		StatusCode: http.StatusNotFound,
	}, nil)

	handler := NewEndpointHandler(mock, mock, zap.NewNop().Sugar())
	r := gin.Default()
	r.DELETE("/api/v1/restore/:restore_id", handler.RestoreV2Delete)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/restore/missing", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

// --- Helper function tests ---

func TestMapJobStatus(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{"Queued", NotStarted},
		{"Processing", InProgress},
		{"Successful", Completed},
		{"Failed", Failed},
		{"", Unknown},
		{"something", Unknown},
		{" Queued ", NotStarted},
		{" FAILED ", Failed},
	}
	for _, tc := range cases {
		got := mapJobStatus(tc.input)
		if got != tc.expected {
			t.Errorf("mapJobStatus(%q) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

func TestNormalizeBlobPath(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{"  bucket/path  ", "bucket/path"},
		{"/bucket/path/", "bucket/path"},
		{"'bucket/path'", "bucket/path"},
		{`"bucket/path"`, "bucket/path"},
		{"", ""},
	}
	for _, tc := range cases {
		got := normalizeBlobPath(tc.input)
		if got != tc.expected {
			t.Errorf("normalizeBlobPath(%q) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

func TestValidateBlobPath(t *testing.T) {
	_, err := validateBlobPath("")
	if err == nil {
		t.Error("expected error for empty blobPath")
	}

	got, err := validateBlobPath("  /bucket/path/  ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "bucket/path" {
		t.Errorf("expected 'bucket/path', got '%s'", got)
	}
}

func TestRestoreDbStatuses(t *testing.T) {
	maps := []entity.RestoreDBMap{
		{
			PreviousDatabaseName: "old_db",
			DatabaseName:         "new_db",
		},
	}

	statuses := RestoreDbStatuses(maps, Completed, "2025-01-01T00:00:00Z")
	if len(statuses) != 1 {
		t.Fatalf("expected 1 status, got %d", len(statuses))
	}
	s := statuses[0]
	if s.DatabaseName != "new_db" {
		t.Errorf("expected DatabaseName 'new_db', got '%s'", s.DatabaseName)
	}
	if s.PreviousDatabaseName != "old_db" {
		t.Errorf("expected PreviousDatabaseName 'old_db', got '%s'", s.PreviousDatabaseName)
	}
	if s.Status != Completed {
		t.Errorf("expected status 'completed', got '%s'", s.Status)
	}
	if s.ErrorMessage != "" {
		t.Errorf("expected empty error message for completed, got '%s'", s.ErrorMessage)
	}
}

func TestRestoreDbStatuses_Failed(t *testing.T) {
	maps := []entity.RestoreDBMap{
		{PreviousDatabaseName: "db1", DatabaseName: "db1"},
	}

	statuses := RestoreDbStatuses(maps, Failed, "2025-01-01T00:00:00Z")
	if len(statuses) != 1 {
		t.Fatalf("expected 1 status, got %d", len(statuses))
	}
	if statuses[0].ErrorMessage != "restore failed" {
		t.Errorf("expected error message 'restore failed', got '%s'", statuses[0].ErrorMessage)
	}
}

func TestRestoreDbStatusesFromNames(t *testing.T) {
	statuses := RestoreDbStatusesFromNames([]string{"db1", "db2"}, Completed, "2025-01-01T00:00:00Z")
	if len(statuses) != 2 {
		t.Fatalf("expected 2 statuses, got %d", len(statuses))
	}
	if statuses[0].DatabaseName != "db1" {
		t.Errorf("expected 'db1', got '%s'", statuses[0].DatabaseName)
	}
}

func TestRestoreDbStatusesFromNames_Empty(t *testing.T) {
	statuses := RestoreDbStatusesFromNames(nil, Completed, "2025-01-01T00:00:00Z")
	if len(statuses) != 0 {
		t.Fatalf("expected 0 statuses for nil input, got %d", len(statuses))
	}
}

func TestDbStatuses(t *testing.T) {
	statuses := DbStatuses([]string{"db1", "db2"}, Completed, "2025-01-01T00:00:00Z", "")
	if len(statuses) != 2 {
		t.Fatalf("expected 2 statuses, got %d", len(statuses))
	}
	if statuses[0].DatabaseName != "db1" {
		t.Errorf("expected 'db1', got '%s'", statuses[0].DatabaseName)
	}
	if statuses[0].Status != Completed {
		t.Errorf("expected status 'completed', got '%s'", statuses[0].Status)
	}
	if statuses[0].CreationTime != "2025-01-01T00:00:00Z" {
		t.Errorf("expected creationTime '2025-01-01T00:00:00Z', got '%s'", statuses[0].CreationTime)
	}
	if statuses[0].ErrorMessage != "" {
		t.Errorf("expected empty errorMessage, got '%s'", statuses[0].ErrorMessage)
	}
}

func TestDbStatuses_Empty(t *testing.T) {
	statuses := DbStatuses(nil, Completed, "2025-01-01T00:00:00Z", "")
	if len(statuses) != 0 {
		t.Fatalf("expected 0 statuses for nil input, got %d", len(statuses))
	}
}

func TestDBEntries(t *testing.T) {
	entries := DBEntries([]string{"db1", "  ", "db2"})
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries (spaces trimmed), got %d", len(entries))
	}
	if entries[0].SimpleName != "db1" {
		t.Errorf("expected 'db1', got '%s'", entries[0].SimpleName)
	}
}

func TestDBEntries_Nil(t *testing.T) {
	entries := DBEntries(nil)
	if entries != nil {
		t.Errorf("expected nil for nil input, got %v", entries)
	}
}
