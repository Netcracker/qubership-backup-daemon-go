package rest

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/controller"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/gin-gonic/gin"
	gomock "go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestEnqueueBackup(t *testing.T) {
	testCases := []struct {
		name               string
		requestBodyJSON    string
		expectedResponse   entity.BackupResponse
		expectedBodyJSON   string
		expectedStatusCode int
		expectedError      error
	}{
		{
			name:            "success",
			requestBodyJSON: `{"externalBackupPath": "./app/repo/coverageo"}`,
			expectedResponse: entity.BackupResponse{
				BackupID: "coverageo",
			},
			expectedError:      nil,
			expectedBodyJSON:   `coverageo`,
			expectedStatusCode: http.StatusOK,
		},
		{
			name:               "bad json request",
			requestBodyJSON:    `{"externalBackupPath": ./app/repo/coverageo"}`,
			expectedResponse:   entity.BackupResponse{},
			expectedError:      nil,
			expectedBodyJSON:   `{"message":"failed to unmarshall body err: invalid character '.' looking for beginning of value"}`,
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name:               "internal error",
			requestBodyJSON:    `{"externalBackupPath": "./app/repo/covrago"}`,
			expectedResponse:   entity.BackupResponse{},
			expectedError:      errors.New("internal error"),
			expectedBodyJSON:   `{"status":"Failed","message":"internal error"}`,
			expectedStatusCode: http.StatusInternalServerError,
		},
		// BWC: unknown body key → 500
		{
			name:               "unknown body key",
			requestBodyJSON:    `{"unknown_key_xyz": "value"}`,
			expectedResponse:   entity.BackupResponse{},
			expectedError:      nil,
			expectedBodyJSON:   `{"message":"Unknown body key: unknown_key_xyz"}`,
			expectedStatusCode: http.StatusInternalServerError,
		},
		// BWC: incremental without prior backups → 409
		{
			name:               "incremental illegal state",
			requestBodyJSON:    `{"externalBackupPath": "./app/repo/coverageo"}`,
			expectedResponse:   entity.BackupResponse{},
			expectedError:      fmt.Errorf("no prior backups: %w", controller.ErrIllegalState),
			expectedBodyJSON:   `{"status":"Failed","message":"no prior backups: illegal state"}`,
			expectedStatusCode: http.StatusConflict,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			fullMock := NewMockBackupDaemonUseCase(ctrl)
			incrMock := NewMockBackupDaemonUseCase(ctrl)
			fullMock.EXPECT().EnqueueBackup(gomock.Any(), gomock.Any()).Return(tc.expectedResponse, tc.expectedError).AnyTimes()
			incrMock.EXPECT().EnqueueBackup(gomock.Any(), gomock.Any()).Return(tc.expectedResponse, tc.expectedError).AnyTimes()

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(fullMock, incrMock, sugar)

			r := gin.Default()
			r.POST("/incremental/backup", handler.Backup)

			req := httptest.NewRequest(http.MethodPost, "/incremental/backup", bytes.NewBufferString(tc.requestBodyJSON))
			req.Header.Set("Content-Type", "application/json")
			req.ContentLength = int64(len(tc.requestBodyJSON))
			w := httptest.NewRecorder()

			r.ServeHTTP(w, req)
			if tc.expectedStatusCode != w.Code {
				t.Fatalf("expected status %d, got %d", tc.expectedStatusCode, w.Code)
			}
			if tc.expectedBodyJSON != w.Body.String() {
				t.Fatalf("expected body %s, got %s", tc.expectedBodyJSON, w.Body.String())
			}
		})
	}
}

func TestRestoreBackup(t *testing.T) {
	testCases := []struct {
		name               string
		requestBodyJSON    string
		expectedResponse   entity.RestoreResponse
		expectedBodyJSON   string
		expectedStatusCode int
		expectedError      error
		statsError         error
	}{
		{
			name:            "success",
			requestBodyJSON: `{"vault":"20250101T000000", "externalBackupPath": "./app/repo/coverageo"}`,
			expectedResponse: entity.RestoreResponse{
				TaskID: "coverageo",
			},
			expectedBodyJSON:   `coverageo`,
			expectedStatusCode: http.StatusOK,
			expectedError:      nil,
		},
		{
			name:               "bad json request",
			requestBodyJSON:    `{"externalBackupPath": ./app/repo/coverageo"}`,
			expectedResponse:   entity.RestoreResponse{},
			expectedError:      nil,
			expectedBodyJSON:   `{"message":"failed to unmarshall body err: invalid character '.' looking for beginning of value"}`,
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name:               "internal error",
			requestBodyJSON:    `{"vault":"20250101T000000", "externalBackupPath": "./app/repo/covrago"}`,
			expectedResponse:   entity.RestoreResponse{},
			expectedError:      errors.New("internal error"),
			expectedBodyJSON:   `{"status":"Failed","message":"internal error"}`,
			expectedStatusCode: http.StatusInternalServerError,
		},
		{
			name:               "not found",
			requestBodyJSON:    `{"dd": "./app/repo/covrago"}`,
			expectedResponse:   entity.RestoreResponse{},
			expectedError:      nil,
			expectedBodyJSON:   `{"message":"Sorry, wrong JSON string. No 'vault' or 'ts' parameter","status":"Failed"}`,
			expectedStatusCode: http.StatusNotFound,
		},
		// BWC: nonexistent vault → 404
		{
			name:               "nonexistent vault",
			requestBodyJSON:    `{"vault":"nonexistent-vault-99999"}`,
			expectedResponse:   entity.RestoreResponse{},
			expectedError:      nil,
			statsError:         errors.New("backup nonexistent-vault-99999 not found"),
			expectedBodyJSON:   `{"message":"Restore failed. Wrong vault name or ts: backup nonexistent-vault-99999 not found","status":"Failed"}`,
			expectedStatusCode: http.StatusNotFound,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			fullMock := NewMockBackupDaemonUseCase(ctrl)
			incrMock := NewMockBackupDaemonUseCase(ctrl)
			fullMock.EXPECT().RestoreBackup(gomock.Any(), gomock.Any()).Return(tc.expectedResponse, tc.expectedError).AnyTimes()
			fullMock.EXPECT().GetBackupStats(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(map[string]interface{}{"id": "20250101T000000"}, tc.statsError).AnyTimes()
			incrMock.EXPECT().RestoreBackup(gomock.Any(), gomock.Any()).Return(tc.expectedResponse, tc.expectedError).AnyTimes()
			incrMock.EXPECT().GetBackupStats(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(map[string]interface{}{"id": "20250101T000000"}, tc.statsError).AnyTimes()

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(fullMock, incrMock, sugar)

			r := gin.Default()
			r.POST("/restore", handler.Restore)

			req := httptest.NewRequest(http.MethodPost, "/restore", bytes.NewBufferString(tc.requestBodyJSON))
			req.Header.Set("Content-Type", "application/json")
			req.ContentLength = int64(len(tc.requestBodyJSON))
			w := httptest.NewRecorder()

			r.ServeHTTP(w, req)
			if tc.expectedStatusCode != w.Code {
				t.Fatalf("expected status %d, got %d", tc.expectedStatusCode, w.Code)
			}
			if tc.expectedBodyJSON != w.Body.String() {
				t.Fatalf("expected body %s, got %s", tc.expectedBodyJSON, w.Body.String())
			}
		})
	}
}

func TestEvict(t *testing.T) {
	testCases := []struct {
		name               string
		expectedError      error
		expectedBodyJSON   string
		expectedStatusCode int
	}{
		{
			name:               "success",
			expectedError:      nil,
			expectedBodyJSON:   "Ok\n",
			expectedStatusCode: http.StatusOK,
		},
		{
			name:               "internal error",
			expectedError:      errors.New("internal error"),
			expectedBodyJSON:   `{"message":"internal error"}`,
			expectedStatusCode: http.StatusInternalServerError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			fullMock := NewMockBackupDaemonUseCase(ctrl)
			incrMock := NewMockBackupDaemonUseCase(ctrl)
			fullMock.EXPECT().EnqueueEviction(gomock.Any(), gomock.Any()).Return(tc.expectedError).AnyTimes()
			incrMock.EXPECT().EnqueueEviction(gomock.Any(), gomock.Any()).Return(tc.expectedError).AnyTimes()

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(fullMock, incrMock, sugar)

			r := gin.Default()
			r.POST("/evict", handler.Evict)

			req := httptest.NewRequest(http.MethodPost, "/evict", nil)
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			r.ServeHTTP(w, req)
			if tc.expectedStatusCode != w.Code {
				t.Fatalf("expected status %d, got %d", tc.expectedStatusCode, w.Code)
			}
			if tc.expectedBodyJSON != w.Body.String() {
				t.Fatalf("expected body %s, got %s", tc.expectedBodyJSON, w.Body.String())
			}
		})
	}
}

func TestEvictVault(t *testing.T) {
	testCases := []struct {
		name               string
		vault              string
		expectedError      error
		expectedBodyJSON   string
		expectedStatusCode int
	}{
		{
			name:               "success",
			vault:              "eeee",
			expectedError:      nil,
			expectedBodyJSON:   "Ok\n",
			expectedStatusCode: http.StatusOK,
		},
		{
			name:               "internal error",
			expectedError:      errors.New("internal error"),
			expectedBodyJSON:   `{"message":"internal error","status":"Failed"}`,
			vault:              "eeee",
			expectedStatusCode: http.StatusInternalServerError,
		},
		// BWC: nonexistent vault → 404
		{
			name:               "vault not found",
			vault:              "nonexistent-vault-99999",
			expectedError:      controller.ErrVaultNotFound,
			expectedBodyJSON:   `{"message":"backup vault not found","status":"Failed"}`,
			expectedStatusCode: http.StatusNotFound,
		},
		// BWC: locked vault → 423
		{
			name:               "vault locked",
			vault:              "locked-vault",
			expectedError:      controller.ErrVaultLocked,
			expectedBodyJSON:   `{"message":"backup vault is locked","status":"Failed"}`,
			expectedStatusCode: http.StatusLocked,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockStorageRepo := NewMockBackupDaemonUseCase(ctrl)
			mockStorageRepo.EXPECT().RemoveBackup(gomock.Any(), gomock.Any()).Return(tc.expectedError).AnyTimes()

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(mockStorageRepo, mockStorageRepo, sugar)

			r := gin.Default()
			r.POST("/evict/:vault", handler.EvictByVault)

			req := httptest.NewRequest(http.MethodPost, "/evict/"+tc.vault, nil)
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			r.ServeHTTP(w, req)
			if tc.expectedStatusCode != w.Code {
				t.Fatalf("expected status %d, got %d", tc.expectedStatusCode, w.Code)
			}
			if tc.expectedBodyJSON != w.Body.String() {
				t.Fatalf("expected body %s, got %s", tc.expectedBodyJSON, w.Body.String())
			}
		})
	}
}

func TestExternalRestore(t *testing.T) {
	testCases := []struct {
		name               string
		requestBodyJSON    string
		expectedResponse   entity.RestoreResponse
		expectedError      error
		expectedBodyJSON   string
		expectedStatusCode int
	}{
		{
			name:            "success",
			requestBodyJSON: `{"<custom_var_key1>":"<custom_var_value1>", "<custom_var_key2>":"<custom_var_value2>"}`,
			expectedError:   nil,
			expectedResponse: entity.RestoreResponse{
				TaskID: "coverageo",
			},
			expectedBodyJSON:   `{"task_id":"coverageo"}`,
			expectedStatusCode: http.StatusOK,
		},
		{
			name:               "internal error",
			requestBodyJSON:    `{"<custom_var_key1>":"<custom_var_value1>", "<custom_var_key2>":"<custom_var_value2>"}`,
			expectedResponse:   entity.RestoreResponse{},
			expectedStatusCode: http.StatusInternalServerError,
			expectedError:      errors.New("internal error"),
			expectedBodyJSON:   `{"message":"failed to restore external backup err: internal error"}`,
		},
		{
			name:               "bad json body",
			requestBodyJSON:    `ddss`,
			expectedResponse:   entity.RestoreResponse{},
			expectedStatusCode: http.StatusBadRequest,
			expectedError:      errors.New("invalid character 'd' looking for beginning of value"),
			expectedBodyJSON:   `{"message":"failed to unmarshall body err: invalid character 'd' looking for beginning of value"}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			fullMock := NewMockBackupDaemonUseCase(ctrl)
			incrMock := NewMockBackupDaemonUseCase(ctrl)
			fullMock.EXPECT().RestoreBackup(gomock.Any(), gomock.Any()).Return(tc.expectedResponse, tc.expectedError).AnyTimes()
			incrMock.EXPECT().RestoreBackup(gomock.Any(), gomock.Any()).Return(tc.expectedResponse, tc.expectedError).AnyTimes()

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(fullMock, incrMock, sugar)

			r := gin.Default()
			r.POST("/external/restore", handler.ExternalRestore)

			req := httptest.NewRequest(http.MethodPost, "/external/restore", bytes.NewBufferString(tc.requestBodyJSON))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			r.ServeHTTP(w, req)
			if tc.expectedStatusCode != w.Code {
				t.Fatalf("expected status %d, got %d", tc.expectedStatusCode, w.Code)
			}
			if tc.expectedBodyJSON != w.Body.String() {
				t.Fatalf("expected body %s, got %s", tc.expectedBodyJSON, w.Body.String())
			}
		})
	}
}

func TestJobStatus(t *testing.T) {
	testCases := []struct {
		name               string
		expectedResponse   entity.JobStatusResponse
		expectedError      error
		expectedBodyJSON   string
		expectedStatusCode int
	}{
		{
			name: "success",
			expectedResponse: entity.JobStatusResponse{
				StatusCode: http.StatusOK,
				Vault:      "",
				TaskID:     "coverageo",
				Type:       "backup",
				Error:      "",
				Status:     "Successful",
			},
			expectedBodyJSON:   `{"status":"Successful","vault":"","type":"backup","err":"","task_id":"coverageo","StatusCode":200}`,
			expectedStatusCode: http.StatusOK,
			expectedError:      nil,
		},
		{
			name:               "internal error",
			expectedResponse:   entity.JobStatusResponse{},
			expectedError:      errors.New("internal error"),
			expectedStatusCode: http.StatusInternalServerError,
			expectedBodyJSON:   `{"message":"failed to get job status err: internal error"}`,
		},
		{
			name: "not found",
			expectedResponse: entity.JobStatusResponse{
				StatusCode: http.StatusNotFound,
				TaskID:     "coverageo",
			},
			expectedError:      nil,
			expectedBodyJSON:   `{"message":"Sorry, no job 'coverageo' recorded in database"}`,
			expectedStatusCode: http.StatusNotFound,
		},
		// BWC: in-progress job → 206
		{
			name: "processing",
			expectedResponse: entity.JobStatusResponse{
				StatusCode: http.StatusPartialContent,
				TaskID:     "coverageo",
				Type:       "backup",
				Status:     "Processing",
			},
			expectedError:      nil,
			expectedBodyJSON:   `{"status":"Processing","vault":"","type":"backup","err":"","task_id":"coverageo","StatusCode":206}`,
			expectedStatusCode: http.StatusPartialContent,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			fullMock := NewMockBackupDaemonUseCase(ctrl)
			incrMock := NewMockBackupDaemonUseCase(ctrl)
			fullMock.EXPECT().GetJobStatus(gomock.Any(), gomock.Any()).Return(tc.expectedResponse, tc.expectedError).AnyTimes()
			incrMock.EXPECT().GetJobStatus(gomock.Any(), gomock.Any()).Return(tc.expectedResponse, tc.expectedError).AnyTimes()

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(fullMock, incrMock, sugar)

			r := gin.Default()
			r.GET("/jobstatus/:task_id", handler.JobStatus)

			req := httptest.NewRequest(http.MethodGet, "/jobstatus/coverageo", nil)
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			r.ServeHTTP(w, req)
			if tc.expectedStatusCode != w.Code {
				t.Fatalf("expected status %d, got %d", tc.expectedStatusCode, w.Code)
			}
			if tc.expectedBodyJSON != w.Body.String() {
				t.Fatalf("expected body %s, got %s", tc.expectedBodyJSON, w.Body.String())
			}
		})
	}
}

func TestS3PresignedURL(t *testing.T) {
	testCases := []struct {
		name               string
		expectedResponse   entity.S3PresignedURLResponse
		expectedError      error
		expectedBodyJSON   string
		expectedStatusCode int
		expirationTime     string
	}{
		{
			name: "success",
			expectedResponse: entity.S3PresignedURLResponse{
				Urls: []string{"url1", "url2"},
			},
			expectedBodyJSON:   `{"urls":["url1","url2"]}`,
			expectedStatusCode: http.StatusOK,
			expectedError:      nil,
			expirationTime:     "20000",
		},
		{
			name:               "internal error",
			expectedResponse:   entity.S3PresignedURLResponse{},
			expectedBodyJSON:   `{"message":"failed to create s3 presigned urls err: internal error"}`,
			expectedError:      errors.New("internal error"),
			expectedStatusCode: http.StatusInternalServerError,
			expirationTime:     "20000",
		},
		{
			name:               "bad request",
			expectedResponse:   entity.S3PresignedURLResponse{},
			expectedError:      nil,
			expectedStatusCode: http.StatusBadRequest,
			expectedBodyJSON:   `{"message":"failed to parse value from url err: strconv.Atoi: parsing \"20000rr\": invalid syntax"}`,
			expirationTime:     "20000rr",
		},
		// BWC: no expiration param → 204
		{
			name:               "no expiration",
			expectedResponse:   entity.S3PresignedURLResponse{},
			expectedError:      nil,
			expectedStatusCode: http.StatusNoContent,
			expectedBodyJSON:   ``,
			expirationTime:     "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			fullMock := NewMockBackupDaemonUseCase(ctrl)
			incrMock := NewMockBackupDaemonUseCase(ctrl)
			fullMock.EXPECT().CreateS3PresignedURL(gomock.Any(), gomock.Any()).Return(tc.expectedResponse, tc.expectedError).AnyTimes()
			incrMock.EXPECT().CreateS3PresignedURL(gomock.Any(), gomock.Any()).Return(tc.expectedResponse, tc.expectedError).AnyTimes()

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(fullMock, incrMock, sugar)

			r := gin.Default()
			r.GET("/backup/s3/:backup_id", handler.S3PresignedURL)

			url := "/backup/s3/20210601T115105"
			if tc.expirationTime != "" {
				url = fmt.Sprintf("/backup/s3/20210601T115105?expiration=%s", tc.expirationTime)
			}
			req := httptest.NewRequest(http.MethodGet, url, nil)
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			r.ServeHTTP(w, req)
			if tc.expectedStatusCode != w.Code {
				t.Fatalf("expected status %d, got %d", tc.expectedStatusCode, w.Code)
			}
			if tc.expectedBodyJSON != w.Body.String() {
				t.Fatalf("expected body %s, got %s", tc.expectedBodyJSON, w.Body.String())
			}
		})
	}
}

func TestListBackupsHandler(t *testing.T) {
	testCases := []struct {
		name               string
		expectedBackups    []string
		expectedError      error
		expectedBodyJSON   string
		expectedStatusCode int
	}{
		{
			name:               "list all backups success",
			expectedBackups:    []string{"backup1", "backup2"},
			expectedError:      nil,
			expectedBodyJSON:   `["backup1","backup2"]`,
			expectedStatusCode: http.StatusOK,
		},
		{
			name:               "internal error listing backups",
			expectedBackups:    nil,
			expectedError:      errors.New("internal error"),
			expectedBodyJSON:   `{"error":"internal error"}`,
			expectedStatusCode: http.StatusInternalServerError,
		},
		// BWC: empty list → 200, []
		{
			name:               "empty list",
			expectedBackups:    nil,
			expectedError:      nil,
			expectedBodyJSON:   `[]`,
			expectedStatusCode: http.StatusOK,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockUseCase := NewMockBackupDaemonUseCase(ctrl)

			mockUseCase.EXPECT().
				ListBackups(gomock.Any(), gomock.Any()).
				Return(tc.expectedBackups, tc.expectedError).
				Times(1)

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(mockUseCase, mockUseCase, sugar)

			r := gin.New()
			r.GET("/backups", handler.ListBackups)

			req := httptest.NewRequest(http.MethodGet, "/backups", nil)
			req.Header.Set("Content-Type", "application/json")

			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if tc.expectedStatusCode != w.Code {
				t.Fatalf("expected status %d, got %d", tc.expectedStatusCode, w.Code)
			}

			if tc.expectedBodyJSON != strings.TrimSpace(w.Body.String()) {
				t.Fatalf("expected body %s, got %s", tc.expectedBodyJSON, w.Body.String())
			}
		})
	}
}

func TestListBackupByVaultHandler(t *testing.T) {
	testCases := []struct {
		name               string
		vault              string
		mockResponse       map[string]interface{}
		mockError          error
		expectedBodyJSON   string
		expectedStatusCode int
	}{
		{
			name:  "success get backup stats",
			vault: "backup1",
			mockResponse: map[string]interface{}{
				"id":    "backup1",
				"size":  "12756b",
				"valid": true,
			},
			mockError:          nil,
			expectedBodyJSON:   `{"id":"backup1","size":"12756b","valid":true}`,
			expectedStatusCode: http.StatusOK,
		},
		// BWC: "not found" in error → 404 (not 500)
		{
			name:               "not found",
			vault:              "nonexistent-vault-99999",
			mockResponse:       nil,
			mockError:          errors.New("backup nonexistent-vault-99999 not found"),
			expectedBodyJSON:   `{"message":"backup nonexistent-vault-99999 not found"}`,
			expectedStatusCode: http.StatusNotFound,
		},
		{
			name:               "internal error",
			vault:              "missing",
			mockResponse:       nil,
			mockError:          errors.New("backup missing not found"),
			expectedBodyJSON:   `{"message":"backup missing not found"}`,
			expectedStatusCode: http.StatusNotFound,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockUseCase := NewMockBackupDaemonUseCase(ctrl)

			mockUseCase.EXPECT().
				ListBackup(gomock.Any(), gomock.Any(), tc.vault).
				Return(tc.mockResponse, tc.mockError).
				Times(1)

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(mockUseCase, mockUseCase, sugar)

			r := gin.New()
			r.GET("/listbackups/:vault", handler.ListBackupByVault)

			req := httptest.NewRequest(http.MethodGet, "/listbackups/"+tc.vault, nil)
			req.Header.Set("Content-Type", "application/json")

			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if tc.expectedStatusCode != w.Code {
				t.Fatalf("expected status %d, got %d", tc.expectedStatusCode, w.Code)
			}

			if strings.TrimSpace(w.Body.String()) != tc.expectedBodyJSON {
				t.Fatalf("expected body %s, got %s", tc.expectedBodyJSON, w.Body.String())
			}
		})
	}
}

func TestHealth(t *testing.T) {
	testCases := []struct {
		name               string
		expectedResponse   entity.HealthResponse
		expectedError      error
		expectedStatusCode int
		expectedBodyJSON   string
	}{
		{
			name: "UP status",
			expectedResponse: entity.HealthResponse{
				Status:          "UP",
				BackupQueueSize: 0,
				Storage:         entity.StorageInfo{DumpCount: 2},
			},
			expectedError:      nil,
			expectedStatusCode: http.StatusOK,
			expectedBodyJSON:   `"UP"`,
		},
		// BWC: last backup failed → "Warning"
		{
			name: "Warning status",
			expectedResponse: entity.HealthResponse{
				Status:          "Warning",
				BackupQueueSize: 0,
				Storage:         entity.StorageInfo{DumpCount: 1},
			},
			expectedError:      nil,
			expectedStatusCode: http.StatusOK,
			expectedBodyJSON:   `"Warning"`,
		},
		{
			name:               "internal error",
			expectedResponse:   entity.HealthResponse{},
			expectedError:      errors.New("storage error"),
			expectedStatusCode: http.StatusInternalServerError,
			expectedBodyJSON:   `"message"`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockUseCase := NewMockBackupDaemonUseCase(ctrl)
			mockUseCase.EXPECT().GetHealth(gomock.Any(), gomock.Any()).Return(tc.expectedResponse, tc.expectedError).AnyTimes()

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(mockUseCase, mockUseCase, sugar)

			r := gin.Default()
			r.GET("/health", handler.Health)

			req := httptest.NewRequest(http.MethodGet, "/health", nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if tc.expectedStatusCode != w.Code {
				t.Fatalf("expected status %d, got %d", tc.expectedStatusCode, w.Code)
			}
			if !strings.Contains(w.Body.String(), tc.expectedBodyJSON) {
				t.Fatalf("expected body to contain %s, got %s", tc.expectedBodyJSON, w.Body.String())
			}
		})
	}
}

func TestHealthPrometheus(t *testing.T) {
	testCases := []struct {
		name               string
		expectedResponse   entity.HealthResponse
		expectedError      error
		expectedStatusCode int
		expectedContains   string
	}{
		// BWC: returns 200, text body with backup_daemon_status and backup_storage_dump_count
		{
			name: "success",
			expectedResponse: entity.HealthResponse{
				Status:          "UP",
				BackupQueueSize: 1,
				Storage:         entity.StorageInfo{DumpCount: 3},
			},
			expectedError:      nil,
			expectedStatusCode: http.StatusOK,
			expectedContains:   "backup_daemon_status",
		},
		{
			name:               "internal error",
			expectedResponse:   entity.HealthResponse{},
			expectedError:      errors.New("storage error"),
			expectedStatusCode: http.StatusInternalServerError,
			expectedContains:   "error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockUseCase := NewMockBackupDaemonUseCase(ctrl)
			mockUseCase.EXPECT().GetHealth(gomock.Any(), gomock.Any()).Return(tc.expectedResponse, tc.expectedError).AnyTimes()

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(mockUseCase, mockUseCase, sugar)

			r := gin.Default()
			r.GET("/health/prometheus", handler.HealthPrometheus)

			req := httptest.NewRequest(http.MethodGet, "/health/prometheus", nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if tc.expectedStatusCode != w.Code {
				t.Fatalf("expected status %d, got %d", tc.expectedStatusCode, w.Code)
			}
			if !strings.Contains(w.Body.String(), tc.expectedContains) {
				t.Fatalf("expected body to contain %s, got %s", tc.expectedContains, w.Body.String())
			}
		})
	}
}

func TestFind(t *testing.T) {
	statsResp := map[string]interface{}{
		"id": "20260319T120718", "failed": false, "locked": false,
		"ts": float64(1773922038000), "is_granular": false,
		"valid": true, "evictable": true, "size": "12594b", "spent_time": "2226ms",
	}

	testCases := []struct {
		name               string
		url                string
		bodyJSON           string
		mockResponse       map[string]interface{}
		mockError          error
		expectedStatusCode int
		expectedContains   string
	}{

		// BWC: ts via body (legacy) → 200
		{
			name:               "by body legacy",
			url:                "/find",
			bodyJSON:           `{"ts":"0"}`,
			mockResponse:       statsResp,
			mockError:          nil,
			expectedStatusCode: http.StatusOK,
			expectedContains:   `"id"`,
		},
		// BWC: no ts → 404
		{
			name:               "no ts param",
			url:                "/find",
			bodyJSON:           "",
			mockResponse:       nil,
			mockError:          nil,
			expectedStatusCode: http.StatusNotFound,
			expectedContains:   `ts`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockUseCase := NewMockBackupDaemonUseCase(ctrl)
			mockUseCase.EXPECT().Find(gomock.Any(), gomock.Any()).Return(tc.mockResponse, tc.mockError).AnyTimes()

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(mockUseCase, mockUseCase, sugar)

			r := gin.Default()
			r.GET("/find", handler.Find)

			var body *bytes.Buffer
			if tc.bodyJSON != "" {
				body = bytes.NewBufferString(tc.bodyJSON)
			} else {
				body = bytes.NewBuffer(nil)
			}
			req := httptest.NewRequest(http.MethodGet, tc.url, body)
			req.Header.Set("Content-Type", "application/json")
			if tc.bodyJSON != "" {
				req.ContentLength = int64(len(tc.bodyJSON))
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if tc.expectedStatusCode != w.Code {
				t.Fatalf("expected status %d, got %d; body: %s", tc.expectedStatusCode, w.Code, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), tc.expectedContains) {
				t.Fatalf("expected body to contain %s, got %s", tc.expectedContains, w.Body.String())
			}
		})
	}
}

func TestTerminate(t *testing.T) {
	testCases := []struct {
		name               string
		backupID           string
		bodyJSON           string
		expectedError      error
		expectedStatusCode int
		expectedBodyJSON   string
	}{
		// BWC: nonexistent vault → 404
		{
			name:               "not found",
			backupID:           "nonexistent-99999",
			bodyJSON:           `{}`,
			expectedError:      controller.ErrVaultNotFound,
			expectedStatusCode: http.StatusNotFound,
			expectedBodyJSON:   `"Failed"`,
		},
		// BWC: completed (not running) → 406
		{
			name:               "not running",
			backupID:           "20260319T120718",
			bodyJSON:           `{}`,
			expectedError:      fmt.Errorf("already done: %w", controller.ErrBackupNotRunning),
			expectedStatusCode: http.StatusNotAcceptable,
			expectedBodyJSON:   `"Failed"`,
		},
		// BWC: success → 200
		{
			name:               "success",
			backupID:           "20260319T120718",
			bodyJSON:           `{}`,
			expectedError:      nil,
			expectedStatusCode: http.StatusOK,
			expectedBodyJSON:   `"OK"`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockUseCase := NewMockBackupDaemonUseCase(ctrl)
			mockUseCase.EXPECT().TerminateBackup(gomock.Any(), gomock.Any()).Return(tc.expectedError).AnyTimes()

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(mockUseCase, mockUseCase, sugar)

			r := gin.Default()
			r.POST("/terminate/:backup_id", handler.Terminate)

			req := httptest.NewRequest(http.MethodPost, "/terminate/"+tc.backupID, bytes.NewBufferString(tc.bodyJSON))
			req.Header.Set("Content-Type", "application/json")
			req.ContentLength = int64(len(tc.bodyJSON))
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if tc.expectedStatusCode != w.Code {
				t.Fatalf("expected status %d, got %d; body: %s", tc.expectedStatusCode, w.Code, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), tc.expectedBodyJSON) {
				t.Fatalf("expected body to contain %s, got %s", tc.expectedBodyJSON, w.Body.String())
			}
		})
	}
}

func TestEvictionPolicy(t *testing.T) {
	testCases := []struct {
		name               string
		bodyJSON           string
		expectedError      error
		expectedStatusCode int
		expectedBodyJSON   string
	}{
		// BWC: valid policy → 200, body contains "Ok"
		{
			name:               "success",
			bodyJSON:           `{"fullEvictionPolicy":"1h/1h"}`,
			expectedError:      nil,
			expectedStatusCode: http.StatusOK,
			expectedBodyJSON:   `Ok`,
		},
		// BWC: missing required field → 500
		{
			name:               "missing field",
			bodyJSON:           `{}`,
			expectedError:      errors.New("fullEvictionPolicy is required"),
			expectedStatusCode: http.StatusInternalServerError,
			expectedBodyJSON:   `"message"`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockUseCase := NewMockBackupDaemonUseCase(ctrl)
			mockUseCase.EXPECT().UpdateEvictionPolicy(gomock.Any(), gomock.Any()).Return(tc.expectedError).AnyTimes()

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(mockUseCase, mockUseCase, sugar)

			r := gin.Default()
			r.POST("/evictionpolicy", handler.EvictionPolicy)

			req := httptest.NewRequest(http.MethodPost, "/evictionpolicy", bytes.NewBufferString(tc.bodyJSON))
			req.Header.Set("Content-Type", "application/json")
			req.ContentLength = int64(len(tc.bodyJSON))
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if tc.expectedStatusCode != w.Code {
				t.Fatalf("expected status %d, got %d; body: %s", tc.expectedStatusCode, w.Code, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), tc.expectedBodyJSON) {
				t.Fatalf("expected body to contain %s, got %s", tc.expectedBodyJSON, w.Body.String())
			}
		})
	}
}

func TestDownloadBackup_LocalSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tmpDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmpDir, "db.tar.gz"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	mockUseCase := NewMockBackupDaemonUseCase(ctrl)
	mockUseCase.EXPECT().DownloadBackup(gomock.Any(), "20260319T120718").Return(tmpDir, func() {}, nil)

	handler := NewEndpointHandler(mockUseCase, mockUseCase, zap.NewNop().Sugar())
	r := gin.Default()
	r.GET("/backup/:backup_id", handler.DownloadBackup)

	req := httptest.NewRequest(http.MethodGet, "/backup/20260319T120718", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Header().Get("Content-Disposition"), "20260319T120718.zip") {
		t.Fatalf("unexpected Content-Disposition: %s", w.Header().Get("Content-Disposition"))
	}

	zr, err := zip.NewReader(bytes.NewReader(w.Body.Bytes()), int64(w.Body.Len()))
	if err != nil {
		t.Fatalf("response is not a valid zip: %v", err)
	}
	if len(zr.File) != 1 || zr.File[0].Name != "db.tar.gz" {
		t.Fatalf("unexpected zip contents: %v", zr.File)
	}
}

func TestDownloadBackup_S3Success_CleanupCalled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tmpDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmpDir, "db.tar.gz"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	cleanupCalled := false
	cleanup := func() { cleanupCalled = true }

	mockUseCase := NewMockBackupDaemonUseCase(ctrl)
	mockUseCase.EXPECT().DownloadBackup(gomock.Any(), "20260319T120718").Return(tmpDir, cleanup, nil)

	handler := NewEndpointHandler(mockUseCase, mockUseCase, zap.NewNop().Sugar())
	r := gin.Default()
	r.GET("/backup/:backup_id", handler.DownloadBackup)

	req := httptest.NewRequest(http.MethodGet, "/backup/20260319T120718", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if !cleanupCalled {
		t.Fatal("cleanup func was not called after response")
	}
}

func TestDownloadBackup(t *testing.T) {
	testCases := []struct {
		name               string
		backupID           string
		folder             string
		expectedError      error
		expectedStatusCode int
	}{
		// BWC: nonexistent vault → 204
		{
			name:               "not found",
			backupID:           "nonexistent-99999",
			folder:             "",
			expectedError:      controller.ErrVaultNotFound,
			expectedStatusCode: http.StatusNoContent,
		},
		// BWC: internal error → 500
		{
			name:               "internal error",
			backupID:           "20260319T120718",
			folder:             "",
			expectedError:      errors.New("read error"),
			expectedStatusCode: http.StatusInternalServerError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockUseCase := NewMockBackupDaemonUseCase(ctrl)
			mockUseCase.EXPECT().DownloadBackup(gomock.Any(), gomock.Any()).Return(tc.folder, func() {}, tc.expectedError).AnyTimes()

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(mockUseCase, mockUseCase, sugar)

			r := gin.Default()
			r.GET("/backup/:backup_id", handler.DownloadBackup)

			req := httptest.NewRequest(http.MethodGet, "/backup/"+tc.backupID, nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if tc.expectedStatusCode != w.Code {
				t.Fatalf("expected status %d, got %d; body: %s", tc.expectedStatusCode, w.Code, w.Body.String())
			}
		})
	}
}
