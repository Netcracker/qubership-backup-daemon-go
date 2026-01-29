package rest

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/gin-gonic/gin"
	"go.uber.org/mock/gomock"
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
			expectedBodyJSON:   `{"backup_id":"coverageo"}`,
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
			expectedBodyJSON:   `{"message":"failed to enqueue backup err: internal error"}`,
			expectedStatusCode: http.StatusInternalServerError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockStorageRepo := NewMockBackupDaemonUseCase(ctrl)
			mockStorageRepo.EXPECT().EnqueueBackup(gomock.Any(), gomock.Any()).Return(tc.expectedResponse, tc.expectedError).AnyTimes()

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(mockStorageRepo, sugar)

			r := gin.Default()
			r.POST("/incremental/backup", handler.Backup)

			req := httptest.NewRequest(http.MethodPost, "/incremental/backup", bytes.NewBufferString(tc.requestBodyJSON))
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

func TestRestoreBackup(t *testing.T) {
	testCases := []struct {
		name               string
		requestBodyJSON    string
		expectedResponse   entity.RestoreResponse
		expectedBodyJSON   string
		expectedStatusCode int
		expectedError      error
	}{
		{
			name:            "success",
			requestBodyJSON: `{"externalBackupPath": "./app/repo/coverageo"}`,
			expectedResponse: entity.RestoreResponse{
				TaskID: "coverageo",
			},
			expectedBodyJSON:   `{"task_id":"coverageo"}`,
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
			requestBodyJSON:    `{"externalBackupPath": "./app/repo/covrago"}`,
			expectedResponse:   entity.RestoreResponse{},
			expectedError:      errors.New("internal error"),
			expectedBodyJSON:   `{"message":"failed to restore backup err: internal error"}`,
			expectedStatusCode: http.StatusInternalServerError,
		},
		{
			name:               "not found",
			requestBodyJSON:    `{"dd": "./app/repo/covrago"}`,
			expectedResponse:   entity.RestoreResponse{},
			expectedError:      nil,
			expectedBodyJSON:   `{"message":"Sorry, wrong JSON string. No 'vault' or 'ts' parameter."}`,
			expectedStatusCode: http.StatusNotFound,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockStorageRepo := NewMockBackupDaemonUseCase(ctrl)
			mockStorageRepo.EXPECT().RestoreBackup(gomock.Any(), gomock.Any()).Return(tc.expectedResponse, tc.expectedError).AnyTimes()

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(mockStorageRepo, sugar)

			r := gin.Default()
			r.POST("/restore", handler.Restore)

			req := httptest.NewRequest(http.MethodPost, "/restore", bytes.NewBufferString(tc.requestBodyJSON))
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
			expectedBodyJSON:   `{"message":"OK"}`,
			expectedStatusCode: http.StatusOK,
		},
		{
			name:               "internal error",
			expectedError:      errors.New("internal error"),
			expectedBodyJSON:   `{"message":"failed to enqueue eviction err: internal error"}`,
			expectedStatusCode: http.StatusInternalServerError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockStorageRepo := NewMockBackupDaemonUseCase(ctrl)
			mockStorageRepo.EXPECT().EnqueueEviction(gomock.Any(), gomock.Any()).Return(tc.expectedError).AnyTimes()

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(mockStorageRepo, sugar)

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
		expectedError      error
		expectedBodyJSON   string
		expectedStatusCode int
	}{
		{
			name:               "success",
			expectedError:      nil,
			expectedBodyJSON:   `{"message":"OK"}`,
			expectedStatusCode: http.StatusOK,
		},
		{
			name:               "internal error",
			expectedError:      errors.New("internal error"),
			expectedBodyJSON:   `{"message":"failed to remove backup err: internal error"}`,
			expectedStatusCode: http.StatusInternalServerError,
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
			handler := NewEndpointHandler(mockStorageRepo, sugar)

			r := gin.Default()
			r.POST("/evict/:vault", handler.EvictByVault)

			req := httptest.NewRequest(http.MethodPost, "/evict/eeee", nil)
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
			mockStorageRepo := NewMockBackupDaemonUseCase(ctrl)
			mockStorageRepo.EXPECT().RestoreBackup(gomock.Any(), gomock.Any()).Return(tc.expectedResponse, tc.expectedError).AnyTimes()

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(mockStorageRepo, sugar)

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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockStorageRepo := NewMockBackupDaemonUseCase(ctrl)
			mockStorageRepo.EXPECT().GetJobStatus(gomock.Any(), gomock.Any()).Return(tc.expectedResponse, tc.expectedError).AnyTimes()

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(mockStorageRepo, sugar)

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
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockStorageRepo := NewMockBackupDaemonUseCase(ctrl)
			mockStorageRepo.EXPECT().CreateS3PresignedURL(gomock.Any(), gomock.Any()).Return(tc.expectedResponse, tc.expectedError).AnyTimes()

			sugar := zap.NewNop().Sugar()
			handler := NewEndpointHandler(mockStorageRepo, sugar)

			r := gin.Default()
			r.GET("/backup/s3/:backup_id", handler.S3PresignedURL)

			req := httptest.NewRequest(http.MethodGet,
				fmt.Sprintf("/backup/s3/20210601T115105?expiration=%s", tc.expirationTime),
				nil)
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
