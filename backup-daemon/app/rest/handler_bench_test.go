package rest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/gin-gonic/gin"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// newBenchHandler creates an EndpointHandler wired to gomock stubs.
// customVarNames lets you simulate the prod config (region, mode, topic_regex …).
func newBenchHandler(tb testing.TB, customVarNames ...string) (*EndpointHandler, *MockBackupDaemonUseCase) {
	ctrl := gomock.NewController(tb)
	mock := NewMockBackupDaemonUseCase(ctrl)
	h := NewEndpointHandler(mock, mock, zap.NewNop().Sugar(), customVarNames...)
	return h, mock
}

// ginCtx builds a *gin.Context backed by an httptest.ResponseRecorder.
func ginCtx(method, path, body string) (*gin.Context, *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(
		context.Background(), method, path,
		strings.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(body))
	c, _ := gin.CreateTestContext(w)
	c.Request = req
	return c, w
}

// ---------------------------------------------------------------------------
// BenchmarkBackupHandler — measures double JSON decode + key validation
// ---------------------------------------------------------------------------

// Payload without any custom vars — hits the "all known keys" fast path.
const backupBodySimple = `{"dbs":["db1","db2","db3"],"allow_eviction":"true","sharded":false}`

// Payload with custom vars — exercises the unknown-key loop + getCustomVarNames().
const backupBodyWithCustomVars = `{"dbs":["db1","db2"],"topic_regex":"my-topic","mode":"compact","region":"us-east-1"}`

// Large payload: 20 DBs + several custom vars — stress-tests allocations inside Backup.
var backupBodyLarge = func() string {
	var dbs []string
	for i := range 20 {
		dbs = append(dbs, fmt.Sprintf(`"topic-%02d"`, i))
	}
	return fmt.Sprintf(`{"dbs":[%s],"topic_regex":".*","mode":"full","region":"eu-west-1"}`,
		strings.Join(dbs, ","))
}()

func BenchmarkBackupHandler_SimpleBody(b *testing.B) {
	h, mock := newBenchHandler(b)
	mock.EXPECT().
		EnqueueBackup(gomock.Any(), gomock.Any()).
		Return(entity.BackupResponse{BackupID: "20060102T150405"}, nil).
		AnyTimes()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		c, _ := ginCtx(http.MethodPost, "/backup", backupBodySimple)
		h.Backup(c)
	}
}

func BenchmarkBackupHandler_WithCustomVars(b *testing.B) {
	h, mock := newBenchHandler(b, "topic_regex=", "mode=", "region=")
	mock.EXPECT().
		EnqueueBackup(gomock.Any(), gomock.Any()).
		Return(entity.BackupResponse{BackupID: "20060102T150405"}, nil).
		AnyTimes()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		c, _ := ginCtx(http.MethodPost, "/backup", backupBodyWithCustomVars)
		h.Backup(c)
	}
}

func BenchmarkBackupHandler_LargeBody(b *testing.B) {
	h, mock := newBenchHandler(b, "topic_regex=", "mode=", "region=")
	mock.EXPECT().
		EnqueueBackup(gomock.Any(), gomock.Any()).
		Return(entity.BackupResponse{BackupID: "20060102T150405"}, nil).
		AnyTimes()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		c, _ := ginCtx(http.MethodPost, "/backup", backupBodyLarge)
		h.Backup(c)
	}
}

// ---------------------------------------------------------------------------
// BenchmarkBackupHandler_EmptyBody — no Content-Length, skips JSON path
// ---------------------------------------------------------------------------

func BenchmarkBackupHandler_EmptyBody(b *testing.B) {
	h, mock := newBenchHandler(b)
	mock.EXPECT().
		EnqueueBackup(gomock.Any(), gomock.Any()).
		Return(entity.BackupResponse{BackupID: "20060102T150405"}, nil).
		AnyTimes()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		w := httptest.NewRecorder()
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "/backup", http.NoBody)
		req.ContentLength = 0
		c, _ := gin.CreateTestContext(w)
		c.Request = req
		h.Backup(c)
	}
}

// ---------------------------------------------------------------------------
// BenchmarkHealthPrometheus — measures fmt.Fprintf chain + strings.Builder
// ---------------------------------------------------------------------------

func BenchmarkHealthPrometheus_NoLastBackup(b *testing.B) {
	h, mock := newBenchHandler(b)
	mock.EXPECT().
		GetHealth(gomock.Any(), gomock.Any()).
		Return(entity.HealthResponse{
			Status:          "UP",
			BackupQueueSize: 0,
			Storage: entity.StorageInfo{
				DumpCount:   5,
				FreeSpace:   1024 * 1024 * 500,
				Size:        1024 * 1024 * 100,
				TotalSpace:  1024 * 1024 * 1024,
				FreeInodes:  100000,
				TotalInodes: 200000,
				UsedInodes:  100000,
			},
		}, nil).
		AnyTimes()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		c, _ := ginCtx(http.MethodGet, "/health/prometheus", "")
		h.HealthPrometheus(c)
	}
}

func BenchmarkHealthPrometheus_WithLastBackup(b *testing.B) {
	h, mock := newBenchHandler(b)
	mock.EXPECT().
		GetHealth(gomock.Any(), gomock.Any()).
		Return(entity.HealthResponse{
			Status:          "UP",
			BackupQueueSize: 2,
			Storage: entity.StorageInfo{
				DumpCount:   42,
				FreeSpace:   1024 * 1024 * 200,
				Size:        1024 * 1024 * 800,
				TotalSpace:  1024 * 1024 * 1024,
				FreeInodes:  80000,
				TotalInodes: 200000,
				UsedInodes:  120000,
				Last: entity.BackupInfo{
					ID:        "20260419T120000",
					TimeStamp: 1774000000000,
					Failed:    false,
					Locked:    false,
					Sharded:   false,
					Metrics:   entity.BackupMetrics{ExitCode: 0, SpentTime: 2500, Size: 1024 * 512},
				},
				LastSuccessful: entity.BackupInfo{
					ID:        "20260419T120000",
					TimeStamp: 1774000000000,
					Metrics:   entity.BackupMetrics{ExitCode: 0, SpentTime: 2500, Size: 1024 * 512},
				},
			},
		}, nil).
		AnyTimes()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		c, _ := ginCtx(http.MethodGet, "/health/prometheus", "")
		h.HealthPrometheus(c)
	}
}

// ---------------------------------------------------------------------------
// BenchmarkListBackups — measures JSON serialization of N vault names
// ---------------------------------------------------------------------------

func BenchmarkListBackups_10(b *testing.B)   { benchListBackups(b, 10) }
func BenchmarkListBackups_100(b *testing.B)  { benchListBackups(b, 100) }
func BenchmarkListBackups_1000(b *testing.B) { benchListBackups(b, 1000) }

func benchListBackups(b *testing.B, n int) {
	b.Helper()
	h, mock := newBenchHandler(b)
	names := make([]string, n)
	for i := range n {
		names[i] = fmt.Sprintf("2026%04d%02dT120000", i/100+1, i%12+1)
	}
	mock.EXPECT().
		ListBackups(gomock.Any(), gomock.Any()).
		Return(names, nil).
		AnyTimes()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		c, _ := ginCtx(http.MethodGet, "/listbackups", "")
		h.ListBackups(c)
	}
}

// ---------------------------------------------------------------------------
// BenchmarkFind — measures ShouldBindJSON + FindRequest.UnmarshalJSON
// ---------------------------------------------------------------------------

func BenchmarkFind_TSAsNumber(b *testing.B) {
	h, mock := newBenchHandler(b)
	mock.EXPECT().
		Find(gomock.Any(), gomock.Any()).
		Return(map[string]interface{}{"id": "20260419T120000", "ts": "1774000000000"}, nil).
		AnyTimes()

	body := `{"ts":1774000000000}`
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		c, _ := ginCtx(http.MethodGet, "/find", body)
		h.Find(c)
	}
}

func BenchmarkFind_TSAsString(b *testing.B) {
	h, mock := newBenchHandler(b)
	mock.EXPECT().
		Find(gomock.Any(), gomock.Any()).
		Return(map[string]interface{}{"id": "20260419T120000", "ts": "1774000000000"}, nil).
		AnyTimes()

	body := `{"ts":"1774000000000"}`
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		c, _ := ginCtx(http.MethodGet, "/find", body)
		h.Find(c)
	}
}
