package rest

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/tasks"
	"github.com/gin-gonic/gin"
	gomock "go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func newMarkerRouter(h *MarkerHandler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/data-validation/marker", h.SetMarker)
	r.GET("/api/v1/data-validation/marker", h.GetMarker)
	return r
}

// --- SetMarker (POST) ---

func TestSetMarker_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	exec := tasks.NewMockCommandExecutor(ctrl)
	exec.EXPECT().ExecuteMarkerSetCmd("my-backup/2024-01-15T12:00:00Z").Return(nil)

	h := NewMarkerHandler(exec, zap.NewNop().Sugar())
	r := newMarkerRouter(h)

	body := `{"marker":"my-backup/2024-01-15T12:00:00Z"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/data-validation/marker", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
}

func TestSetMarker_NoExecutor(t *testing.T) {
	h := NewMarkerHandler(nil, zap.NewNop().Sugar())
	r := newMarkerRouter(h)

	body := `{"marker":"backup/2024-06-17T00:00:00Z"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/data-validation/marker", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
}

func TestSetMarker_MissingField(t *testing.T) {
	h := NewMarkerHandler(nil, zap.NewNop().Sugar())
	r := newMarkerRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/data-validation/marker", bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestSetMarker_InvalidTimestamp(t *testing.T) {
	h := NewMarkerHandler(nil, zap.NewNop().Sugar())
	r := newMarkerRouter(h)

	body := `{"marker":"my-backup/not-a-timestamp"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/data-validation/marker", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestSetMarker_NoSlash(t *testing.T) {
	h := NewMarkerHandler(nil, zap.NewNop().Sugar())
	r := newMarkerRouter(h)

	body := `{"marker":"justanametimestamp"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/data-validation/marker", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestSetMarker_CommandError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	exec := tasks.NewMockCommandExecutor(ctrl)
	exec.EXPECT().ExecuteMarkerSetCmd(gomock.Any()).Return(errors.New("script failed"))

	h := NewMarkerHandler(exec, zap.NewNop().Sugar())
	r := newMarkerRouter(h)

	body := `{"marker":"my-backup/2024-01-15T12:00:00Z"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/data-validation/marker", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", w.Code, w.Body.String())
	}
}

// --- GetMarker (GET) ---

func TestGetMarker_NotSet(t *testing.T) {
	h := NewMarkerHandler(nil, zap.NewNop().Sugar())
	r := newMarkerRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/data-validation/marker", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestGetMarker_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	exec := tasks.NewMockCommandExecutor(ctrl)
	exec.EXPECT().ExecuteMarkerSetCmd("my-backup/2024-01-15T12:00:00Z").Return(nil)
	exec.EXPECT().ExecuteMarkerValidateCmd("my-backup/2024-01-15T12:00:00Z").Return(nil)

	h := NewMarkerHandler(exec, zap.NewNop().Sugar())
	r := newMarkerRouter(h)

	// First set the marker
	body := `{"marker":"my-backup/2024-01-15T12:00:00Z"}`
	setReq := httptest.NewRequest(http.MethodPost, "/api/v1/data-validation/marker", bytes.NewBufferString(body))
	setReq.Header.Set("Content-Type", "application/json")
	setW := httptest.NewRecorder()
	r.ServeHTTP(setW, setReq)
	if setW.Code != http.StatusCreated {
		t.Fatalf("setup POST failed: %d %s", setW.Code, setW.Body.String())
	}

	// Then get it
	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/data-validation/marker", nil)
	getW := httptest.NewRecorder()
	r.ServeHTTP(getW, getReq)

	if getW.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", getW.Code, getW.Body.String())
	}
	var resp entity.MarkerResponse
	if err := json.Unmarshal(getW.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if resp.Marker != "my-backup/2024-01-15T12:00:00Z" {
		t.Errorf("unexpected marker: %q", resp.Marker)
	}
}

func TestGetMarker_OverwritesPrevious(t *testing.T) {
	h := NewMarkerHandler(nil, zap.NewNop().Sugar())
	r := newMarkerRouter(h)

	for _, m := range []string{"backup-a/2024-01-01T00:00:00Z", "backup-b/2024-06-01T00:00:00Z"} {
		body, _ := json.Marshal(entity.MarkerRequest{Marker: m})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/data-validation/marker", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusCreated {
			t.Fatalf("POST %q failed: %d %s", m, w.Code, w.Body.String())
		}
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/data-validation/marker", nil)
	getW := httptest.NewRecorder()
	r.ServeHTTP(getW, getReq)

	if getW.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", getW.Code)
	}
	var resp entity.MarkerResponse
	_ = json.Unmarshal(getW.Body.Bytes(), &resp)
	if resp.Marker != "backup-b/2024-06-01T00:00:00Z" {
		t.Errorf("expected last marker, got %q", resp.Marker)
	}
}

func TestGetMarker_ValidateCommandError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	exec := tasks.NewMockCommandExecutor(ctrl)
	exec.EXPECT().ExecuteMarkerSetCmd(gomock.Any()).Return(nil)
	exec.EXPECT().ExecuteMarkerValidateCmd(gomock.Any()).Return(errors.New("validation failed"))

	h := NewMarkerHandler(exec, zap.NewNop().Sugar())
	r := newMarkerRouter(h)

	body := `{"marker":"my-backup/2024-01-15T12:00:00Z"}`
	setReq := httptest.NewRequest(http.MethodPost, "/api/v1/data-validation/marker", bytes.NewBufferString(body))
	setReq.Header.Set("Content-Type", "application/json")
	setW := httptest.NewRecorder()
	r.ServeHTTP(setW, setReq)

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/data-validation/marker", nil)
	getW := httptest.NewRecorder()
	r.ServeHTTP(getW, getReq)

	if getW.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", getW.Code, getW.Body.String())
	}
}
