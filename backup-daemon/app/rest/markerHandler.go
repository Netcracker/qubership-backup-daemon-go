package rest

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/tasks"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// apiError matches the qubership error format.
type apiError struct {
	Status int    `json:"status"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
}

type MarkerHandler struct {
	mu       sync.RWMutex
	marker   *string
	executor tasks.CommandExecutor
	logger   *zap.SugaredLogger
}

func NewMarkerHandler(executor tasks.CommandExecutor, logger *zap.SugaredLogger) *MarkerHandler {
	return &MarkerHandler{
		executor: executor,
		logger:   logger,
	}
}

func (h *MarkerHandler) SetMarker(ctx *gin.Context) {
	var req entity.MarkerRequest
	if err := ctx.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Marker) == "" {
		ctx.JSON(http.StatusBadRequest, apiError{
			Status: http.StatusBadRequest,
			Title:  "Bad Request",
			Detail: "marker field is required",
		})
		return
	}

	if err := validateMarker(req.Marker); err != nil {
		ctx.JSON(http.StatusBadRequest, apiError{
			Status: http.StatusBadRequest,
			Title:  "Bad Request",
			Detail: err.Error(),
		})
		return
	}

	if h.executor != nil {
		if err := h.executor.ExecuteMarkerSetCmd(req.Marker); err != nil {
			h.logger.Errorw("marker set command failed", "error", err)
			ctx.JSON(http.StatusInternalServerError, apiError{
				Status: http.StatusInternalServerError,
				Title:  "Internal Server Error",
				Detail: err.Error(),
			})
			return
		}
	}

	h.mu.Lock()
	v := req.Marker
	h.marker = &v
	h.mu.Unlock()

	ctx.Status(http.StatusCreated)
}

func (h *MarkerHandler) GetMarker(ctx *gin.Context) {
	h.mu.RLock()
	m := h.marker
	h.mu.RUnlock()

	if m == nil {
		ctx.JSON(http.StatusNotFound, apiError{
			Status: http.StatusNotFound,
			Title:  "Not Found",
			Detail: "no data-validation marker has been set",
		})
		return
	}

	if h.executor != nil {
		if err := h.executor.ExecuteMarkerValidateCmd(*m); err != nil {
			h.logger.Errorw("marker validate command failed", "error", err)
			ctx.JSON(http.StatusInternalServerError, apiError{
				Status: http.StatusInternalServerError,
				Title:  "Internal Server Error",
				Detail: err.Error(),
			})
			return
		}
	}

	ctx.JSON(http.StatusOK, entity.MarkerResponse{Marker: *m})
}

// validateMarker checks format: "<backup_name>/<RFC3339 timestamp>"
func validateMarker(marker string) error {
	idx := strings.LastIndex(marker, "/")
	if idx < 0 {
		return &markerFormatError{"marker must be in format \"<backup_name>/<RFC3339 timestamp>\""}
	}
	ts := marker[idx+1:]
	if _, err := time.Parse(time.RFC3339, ts); err != nil {
		return &markerFormatError{"timestamp portion must be a valid RFC3339 value, e.g. 2024-01-15T12:00:00Z"}
	}
	return nil
}

type markerFormatError struct{ msg string }

func (e *markerFormatError) Error() string { return e.msg }
