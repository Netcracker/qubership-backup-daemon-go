package rest

import (
	"net/http"
	"strings"
	"time"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type MarkerExecutor interface {
	ExecuteMarkerSetCmd(marker string) error
	ExecuteMarkerGetCmd() (string, error)
}

type MarkerHandler struct {
	executor MarkerExecutor
	logger   *zap.SugaredLogger
}

func NewMarkerHandler(executor MarkerExecutor, logger *zap.SugaredLogger) *MarkerHandler {
	return &MarkerHandler{
		executor: executor,
		logger:   logger,
	}
}

func (h *MarkerHandler) SetMarker(ctx *gin.Context) {
	var req entity.MarkerRequest
	if err := ctx.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Marker) == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"message": "marker field is required"})
		return
	}

	if err := validateMarker(req.Marker); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}

	if h.executor != nil {
		if err := h.executor.ExecuteMarkerSetCmd(req.Marker); err != nil {
			h.logger.Errorw("marker set command failed", "error", err)
			ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
			return
		}
	}

	ctx.Status(http.StatusCreated)
}

func (h *MarkerHandler) GetMarker(ctx *gin.Context) {
	if h.executor == nil {
		ctx.JSON(http.StatusNotFound, gin.H{"message": "no data-validation marker has been set"})
		return
	}

	marker, err := h.executor.ExecuteMarkerGetCmd()
	if err != nil {
		h.logger.Errorw("marker get command failed", "error", err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
		return
	}

	if marker == "" {
		ctx.JSON(http.StatusNotFound, gin.H{"message": "no data-validation marker has been set"})
		return
	}

	ctx.JSON(http.StatusOK, entity.MarkerResponse{Marker: marker})
}

// validateMarker checks format: "<backup_name>/<RFC3339 timestamp>"
func validateMarker(marker string) error {
	if strings.ContainsAny(marker, " \t") {
		return &markerFormatError{"marker must not contain spaces or tabs"}
	}
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
