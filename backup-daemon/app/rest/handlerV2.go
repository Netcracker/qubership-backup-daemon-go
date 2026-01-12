package rest

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/gin-gonic/gin"
)

func (h *EndpointHandler) BackupV2(ctx *gin.Context) {
	var req entity.BackupV2Request
	if err := ctx.ShouldBindJSON(&req); err != nil && ctx.Request.ContentLength > 0 {
		ctx.JSON(http.StatusBadRequest, gin.H{"message": fmt.Sprintf("failed to unmarshall body err: %v", err)})
		return
	}

	blob, err := validateBlobPath(req.BlobPath)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}
	req.BlobPath = blob
	if req.Databases == nil {
		req.Databases = []string{}
	}

	internal := mapBackupV2ToInternal(req, getProcType(ctx.Request.URL.Path))

	resp, err := h.backupDaemonUseCase.EnqueueBackup(ctx, internal)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprintf("failed to enqueue backup err: %v", err)})
		return
	}

	ctx.JSON(http.StatusOK, buildBackupV2Response(req, resp.BackupID, NotStarted))
}

func (h *EndpointHandler) BackupV2Status(ctx *gin.Context) {
	backupID := ctx.Param("backup_id")

	js, err := h.backupDaemonUseCase.GetJobStatus(ctx, entity.JobStatusRequest{TaskID: backupID})
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprintf("failed to get job status err: %v", err)})
		return
	}
	if js.StatusCode == http.StatusNotFound {
		ctx.JSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("Sorry, no job '%s' recorded in database", backupID)})
		return
	}

	status := mapJobStatus(js.Status)
	blob := normalizeBlobPath(js.BlobPath)
	storage := strings.TrimSpace(js.StorageName)

	dbs := js.Databases
	resp := entity.BackupV2Response{
		Status:       status,
		BackupID:     backupID,
		CreationTime: timeCreationNow(),
		StorageName:  storage,
		BlobPath:     blob,
		Databases:    DbStatuses(dbs, status),
	}

	ctx.JSON(http.StatusOK, resp)
}

func (h *EndpointHandler) BackupV2Delete(ctx *gin.Context) {
	backupID := strings.TrimSpace(ctx.Param("backup_id"))
	if backupID == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"message": "backup_id is required"})
		return
	}
	blob := normalizeBlobPath(ctx.Query("blobPath"))
	if blob == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"message": "blobPath is required"})
		return
	}
	err := h.backupDaemonUseCase.RemoveBackupV2(ctx, entity.EvictByVaultV2Request{
		Vault:    backupID,
		BlobPath: blob,
	})
	if err != nil {
		msg := err.Error()

		switch {
		case strings.Contains(msg, "not found"):
			ctx.JSON(http.StatusNotFound, gin.H{"message": msg})
		case strings.Contains(msg, "locked"):
			ctx.JSON(http.StatusConflict, gin.H{"message": msg})
		default:
			ctx.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprintf("failed to delete backup: %v", err)})
		}
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"message":  "OK",
		"backupId": backupID,
		"blobPath": blob,
	})
}

func (h *EndpointHandler) RestoreV2(ctx *gin.Context) {
	backupID := ctx.Param("backup_id")

	var req entity.RestoreV2Request
	if err := ctx.ShouldBindJSON(&req); err != nil && ctx.Request.ContentLength > 0 {
		ctx.JSON(http.StatusBadRequest, gin.H{"message": fmt.Sprintf("failed to unmarshall body err: %v", err)})
		return
	}

	blob, err := validateBlobPath(req.BlobPath)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}
	req.BlobPath = blob

	if req.Databases == nil {
		req.Databases = []entity.RestoreDBMap{}
	}
	for _, m := range req.Databases {
		if strings.TrimSpace(m.PreviousDatabaseName) == "" || strings.TrimSpace(m.DatabaseName) == "" {
			ctx.JSON(http.StatusBadRequest, gin.H{"message": "each databases item must have previousDatabaseName and databaseName"})
			return
		}
	}

	internal := mapRestoreV2ToInternal(backupID, req, getProcType(ctx.Request.URL.Path))

	resp, err := h.backupDaemonUseCase.RestoreBackup(ctx, internal)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprintf("failed to restore backup err: %v", err)})
		return
	}

	ctx.JSON(http.StatusOK, buildRestoreV2Response(req, resp.TaskID, NotStarted))
}

func (h *EndpointHandler) RestoreV2Status(ctx *gin.Context) {
	taskID := strings.TrimSpace(ctx.Param("restore_id"))
	if taskID == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"message": "restore_id is required"})
		return
	}

	js, err := h.backupDaemonUseCase.GetJobStatus(ctx, entity.JobStatusRequest{TaskID: taskID})
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprintf("failed to get job status err: %v", err)})
		return
	}

	if js.Type != "incremental restore" && js.Type != "restore" {
		ctx.JSON(http.StatusNotFound, gin.H{
			"message": fmt.Sprintf("job '%s' is not a restore task", taskID),
		})
		return
	}
	if js.StatusCode == http.StatusNotFound {
		ctx.JSON(http.StatusNotFound, gin.H{"message": fmt.Sprintf("Sorry, no job '%s' recorded in database", taskID)})
		return
	}

	status := mapJobStatus(js.Status)

	resp := entity.RestoreV2Response{
		Status:       status,
		RestoreID:    taskID,
		CreationTime: timeCreationNow(),
		StorageName:  js.StorageName,
		BlobPath:     js.BlobPath,
		Databases:    DbStatuses(js.Databases, status),
	}

	ctx.JSON(http.StatusOK, resp)
}
