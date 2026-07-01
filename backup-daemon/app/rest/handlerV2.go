package rest

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/controller"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/utils"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type EndpointHandlerV2 struct {
	fullBackup     controller.BackupDaemonUseCase
	s3Registry     utils.S3AliasRegistry
	logger         *zap.SugaredLogger
	customVarNames []string
}

func NewEndpointHandlerV2(full controller.BackupDaemonUseCase, s3Registry utils.S3AliasRegistry, logger *zap.SugaredLogger, customVarNames ...string) *EndpointHandlerV2 {
	return &EndpointHandlerV2{
		fullBackup:     full,
		s3Registry:     s3Registry,
		logger:         logger,
		customVarNames: customVarNames,
	}
}

func (h *EndpointHandlerV2) BackupV2(ctx *gin.Context) {
	var req entity.BackupV2Request
	if err := ctx.ShouldBindJSON(&req); err != nil && ctx.Request.ContentLength > 0 {
		msg := fmt.Sprintf("failed to unmarshall body err: %v", err)
		h.logger.Error(msg)
		ctx.JSON(http.StatusBadRequest, gin.H{"message": msg})
		return
	}

	blob, err := validateBlobPath(req.BlobPath)
	if err != nil {
		h.logger.Error(err.Error())
		ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}
	req.BlobPath = blob
	storageName, usedDefaultStorage := normalizeStorageName(req.StorageName)
	req.StorageName = storageName
	if usedDefaultStorage {
		h.logger.Infof("storageName is empty, using default storageName %q", req.StorageName)
	}

	if h.s3Registry != nil && !h.s3Registry.Has(req.StorageName) {
		msg := fmt.Sprintf("unknown storageName %q", req.StorageName)
		h.logger.Error(msg)
		ctx.JSON(http.StatusBadRequest, gin.H{"message": msg})
		return
	}
	if req.Databases == nil {
		req.Databases = []string{}
	}

	internal := mapBackupV2ToInternal(req, getProcType(ctx.Request.URL.Path))

	resp, err := h.fullBackup.EnqueueBackup(ctx, internal)
	if err != nil {
		msg := fmt.Sprintf("failed to enqueue backup err: %v", err)
		h.logger.Error(msg)
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": msg})
		return
	}

	ctx.JSON(http.StatusOK, buildBackupV2Response(req, resp.BackupID, NotStarted, resp.CreationTime))
}

func (h *EndpointHandlerV2) BackupV2Status(ctx *gin.Context) {
	backupID := ctx.Param("backup_id")

	js, err := h.fullBackup.GetJobStatus(ctx, entity.JobStatusRequest{TaskID: backupID})
	if err != nil {
		msg := fmt.Sprintf("failed to get job status err: %v", err)
		h.logger.Error(msg)
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": msg})
		return
	}
	if js.StatusCode == http.StatusNotFound {
		msg := fmt.Sprintf("Sorry, no job '%s' recorded in database", backupID)
		h.logger.Info(msg)
		ctx.JSON(http.StatusNotFound, gin.H{"message": msg})
		return
	}

	status := mapJobStatus(js.Status)
	storage := strings.TrimSpace(js.StorageName)

	var errMsg string
	if status == Failed {
		errMsg = js.Error
	}

	dbs := js.Databases
	resp := entity.BackupV2Response{
		Status:         status,
		ErrorMessage:   errMsg,
		BackupID:       backupID,
		CreationTime:   js.CreationTime,
		CompletionTime: js.CompletionTime,
		StorageName:    storage,
		BlobPath:       js.BlobPath,
		Databases:      DbStatuses(dbs, status, js.CreationTime, errMsg),
	}

	ctx.JSON(http.StatusOK, resp)
}

func (h *EndpointHandlerV2) BackupV2Delete(ctx *gin.Context) {
	backupID := strings.TrimSpace(ctx.Param("backup_id"))
	if backupID == "" {
		msg := "backup_id is required"
		h.logger.Error(msg)
		ctx.JSON(http.StatusBadRequest, gin.H{"message": msg})
		return
	}
	blob := normalizeBlobPath(ctx.Query("blobPath"))
	if blob == "" {
		msg := "blobPath is required"
		h.logger.Error(msg)
		ctx.JSON(http.StatusBadRequest, gin.H{"message": msg})
		return
	}
	err := h.fullBackup.RemoveBackupV2(ctx, entity.EvictByVaultV2Request{
		Vault:    backupID,
		BlobPath: blob,
	})
	if err != nil {
		msg := err.Error()

		switch {
		case strings.Contains(msg, "no job found"):
			h.logger.Info(msg)
			ctx.JSON(http.StatusNotFound, gin.H{"message": msg})
		case strings.Contains(msg, "locked"):
			h.logger.Info(msg)
			ctx.JSON(http.StatusConflict, gin.H{"message": msg})
		default:
			h.logger.Error(msg)
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

func (h *EndpointHandlerV2) RestoreV2(ctx *gin.Context) {
	backupID := ctx.Param("backup_id")
	var req entity.RestoreV2Request
	if err := ctx.ShouldBindJSON(&req); err != nil && ctx.Request.ContentLength > 0 {
		msg := fmt.Sprintf("failed to unmarshall body err: %v", err)
		h.logger.Error(msg)
		ctx.JSON(http.StatusBadRequest, gin.H{"message": msg})
		return
	}

	blob, err := validateBlobPath(req.BlobPath)
	if err != nil {
		h.logger.Error(err.Error())
		ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}
	req.BlobPath = blob
	storageName, usedDefaultStorage := normalizeStorageName(req.StorageName)
	req.StorageName = storageName

	if usedDefaultStorage {
		h.logger.Infof("storageName is empty, using default storageName %q", req.StorageName)
	}

	if h.s3Registry != nil && !h.s3Registry.Has(req.StorageName) {
		msg := fmt.Sprintf("unknown storageName %q", req.StorageName)
		h.logger.Error(msg)
		ctx.JSON(http.StatusBadRequest, gin.H{"message": msg})
		return
	}

	if req.Databases == nil {
		req.Databases = []entity.RestoreDBMap{}
	}
	for _, m := range req.Databases {
		if strings.TrimSpace(m.PreviousDatabaseName) == "" || strings.TrimSpace(m.DatabaseName) == "" {
			msg := "each databases item must have previousDatabaseName and databaseName"
			h.logger.Error(msg)
			ctx.JSON(http.StatusBadRequest, gin.H{"message": msg})
			return
		}
	}

	internal := mapRestoreV2ToInternal(backupID, req, getProcType(ctx.Request.URL.Path))

	resp, err := h.fullBackup.RestoreBackup(ctx, internal)
	if err != nil {
		if errors.Is(err, controller.ErrVaultNotFound) {
			msg := fmt.Sprintf("backup %s not found", backupID)
			h.logger.Info(msg)
			ctx.JSON(http.StatusNotFound, gin.H{"message": msg})
			return
		}
		msg := fmt.Sprintf("failed to restore backup err: %v", err)
		h.logger.Error(msg)
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": msg})
		return
	}

	status := NotStarted
	if req.DryRun {
		status = Completed
	}

	ctx.JSON(http.StatusOK, buildRestoreV2Response(req, resp.TaskID, status, resp.CreationTime))
}

func (h *EndpointHandlerV2) RestoreV2Status(ctx *gin.Context) {
	taskID := strings.TrimSpace(ctx.Param("restore_id"))
	if taskID == "" {
		msg := "restore_id is required"
		h.logger.Error(msg)
		ctx.JSON(http.StatusBadRequest, gin.H{"message": msg})
		return
	}

	js, err := h.fullBackup.GetJobStatus(ctx, entity.JobStatusRequest{TaskID: taskID})
	if err != nil {
		msg := fmt.Sprintf("failed to get job status err: %v", err)
		h.logger.Error(msg)
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": msg})
		return
	}

	if js.Type != "incremental restore" && js.Type != "restore" {
		msg := fmt.Sprintf("job '%s' is not a restore task", taskID)
		h.logger.Error(msg)
		ctx.JSON(http.StatusNotFound, gin.H{
			"message": msg,
		})
		return
	}
	if js.StatusCode == http.StatusNotFound {
		msg := fmt.Sprintf("Sorry, no job '%s' recorded in database", taskID)
		h.logger.Info(msg)
		ctx.JSON(http.StatusNotFound, gin.H{"message": msg})
		return
	}

	status := mapJobStatus(js.Status)

	var errMsg string
	if status == Failed {
		errMsg = js.Error
	}

	var databases []entity.RestoreDatabaseV2Status
	if len(js.RestoreDatabases) > 0 {
		databases = RestoreDbStatuses(js.RestoreDatabases, status, js.CreationTime)
	} else {
		databases = RestoreDbStatusesFromNames(js.Databases, status, js.CreationTime)
	}

	resp := entity.RestoreV2Response{
		Status:         status,
		ErrorMessage:   errMsg,
		RestoreID:      taskID,
		CreationTime:   js.CreationTime,
		CompletionTime: js.CompletionTime,
		StorageName:    js.StorageName,
		BlobPath:       js.BlobPath,
		Databases:      databases,
	}

	ctx.JSON(http.StatusOK, resp)
}

func (h *EndpointHandlerV2) RestoreV2Delete(ctx *gin.Context) {
	restoreID := strings.TrimSpace(ctx.Param("restore_id"))
	if restoreID == "" {
		msg := "restore_id is required"
		h.logger.Error(msg)
		ctx.JSON(http.StatusBadRequest, gin.H{"message": msg})
		return
	}

	js, err := h.fullBackup.GetJobStatus(ctx, entity.JobStatusRequest{TaskID: restoreID})
	if err != nil {
		msg := fmt.Sprintf("failed to get job status err: %v", err)
		h.logger.Error(msg)
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": msg})
		return
	}
	if js.StatusCode == http.StatusNotFound {
		msg := fmt.Sprintf("Sorry, no job '%s' found in database", restoreID)
		h.logger.Info(msg)
		ctx.JSON(http.StatusNotFound, gin.H{"message": msg})
		return
	}

	blob := normalizeBlobPath(ctx.Query("blobPath"))
	if blob == "" {
		if js.BlobPath != "" {
			blob = js.BlobPath
		} else {
			msg := "blobPath is required"
			h.logger.Error(msg)
			ctx.JSON(http.StatusBadRequest, gin.H{"message": msg})
			return
		}
	}

	err = h.fullBackup.RemoveRestoreV2(ctx, entity.EvictByVaultV2Request{
		Vault:    js.Vault,
		BlobPath: blob,
		TaskID:   restoreID,
	})
	if err != nil {
		msg := fmt.Sprintf("failed to delete restore: %v", err)
		h.logger.Error(msg)
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": msg})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"message":   "OK",
		"restoreId": restoreID,
		"blobPath":  blob,
	})
}
