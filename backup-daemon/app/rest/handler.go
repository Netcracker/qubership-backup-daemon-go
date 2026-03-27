package rest

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/controller"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type EndpointHandler struct {
	backupDaemonUseCase controller.BackupDaemonUseCase
	logger              *zap.SugaredLogger
	customVarNames      []string
}

func NewEndpointHandler(backupDaemonUseCase controller.BackupDaemonUseCase, logger *zap.SugaredLogger, customVarNames ...string) *EndpointHandler {
	return &EndpointHandler{
		backupDaemonUseCase: backupDaemonUseCase,
		logger:              logger,
		customVarNames:      customVarNames,
	}
}

func (h *EndpointHandler) getCustomVarNames() map[string]bool {
	m := make(map[string]bool)
	for _, name := range h.customVarNames {
		parts := strings.SplitN(name, "=", 2)
		m[parts[0]] = true
	}
	return m
}

var backupAllowedKeys = map[string]bool{
	"dbs": true, "allow_eviction": true, "externalBackupPath": true,
	"sharded": true, "prefix": true, "mode": true, "custom_vars": true,
	"args": true,
}

func (h *EndpointHandler) Backup(ctx *gin.Context) {
	var request entity.BackupRequest

	if ctx.Request.ContentLength > 0 {
		bodyBytes, _ := io.ReadAll(ctx.Request.Body)
		ctx.Request.Body = io.NopCloser(strings.NewReader(string(bodyBytes)))

		var raw map[string]interface{}
		if err := json.Unmarshal(bodyBytes, &raw); err != nil {
			h.logger.Errorf("failed to unmarshall body err: %v", err)
			ctx.JSON(http.StatusBadRequest, gin.H{"message": fmt.Sprintf("failed to unmarshall body err: %v", err)})
			return
		}
		customVars := h.getCustomVarNames()
		for key := range raw {
			if !backupAllowedKeys[key] && !customVars[key] {
				ctx.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprintf("Unknown body key: %s", key)})
				return
			}
		}

		if err := json.Unmarshal(bodyBytes, &request); err != nil {
			h.logger.Errorf("failed to unmarshall body err: %v", err)
			ctx.JSON(http.StatusBadRequest, gin.H{"message": fmt.Sprintf("failed to unmarshall body err: %v", err)})
			return
		}
	}

	if len(request.DBs) == 0 && len(request.Args) > 0 {
		for _, db := range request.Args {
			request.DBs = append(request.DBs, entity.DBEntry{
				Name:       db,
				SimpleName: db,
			})
		}
	}

	request.ProcType = getProcType(ctx.Request.URL.Path)
	response, err := h.backupDaemonUseCase.EnqueueBackup(ctx, request)
	if err != nil {
		h.logger.Errorf("failed to enqueue backup err: %v", err)
		if errors.Is(err, controller.ErrIllegalState) {
			ctx.Data(http.StatusConflict, "application/json", []byte(fmt.Sprintf(`{"status":"Failed","message":"%s"}`, escapeJSON(err.Error()))))
			return
		}
		ctx.Data(http.StatusInternalServerError, "application/json", []byte(fmt.Sprintf(`{"status":"Failed","message":"%s"}`, escapeJSON(err.Error()))))
		return
	}
	ctx.Data(http.StatusOK, "application/json", []byte(response.BackupID))
}

func (h *EndpointHandler) Restore(ctx *gin.Context) {
	var request entity.RestoreRequest
	if ctx.Request.ContentLength > 0 {
		if err := ctx.ShouldBindJSON(&request); err != nil {
			h.logger.Errorf("failed to unmarshall body err: %v", err)
			ctx.JSON(http.StatusBadRequest, gin.H{
				"message": fmt.Sprintf("failed to unmarshall body err: %v", err),
			})
			return
		}
	}
	if len(request.Vault) == 0 && len(request.TimeStamp) == 0 {
		h.logger.Error("Sorry, wrong JSON string. No 'vault' or 'ts' parameter")
		ctx.JSON(http.StatusNotFound, gin.H{
			"status":  "Failed",
			"message": "Sorry, wrong JSON string. No 'vault' or 'ts' parameter",
		})
		return
	}
	request.ProcType = getProcType(ctx.Request.URL.Path)

	// Validate vault and resolve canonical name from stats, matching Python behavior.
	if len(request.ExternalBackupPath) == 0 && (len(request.Vault) > 0 || len(request.TimeStamp) > 0) {
		vaultName := request.Vault
		tsArg := request.TimeStamp
		stats, err := h.backupDaemonUseCase.GetBackupStats(ctx, vaultName, tsArg, "", request.ProcType)
		if err != nil {
			h.logger.Errorf("restore failed, wrong vault name: %v", err)
			ctx.JSON(http.StatusNotFound, gin.H{
				"status":  "Failed",
				"message": fmt.Sprintf("Restore failed. Wrong vault name or ts: %v", err),
			})
			return
		}
		// Use the resolved canonical id (matches Python: name = message['id']).
		if id, ok := stats["id"].(string); ok && id != "" {
			request.Vault = id
			request.TimeStamp = ""
		}
	}

	response, err := h.backupDaemonUseCase.RestoreBackup(ctx, request)
	if err != nil {
		if errors.Is(err, controller.ErrVaultNotFound) {
			h.logger.Errorf("backup vault not found: %v", err)
			ctx.JSON(http.StatusNotFound, gin.H{
				"message": "backup vault not found",
			})
			return
		}
		h.logger.Errorf("failed to restore backup err: %v", err)
		ctx.Data(http.StatusInternalServerError, "application/json", []byte(fmt.Sprintf(`{"status":"Failed","message":"%s"}`, escapeJSON(err.Error()))))
		return
	}
	ctx.Data(http.StatusOK, "application/json", []byte(response.TaskID))
}

func (h *EndpointHandler) Evict(ctx *gin.Context) {
	procType := getProcType(ctx.Request.URL.Path)
	request := entity.EvictRequest{
		ProcType: procType,
	}

	err := h.backupDaemonUseCase.EnqueueEviction(ctx, request)
	if err != nil {
		h.logger.Errorf("failed to enqueue eviction err: %v", err)
		ctx.Data(http.StatusInternalServerError, "application/json", []byte(fmt.Sprintf(`{"message":"%s"}`, escapeJSON(err.Error()))))
		return
	}
	ctx.Data(http.StatusOK, "application/json", []byte("Ok\n"))
}

func (h *EndpointHandler) EvictByVault(ctx *gin.Context) {
	vault := ctx.Param("vault")
	procType := getProcType(ctx.Request.URL.Path)

	req := entity.EvictByVaultRequest{Vault: vault, ProcType: procType}

	if err := h.backupDaemonUseCase.RemoveBackup(ctx, req); err != nil {
		h.logger.Errorf("failed to remove backup: %v", err)

		switch {
		case errors.Is(err, controller.ErrVaultNotFound):
			ctx.JSON(http.StatusNotFound, gin.H{"status": "Failed", "message": "backup vault not found"})
		case errors.Is(err, controller.ErrVaultLocked):
			ctx.JSON(http.StatusLocked, gin.H{"status": "Failed", "message": "backup vault is locked"})
		default:
			ctx.JSON(http.StatusInternalServerError, gin.H{"status": "Failed", "message": err.Error()})
		}
		return
	}

	ctx.Data(http.StatusOK, "application/json", []byte("Ok\n"))
}

func (h *EndpointHandler) ExternalRestore(ctx *gin.Context) {
	var request entity.RestoreRequest
	if err := ctx.ShouldBindJSON(&request.CustomVars); err != nil {
		h.logger.Errorf("failed to unmarshall body err: %v", err)
		ctx.JSON(http.StatusBadRequest, gin.H{
			"message": fmt.Sprintf("failed to unmarshall body err: %v", err),
		})
		return
	}
	request.ProcType = controller.FULL
	response, err := h.backupDaemonUseCase.RestoreBackup(ctx, request)
	if err != nil {
		h.logger.Errorf("failed to restore external backup err: %v", err)
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": fmt.Sprintf("failed to restore external backup err: %v", err),
		})
		return
	}
	ctx.JSON(http.StatusOK, response)
}

func (h *EndpointHandler) JobStatus(ctx *gin.Context) {
	request := entity.JobStatusRequest{
		TaskID: ctx.Param("task_id"),
	}
	response, err := h.backupDaemonUseCase.GetJobStatus(ctx, request)
	if err != nil {
		h.logger.Errorf("failed to get job status err: %v", err)
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": fmt.Sprintf("failed to get job status err: %v", err),
		})
		return
	}
	if response.StatusCode == http.StatusNotFound {
		h.logger.Errorf("Sorry, no job '%s' recorded in database", request.TaskID)
		ctx.JSON(http.StatusNotFound, gin.H{
			"message": fmt.Sprintf("Sorry, no job '%s' recorded in database", request.TaskID),
		})
		return
	}
	ctx.JSON(response.StatusCode, response)
}

func (h *EndpointHandler) ListBackups(ctx *gin.Context) {
	procType := getProcType(ctx.Request.URL.Path)
	backups, err := h.backupDaemonUseCase.ListBackups(ctx, procType)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if backups == nil {
		backups = []string{}
	}
	ctx.JSON(http.StatusOK, backups)
}

func (h *EndpointHandler) ListBackupByVault(ctx *gin.Context) {
	vault := ctx.Param("vault")
	if vault == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "vault path is required"})
		return
	}
	procType := getProcType(ctx.Request.URL.Path)

	result, err := h.backupDaemonUseCase.ListBackup(ctx, procType, vault)
	if err != nil {
		h.logger.Errorf("failed to list backup by vault: %v", err)
		if strings.Contains(err.Error(), "not found") {
			ctx.JSON(http.StatusNotFound, gin.H{"message": err.Error()})
		} else {
			ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}
	ctx.JSON(http.StatusOK, result)
}

func (h *EndpointHandler) Find(ctx *gin.Context) {
	// Support ts from query param (new) and JSON body (BWC legacy).
	ts := ctx.Query("ts")
	if ts == "" && ctx.Request.ContentLength > 0 {
		var body struct {
			TimeStamp string `json:"ts"`
		}
		if err := ctx.ShouldBindJSON(&body); err == nil {
			ts = body.TimeStamp
		}
	}
	if ts == "" {
		ctx.JSON(http.StatusNotFound, gin.H{
			"message": `Sorry, wrong JSON string. No "ts" parameter.`,
		})
		return
	}
	request := entity.FindRequest{
		TimeStamp: ts,
		ProcType:  getProcType(ctx.Request.URL.Path),
	}
	result, err := h.backupDaemonUseCase.Find(ctx, request)
	if err != nil {
		h.logger.Errorf("failed to find backup: %v", err)
		ctx.JSON(http.StatusNotFound, gin.H{
			"message": err.Error(),
		})
		return
	}
	ctx.JSON(http.StatusOK, result)
}

func (h *EndpointHandler) S3PresignedURL(ctx *gin.Context) {
	expirationStr := ctx.Query("expiration")
	if expirationStr == "" {
		ctx.JSON(http.StatusNoContent, nil)
		return
	}
	expiration, err := strconv.Atoi(expirationStr)
	if err != nil {
		h.logger.Errorf("failed to parse value from url err: %v", err)
		ctx.JSON(http.StatusBadRequest, gin.H{
			"message": fmt.Sprintf("failed to parse value from url err: %v", err),
		})
		return
	}
	request := entity.S3PresignedURLRequest{
		BackupID:   ctx.Param("backup_id"),
		ProcType:   getProcType(ctx.Request.URL.Path),
		Expiration: expiration,
	}
	response, err := h.backupDaemonUseCase.CreateS3PresignedURL(ctx, request)
	if err != nil {
		h.logger.Errorf("failed to create s3 presigned url err: %v", err)
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": fmt.Sprintf("failed to create s3 presigned urls err: %v", err),
		})
		return
	}
	ctx.JSON(http.StatusOK, response)
}

func (h *EndpointHandler) Health(ctx *gin.Context) {
	procType := getProcType(ctx.Request.URL.Path)
	resp, err := h.backupDaemonUseCase.GetHealth(ctx, procType)
	if err != nil {
		h.logger.Errorf("failed to get health: %v", err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, resp)
}

func (h *EndpointHandler) HealthPrometheus(ctx *gin.Context) {
	procType := getProcType(ctx.Request.URL.Path)
	resp, err := h.backupDaemonUseCase.GetHealth(ctx, procType)
	if err != nil {
		ctx.String(http.StatusInternalServerError, "# error getting health\n")
		return
	}

	var sb strings.Builder
	if _, err = fmt.Fprintf(&sb, "backup_daemon_status %d\n", boolToInt(resp.Status == "UP")); err != nil {
		return
	}
	if _, err = fmt.Fprintf(&sb, "backup_queue_size %d\n", resp.BackupQueueSize); err != nil {
		return
	}
	if _, err = fmt.Fprintf(&sb, "backup_storage_dump_count %d\n", resp.Storage.DumpCount); err != nil {
		return
	}
	if _, err = fmt.Fprintf(&sb, "backup_storage_free_space %d\n", resp.Storage.FreeSpace); err != nil {
		return
	}
	if _, err = fmt.Fprintf(&sb, "backup_storage_size %d\n", resp.Storage.Size); err != nil {
		return
	}
	if _, err = fmt.Fprintf(&sb, "backup_storage_space_total %d\n", resp.Storage.TotalSpace); err != nil {
		return
	}
	if _, err = fmt.Fprintf(&sb, "backup_storage_free_inodes %d\n", resp.Storage.FreeInodes); err != nil {
		return
	}
	if _, err = fmt.Fprintf(&sb, "backup_storage_inodes_total %d\n", resp.Storage.TotalInodes); err != nil {
		return
	}
	if _, err = fmt.Fprintf(&sb, "backup_storage_used_inodes %d\n", resp.Storage.UsedInodes); err != nil {
		return
	}

	if resp.Storage.Last.ID != "" {
		id := resp.Storage.Last.ID
		if _, err = fmt.Fprintf(&sb, "backup_storage_last_failed{id=\"%s\"} %d\n", id, boolToInt(resp.Storage.Last.Failed)); err != nil {
			return
		}
		if _, err = fmt.Fprintf(&sb, "backup_storage_last_locked{id=\"%s\"} %d\n", id, boolToInt(resp.Storage.Last.Locked)); err != nil {
			return
		}
		if _, err = fmt.Fprintf(&sb, "backup_storage_last_exit_code{id=\"%s\"} %d\n", id, resp.Storage.Last.Metrics.ExitCode); err != nil {
			return
		}
		if _, err = fmt.Fprintf(&sb, "backup_storage_last_size{id=\"%s\"} %d\n", id, resp.Storage.Last.Metrics.Size); err != nil {
			return
		}
		if _, err = fmt.Fprintf(&sb, "backup_storage_last_spent_time{id=\"%s\"} %d\n", id, resp.Storage.Last.Metrics.SpentTime); err != nil {
			return
		}
		if _, err = fmt.Fprintf(&sb, "backup_storage_last_sharded{id=\"%s\"} %d\n", id, boolToInt(resp.Storage.Last.Sharded)); err != nil {
			return
		}
		if _, err = fmt.Fprintf(&sb, "backup_storage_last_timestamp{id=\"%s\"} %d\n", id, resp.Storage.Last.TimeStamp); err != nil {
			return
		}
	}

	if resp.Storage.LastSuccessful.ID != "" {
		id := resp.Storage.LastSuccessful.ID
		if _, err = fmt.Fprintf(&sb, "backup_storage_last_successful_failed{id=\"%s\"} %d\n", id, boolToInt(resp.Storage.LastSuccessful.Failed)); err != nil {
			return
		}
		if _, err = fmt.Fprintf(&sb, "backup_storage_last_successful_locked{id=\"%s\"} %d\n", id, boolToInt(resp.Storage.LastSuccessful.Locked)); err != nil {
			return
		}
		if _, err = fmt.Fprintf(&sb, "backup_storage_last_successful_exit_code{id=\"%s\"} %d\n", id, resp.Storage.LastSuccessful.Metrics.ExitCode); err != nil {
			return
		}
		if _, err = fmt.Fprintf(&sb, "backup_storage_last_successful_size{id=\"%s\"} %d\n", id, resp.Storage.LastSuccessful.Metrics.Size); err != nil {
			return
		}
		if _, err = fmt.Fprintf(&sb, "backup_storage_last_successful_spent_time{id=\"%s\"} %d\n", id, resp.Storage.LastSuccessful.Metrics.SpentTime); err != nil {
			return
		}
		if _, err = fmt.Fprintf(&sb, "backup_storage_last_successful_sharded{id=\"%s\"} %d\n", id, boolToInt(resp.Storage.LastSuccessful.Sharded)); err != nil {
			return
		}
		if _, err = fmt.Fprintf(&sb, "backup_storage_last_successful_timestamp{id=\"%s\"} %d\n", id, resp.Storage.LastSuccessful.TimeStamp); err != nil {
			return
		}
	}

	ctx.String(http.StatusOK, sb.String())
}

func (h *EndpointHandler) EvictionPolicy(ctx *gin.Context) {
	var request entity.EvictionPolicyRequest
	if err := ctx.ShouldBindJSON(&request); err != nil {
		h.logger.Errorf("failed to unmarshall body err: %v", err)
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": fmt.Sprintf("failed to unmarshall body err: %v", err),
		})
		return
	}
	if err := h.backupDaemonUseCase.UpdateEvictionPolicy(ctx, request); err != nil {
		h.logger.Errorf("failed to update eviction policy: %v", err)
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}
	ctx.Data(http.StatusOK, "application/json", []byte("Ok\n"))
}

func (h *EndpointHandler) Terminate(ctx *gin.Context) {
	backupID := ctx.Param("backup_id")
	var request entity.TerminateRequest
	if ctx.Request.ContentLength > 0 {
		_ = ctx.ShouldBindJSON(&request)
	}
	request.BackupID = backupID

	if err := h.backupDaemonUseCase.TerminateBackup(ctx, request); err != nil {
		h.logger.Errorf("failed to terminate backup: %v", err)

		switch {
		case errors.Is(err, controller.ErrVaultNotFound):
			ctx.JSON(http.StatusNotFound, gin.H{"status": "Failed", "message": fmt.Sprintf("Backup %s not found", backupID)})
		case errors.Is(err, controller.ErrBackupNotRunning):
			ctx.JSON(http.StatusNotAcceptable, gin.H{"status": "Failed", "message": fmt.Sprintf("Backup %s already finished or canceled", backupID)})
		default:
			ctx.JSON(http.StatusInternalServerError, gin.H{"status": "Failed", "message": err.Error()})
		}
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"message": "OK"})
}

func (h *EndpointHandler) DownloadBackup(ctx *gin.Context) {
	backupID := ctx.Param("backup_id")
	folder, err := h.backupDaemonUseCase.DownloadBackup(ctx, backupID)
	if err != nil {
		if errors.Is(err, controller.ErrVaultNotFound) {
			ctx.Status(http.StatusNoContent)
			return
		}
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
		return
	}

	ctx.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%s.zip", backupID))
	ctx.Header("Content-Type", "application/zip")
	ctx.Status(http.StatusOK)

	zw := zip.NewWriter(ctx.Writer)
	defer func() {
		if err := zw.Close(); err != nil {
			h.logger.Errorf("failed to close zip writer: %v", err)
		}
	}()

	_ = filepath.Walk(folder, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		relPath, _ := filepath.Rel(folder, path)
		w, err := zw.Create(relPath)
		if err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer func() {
			if err := f.Close(); err != nil {
				h.logger.Errorf("failed to close file %s: %v", path, err)
			}
		}()
		_, _ = io.Copy(w, f)
		return nil
	})
}

func (h *EndpointHandler) UploadBackup(ctx *gin.Context) {
	ctx.JSON(http.StatusNotImplemented, gin.H{"message": "upload not implemented"})
}

func getProcType(url string) string {
	if strings.Contains(url, "incremental") {
		return controller.INCREMENTAL
	}
	return controller.FULL
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func escapeJSON(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return s
}
