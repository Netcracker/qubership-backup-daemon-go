package rest

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/controller"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type EndpointHandler struct {
	backupDaemonUseCase controller.BackupDaemonUseCase
	logger              *zap.SugaredLogger
}

func NewEndpointHandler(backupDaemonUseCase controller.BackupDaemonUseCase, logger *zap.SugaredLogger) *EndpointHandler {
	return &EndpointHandler{
		backupDaemonUseCase: backupDaemonUseCase,
		logger:              logger,
	}
}

func (h *EndpointHandler) Backup(ctx *gin.Context) {
	var request entity.BackupRequest

	if err := ctx.ShouldBindJSON(&request); err != nil && ctx.Request.ContentLength > 0 {
		h.logger.Errorf("failed to unmarshall body err: %v", err)
		ctx.JSON(http.StatusBadRequest, gin.H{
			"message": fmt.Sprintf("failed to unmarshall body err: %v", err),
		})
		return
	}
	request.ProcType = getProcType(ctx.Request.URL.Path)
	response, err := h.backupDaemonUseCase.EnqueueBackup(ctx, request)
	if err != nil {
		h.logger.Errorf("failed to enqueue backup err: %v", err)
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": fmt.Sprintf("failed to enqueue backup err: %v", err),
		})
		return
	}
	ctx.JSON(http.StatusOK, response)
}

func (h *EndpointHandler) Restore(ctx *gin.Context) {
	var request entity.RestoreRequest
	// TODO the unknown values it need to give to custom vars format {"vault":"20190321T080000", "dbs":["db1","db2","db3"], "changeDbNames":{"db1":"new_db1_name","db2":"new_db2_name"},  //unknown "clean":"true"}
	if err := ctx.ShouldBindJSON(&request); err != nil {
		h.logger.Errorf("failed to unmarshall body err: %v", err)
		ctx.JSON(http.StatusBadRequest, gin.H{
			"message": fmt.Sprintf("failed to unmarshall body err: %v", err),
		})
		return
	}
	if len(request.Vault) == 0 && len(request.TimeStamp) == 0 {
		h.logger.Error("Sorry, wrong JSON string. No 'vault' or 'ts' parameter")
		ctx.JSON(http.StatusNotFound, gin.H{
			"message": "Sorry, wrong JSON string. No 'vault' or 'ts' parameter",
		})
		return
	}
	request.ProcType = getProcType(ctx.Request.URL.Path)
	response, err := h.backupDaemonUseCase.RestoreBackup(ctx, request)
	if err != nil {
		h.logger.Errorf("failed to restore backup err: %v", err)
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": fmt.Sprintf("failed to restore backup err: %v", err),
		})
		return
	}
	ctx.JSON(http.StatusOK, response)
}

func (h *EndpointHandler) Evict(ctx *gin.Context) {
	procType := getProcType(ctx.Request.URL.Path)
	request := entity.EvictRequest{
		ProcType: procType,
	}

	err := h.backupDaemonUseCase.EnqueueEviction(ctx, request)
	if err != nil {
		h.logger.Errorf("failed to enqueue eviction err: %v", err)
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": fmt.Sprintf("failed to enqueue eviction err: %v", err),
		})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{
		"message": "OK",
	})
}

func (h *EndpointHandler) EvictByVault(ctx *gin.Context) {
	vault := ctx.Param("vault")
	procType := getProcType(ctx.Request.URL.Path)
	request := entity.EvictByVaultRequest{
		Vault:    vault,
		ProcType: procType,
	}
	err := h.backupDaemonUseCase.RemoveBackup(ctx, request)
	if err != nil {
		h.logger.Errorf("failed to remove backup err: %v", err)
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": fmt.Sprintf("failed to remove backup err: %v", err),
		})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{
		"message": "OK",
	})
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

//func (h *EndpointHandler) ListBackups(ctx *gin.Context) {
//
//}
//
//func (h *EndpointHandler) ListBackupByVault(ctx *gin.Context) {
//
//}
//
//func (h *EndpointHandler) Find(ctx *gin.Context) {
//	var request entity.FindRequest
//	if err := ctx.ShouldBindJSON(&request); err != nil {
//		ctx.JSON(http.StatusBadRequest, gin.H{
//			"message": fmt.Sprintf("failed to unmarshall body err: %v", err),
//		})
//		return
//	}
//
//}

func (h *EndpointHandler) S3PresignedURL(ctx *gin.Context) {
	expiration, err := strconv.Atoi(ctx.Query("expiration"))
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
	ctx.JSON(http.StatusOK, gin.H{
		"message": "OK",
	})
}

func getProcType(url string) string {
	if strings.Contains(url, "incremental") {
		return controller.INCREMENTAL
	}
	return controller.FULL
}
