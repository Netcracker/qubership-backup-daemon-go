package rest

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type router struct {
}

func NewRouter() *router {
	return &router{}
}

func (s *router) GetHandler(eh *EndpointHandler) http.Handler {
	r := gin.Default()

	r.NoRoute(func(ctx *gin.Context) {
		ctx.JSON(http.StatusNotFound, gin.H{
			"message": "Page not found",
		})
	})

	incremental := r.Group("/incremental")
	{
		incremental.POST("/backup", eh.Backup)
		incremental.POST("/restore", eh.Restore)
		incremental.POST("/evict", eh.Evict)
		incremental.POST("/evict/:vault", eh.EvictByVault)
		incremental.POST("/evictionpolicy", eh.EvictionPolicy)
		incremental.GET("/jobstatus/:task_id", eh.JobStatus)
		incremental.GET("/health", eh.Health)
		incremental.GET("/listbackups", eh.ListBackups)
		incremental.GET("/listbackups/:vault", eh.ListBackupByVault)
		incremental.GET("/find", eh.Find)
		incremental.GET("/backup/:backup_id", eh.DownloadBackup)
		incremental.GET("/terminate/:backup_id", eh.Terminate)
		incremental.POST("/terminate/:backup_id", eh.Terminate)
	}

	full := r.Group("/")
	{
		full.POST("/backup", eh.Backup)
		full.POST("/restore", eh.Restore)
		full.POST("/evict", eh.Evict)
		full.POST("/evict/:vault", eh.EvictByVault)
		full.POST("/evictionpolicy", eh.EvictionPolicy)
		full.POST("/external/restore", eh.ExternalRestore)
		full.GET("/jobstatus/:task_id", eh.JobStatus)
		full.GET("/backup/s3/:backup_id", eh.S3PresignedURL)
		full.GET("/backup/:backup_id", eh.DownloadBackup)
		full.GET("/health", eh.Health)
		full.GET("/health/prometheus", eh.HealthPrometheus)
		full.GET("/listbackups", eh.ListBackups)
		full.GET("/listbackups/:vault", eh.ListBackupByVault)
		full.GET("/find", eh.Find)
		full.GET("/terminate/:backup_id", eh.Terminate)
		full.POST("/terminate/:backup_id", eh.Terminate)
		full.POST("/restore/backup", eh.UploadBackup)
	}

	v1 := r.Group("/api/v1")
	{
		v1.POST("/backup", eh.BackupV2)
		v1.GET("/backup/:backup_id", eh.BackupV2Status)
		v1.DELETE("/backup/:backup_id", eh.BackupV2Delete)
		v1.POST("/restore/:backup_id", eh.RestoreV2)
		v1.GET("/restore/:restore_id", eh.RestoreV2Status)
		v1.DELETE("/restore/:restore_id", eh.RestoreV2Delete)
	}

	return r
}
