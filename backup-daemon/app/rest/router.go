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

	r.NoRoute(func(ctx *gin.Context) { // check for 404
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
		incremental.GET("/jobstatus/:task_id", eh.JobStatus)
	}

	full := r.Group("/")
	{
		full.POST("/backup", eh.Backup)
		full.POST("/restore", eh.Restore)
		full.POST("/evict", eh.Evict)
		full.POST("/evict/:vault", eh.EvictByVault)
		full.POST("/external/restore", eh.ExternalRestore)
		full.GET("/jobstatus/:task_id", eh.JobStatus)
		full.GET("/backup/s3/:backup_id", eh.S3PresignedURL)
		full.GET("/health", eh.Health)
	}

	v1 := r.Group("/api/v1")
	{
		v1.POST("/backup", eh.BackupV2)
		v1.GET("/backup/:backup_id", eh.BackupV2Status)
		v1.DELETE("/backup/:backup_id", eh.BackupV2Delete)
		v1.POST("/restore/:backup_id", eh.RestoreV2)
		v1.GET("/restore/:restore_id", eh.RestoreV2Status)

	}

	return r
}
