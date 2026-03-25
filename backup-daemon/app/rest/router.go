package rest

import (
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

type router struct {
}

// requirePostDeleteBasicAuth is a minimal auth gate for write operations.
// It enables HTTP BasicAuth only when both expected env vars are set.
func requirePostDeleteBasicAuth() gin.HandlerFunc {
	expectedUsername := os.Getenv("BACKUP_DAEMON_API_CREDENTIALS_USERNAME")
	expectedPassword := os.Getenv("BACKUP_DAEMON_API_CREDENTIALS_PASSWORD")

	// Backward compatible default: if credentials aren't configured, don't block requests.
	// (If you want fail-closed instead, return 401 when either env var is empty.)
	if expectedUsername == "" || expectedPassword == "" {
		return func(c *gin.Context) {
			c.Next()
		}
	}

	return func(c *gin.Context) {
		// Only protect write endpoints, based on common API usage in this repo.
		if c.Request.Method != http.MethodPost && c.Request.Method != http.MethodDelete {
			c.Next()
			return
		}

		username, password, ok := c.Request.BasicAuth()
		if !ok || username != expectedUsername || password != expectedPassword {
			c.Header("WWW-Authenticate", `Basic realm="backup-daemon"`)
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		c.Next()
	}
}

func NewRouter() *router {
	return &router{}
}

func (s *router) GetHandler(eh *EndpointHandler) http.Handler {
	r := gin.Default()
	r.Use(requirePostDeleteBasicAuth())

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
