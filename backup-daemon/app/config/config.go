package config

import "time"

type Config struct {
	Port            int           `long:"port" description:"HTTP server port" default:"8080"`
	ShutdownTimeout time.Duration `long:"shutdown-timeout" description:"Timeout for server shutdown" default:"2s"`

	StorageRoot  string `long:"storage-root" description:"Local storage root path" default:"/backup-storage" env:"STORAGE"`
	ExternalRoot string `long:"external-root" description:"External storage path" default:"/external" env:"STORAGE_EXTERNAL"`
	Namespace    string `long:"namespace" description:"Namespace for storage" default:"default"`
	AllowPrefix  bool   `long:"allow-prefix" description:"Allow prefix matching in storage" env:"ALLOW_PREFIX"`

	S3URL           string `long:"s3-url" description:"S3 endpoint URL" env:"S3_URL"`
	AccessKeyID     string `long:"s3-access-key-id" description:"S3 access key ID" env:"S3_KEY_ID"`
	AccessKeySecret string `long:"s3-access-key-secret" description:"S3 access key secret" env:"S3_KEY_SECRET"`
	BucketName      string `long:"s3-bucket" description:"S3 bucket name" env:"S3_BUCKET"`
	Region          string `long:"s3-region" description:"S3 region" default:"us-east-1"`
	S3Enabled       bool   `long:"s3-enabled" description:"Enable S3 storage" env:"S3_ENABLED"`
	S3SslVerify     bool   `long:"s3-ssl-verify" description:"Verify S3 certificates" env:"S3_SSL_VERIFY"`

	EvictCmd   string `long:"evict-cmd"   description:"Command to evict data"     default:"ls -la {{.data_folder}}" env:"EVICT_CMD"`
	BackupCmd  string `long:"backup-cmd"  description:"Command to backup data"    default:"ls -la {{.data_folder}}" env:"BACKUP_COMMAND"`
	RestoreCmd string `long:"restore-cmd" description:"Command to restore data"   default:"ls -la {{.data_folder}}" env:"RESTORE_COMMAND"`
	DbListCmd  string `long:"dblist-cmd"  description:"Command to list databases" default:"ls -la {{.data_folder}}" env:"LIST_COMMAND"`

	CustomVars   []string `long:"custom-vars" description:"Custom variables for executor" default:"skip_users_recovery" default:"clean" default:"storageName" default:"blob_path"` //nolint:all
	DatabasesKey string   `long:"databases-key" description:"Key for databases list" default:"--dbs" env:"DATABASES_KEY"`
	DbmapKey     string   `long:"dbmap-key" description:"Key for database map" default:"--dbmap" env:"DBMAP_KEY"`
	DBPath       string   `long:"db-path" description:"SQLite DB file path" default:"/backup-storage/database.db" env:"DB_PATH"`

	EvictionPolicy         string `long:"eviction" description:"Eviction policy (e.g. 0/1h,4h/1d)" env:"EVICTION_POLICY"`
	GranularEvictionPolicy string `long:"granular_eviction" description:"Granular eviction policy (e.g. 0/1h,4h/1d)" env:"GRANULAR_EVICTION_POLICY"`
}
