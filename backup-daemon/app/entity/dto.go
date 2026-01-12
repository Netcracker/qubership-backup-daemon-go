package entity

import (
	"encoding/json"
	"time"
)

type EvictRequest struct {
	ProcType string
}

type EvictByVaultRequest struct {
	Vault    string
	ProcType string
}

type EvictByVaultV2Request struct {
	Vault    string
	BlobPath string
}

type HealthResponse struct {
	Status          string      `json:"status"`
	BackupQueueSize int         `json:"backup_queue_size"`
	Storage         StorageInfo `json:"storage"`
}

type StorageInfo struct {
	TotalSpace     int        `json:"total_space"`
	DumpCount      int        `json:"dump_count"`
	FreeSpace      int        `json:"free_space"`
	Size           int        `json:"size"`
	TotalInodes    int        `json:"total_inodes"`
	FreeInodes     int        `json:"free_inodes"`
	UsedInodes     int        `json:"used_inodes"`
	Last           BackupInfo `json:"last"`
	LastSuccessful BackupInfo `json:"lastSuccessful"`
}

type BackupInfo struct {
	ID        string        `json:"id"`
	Metrics   BackupMetrics `json:"metrics"`
	Failed    bool          `json:"failed"`
	Locked    bool          `json:"locked"`
	Sharded   bool          `json:"sharded"`
	TimeStamp time.Time     `json:"ts"`
}

type BackupMetrics struct {
	ExitCode  int `json:"exit_code"`
	Exception int `json:"exception,omitempty"` // only in last
	SpentTime int `json:"spent_time"`
	Size      int `json:"size"`
}

type BackupRequest struct {
	DBs                []DBEntry         `json:"dbs,omitempty"`
	AllowEviction      bool              `json:"allow_eviction,omitempty"`
	ExternalBackupPath string            `json:"externalBackupPath,omitempty"`
	Sharded            bool              `json:"sharded,omitempty"`
	Prefix             string            `json:"prefix,omitempty"`
	Mode               string            `json:"mode,omitempty"`
	CustomVars         map[string]string `json:"custom_vars,omitempty"`
	ProcType           string
}

type DBEntry struct {
	SimpleName string
	Object     map[string]DBObject
}

func (d *DBEntry) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		d.SimpleName = s
		return nil
	}
	var obj map[string]DBObject
	if err := json.Unmarshal(data, &obj); err != nil {
		return err
	}
	for k, v := range obj {
		d.SimpleName = k
		d.Object = map[string]DBObject{k: v}
	}
	return nil
}

type DBObject struct {
	Collections []CollectionItem `json:"collections,omitempty"`
	Tables      []string         `json:"tables,omitempty"`
}

type CollectionItem struct {
	Name    string
	Details map[string]interface{}
}

func (c *CollectionItem) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		c.Name = s
		return nil
	}
	var obj map[string]map[string]interface{}
	if err := json.Unmarshal(data, &obj); err != nil {
		return err
	}
	for k, v := range obj {
		c.Name = k
		c.Details = v
	}
	return nil
}

type BackupResponse struct {
	BackupID string `json:"backup_id"` // uuid
}

type RestoreRequest struct {
	ExternalBackupPath string            `json:"externalBackupPath,omitempty"`
	Vault              string            `json:"vault,omitempty"`
	TimeStamp          string            `json:"ts,omitempty"`
	DBs                []DBEntry         `json:"dbs,omitempty"`
	ChangeDbNames      map[string]string `json:"changeDbNames,omitempty"`
	CustomVars         map[string]string `json:"custom_vars,omitempty"`
	ProcType           string
}

type RestoreResponse struct {
	TaskID string `json:"task_id"`
}

type JobStatusRequest struct {
	TaskID string
}

type JobStatusResponse struct {
	Status string `json:"status"`
	Vault  string `json:"vault"`
	Type   string `json:"type"`
	Error  string `json:"err"`
	TaskID string `json:"task_id"`

	StorageName string   `json:"storageName"`
	BlobPath    string   `json:"blobPath"`
	Databases   []string `json:"databases,omitempty"`
	StatusCode  int
}

type ListBackupsRequest struct {
	ProcType string
}

type FindRequest struct {
	TimeStamp string `json:"ts"`
	ProcType  string
}

type FindResponse struct {
	TimeStamp  string            `json:"ts"`
	SpentTime  string            `json:"spent_time"`
	DBList     string            `json:"db_list"`
	VaultID    string            `json:"id"`
	Size       string            `json:"size"`
	Evictable  bool              `json:"evictable"`
	Locked     bool              `json:"locked"`
	ExitCode   int               `json:"exit_code"`
	Failed     bool              `json:"failed"`
	Valid      bool              `json:"valid"`
	IsGranular bool              `json:"is_granular"`
	Sharded    bool              `json:"sharded"`
	Canceled   bool              `json:"canceled"`
	CustomVars map[string]string `json:"custom_vars,omitempty"`
}

type EvictionPolicyRequest struct {
	FullEvictionPolicy string `json:"fullEvictionPolicy"`
}

type TerminateRequest struct {
	ExternalBackupPath string `json:"externalBackupPath,omitempty"`
	BackupID           string
}

type ExternalRestoreRequest struct {
	CustomVars map[string]string
}

type ExternalRestoreResponse struct {
	TaskID string `json:"task_id"`
}

type S3PresignedURLRequest struct {
	BackupID   string
	ProcType   string
	Expiration int
}

type S3PresignedURLResponse struct {
	Urls []string `json:"urls"`
}
