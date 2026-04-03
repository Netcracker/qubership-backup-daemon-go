package entity

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
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
	TaskID   string
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
	Canceled  bool          `json:"canceled"`
	TimeStamp int64         `json:"ts"`
}

type BackupMetrics struct {
	ExitCode  int `json:"exit_code"`
	Exception int `json:"exception,omitempty"` // only in last
	SpentTime int `json:"spent_time"`
	Size      int `json:"size"`
}

type BackupRequest struct {
	DBs                []DBEntry         `json:"dbs,omitempty"`
	Args               []string          `json:"args,omitempty"`
	AllowEviction      string            `json:"allow_eviction,omitempty"`
	ExternalBackupPath string            `json:"externalBackupPath,omitempty"`
	Sharded            bool              `json:"sharded,omitempty"`
	Prefix             string            `json:"prefix,omitempty"`
	Mode               string            `json:"mode,omitempty"`
	CustomVars         map[string]string `json:"custom_vars,omitempty"`
	ProcType           string
}

/*
func (b *BackupRequest) UnmarshalJSON(data []byte) error {
	type plain BackupRequest
	var backup plain

	if err := json.Unmarshal(data, &backup); err != nil {
		return err
	}

	var unknownFields map[string]interface{}
	if err := json.Unmarshal(data, &unknownFields); err != nil {
		return err
	}

	if backup.CustomVars == nil {
		backup.CustomVars = make(map[string]string)
	}

	for _, field := range getFieldsName(&BackupRequest{}) {
		delete(unknownFields, field)
	}

	for k, v := range unknownFields {
		converted, err := convertAnyToStr(v)
		if err != nil {
			return err
		}
		backup.CustomVars[k] = converted
	}
	*b = BackupRequest(backup)
	return nil
}*/

type DBEntry struct {
	Name       string
	SimpleName string
	Object     map[string]DBObject
}

func (d *DBEntry) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		d.Name = s
		d.SimpleName = s
		return nil
	}
	var obj map[string]DBObject
	if err := json.Unmarshal(data, &obj); err != nil {
		return err
	}
	for k, v := range obj {
		d.Name = k
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
	BackupID     string `json:"backup_id"` // uuid
	CreationTime string `json:"creation_time,omitempty"`
}

type RestoreRequest struct {
	ExternalBackupPath string            `json:"externalBackupPath,omitempty"`
	Vault              string            `json:"vault,omitempty"`
	TimeStamp          string            `json:"ts,omitempty"`
	DBs                []DBEntry         `json:"dbs,omitempty"`
	ChangeDbNames      map[string]string `json:"changeDbNames,omitempty"`
	CustomVars         map[string]string `json:"custom_vars,omitempty"`
	ProcType           string
	RestoreDBMaps      []RestoreDBMap `json:"-"`
}

func (r *RestoreRequest) UnmarshalJSON(data []byte) error {
	type plain RestoreRequest
	var restore plain

	if err := json.Unmarshal(data, &restore); err != nil {
		return err
	}

	var unknownFields map[string]interface{}
	if err := json.Unmarshal(data, &unknownFields); err != nil {
		return err
	}
	if restore.CustomVars == nil {
		restore.CustomVars = make(map[string]string)
	}
	for _, field := range getFieldsName(&RestoreRequest{}) {
		delete(unknownFields, field)
	}

	for k, v := range unknownFields {
		converted, err := convertAnyToStr(v)
		if err != nil {
			return err
		}
		restore.CustomVars[k] = converted
	}
	*r = RestoreRequest(restore)
	return nil
}

type RestoreResponse struct {
	TaskID       string `json:"task_id"`
	CreationTime string `json:"creation_time,omitempty"`
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

	StorageName      string         `json:"storageName,omitempty"`
	BlobPath         string         `json:"blobPath,omitempty"`
	Databases        []string       `json:"databases,omitempty"`
	CreationTime     string         `json:"creationTime,omitempty"`
	CompletionTime   string         `json:"completionTime,omitempty"`
	RestoreDatabases []RestoreDBMap `json:"restoreDatabases,omitempty"`
	StatusCode       int
}

type ListBackupsRequest struct {
	ProcType string
}

type FindRequest struct {
	TimeStamp string `json:"ts"`
	ProcType  string `json:"-"`
}

func (f *FindRequest) UnmarshalJSON(data []byte) error {
	var raw struct {
		TimeStamp json.RawMessage `json:"ts"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if raw.TimeStamp == nil {
		return nil
	}

	// Попробовать как строку
	var s string
	if err := json.Unmarshal(raw.TimeStamp, &s); err == nil {
		f.TimeStamp = s
		return nil
	}

	// Попробовать как число
	var n int64
	if err := json.Unmarshal(raw.TimeStamp, &n); err == nil {
		f.TimeStamp = strconv.FormatInt(n, 10)
		return nil
	}

	return fmt.Errorf("ts must be a string or number")
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

func convertAnyToStr(v interface{}) (string, error) {
	if v == nil {
		return "", nil
	}
	switch val := v.(type) {
	case string:
		return val, nil
	case []string:
		return strings.Join(val, ","), nil
	case int:
		return strconv.Itoa(val), nil
	case int64:
		return strconv.FormatInt(val, 10), nil
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(val), nil

	}
	return "", fmt.Errorf("cannot convert %s to str", reflect.TypeOf(v))
}

func getFieldsName(obj interface{}) []string {
	var result []string
	objValues := reflect.ValueOf(obj).Elem()
	for i := 0; i < objValues.NumField(); i++ {
		jsonStr := objValues.Type().Field(i).Tag.Get("json")
		if jsonStr == "" && objValues.Type().Field(i).Type.Kind() == reflect.Struct {
			continue
		}
		field := strings.Split(jsonStr, ",")[0]
		result = append(result, field)
	}
	return result
}
