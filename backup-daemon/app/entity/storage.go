package entity

type Storage struct {
	Root              string `json:"root"`
	NameSpace         string `json:"name_space"`
	FileSystem        string `json:"file_system"`
	GranularFolder    string `json:"granular_folder"`
	RestoreLogsFolder string `json:"restore_logs_folder"`
	S3Enabled         bool   `json:"s3_enabled"`
	AllowPrefix       bool   `json:"allow_prefix"`
	ExternalRoot      string `json:"external_root"`
}
