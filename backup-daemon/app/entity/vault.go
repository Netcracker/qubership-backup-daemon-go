package entity

type Vault struct {
	Folder             string                 `json:"folder"`
	TimeStamp          int64                  `json:"time_stamp"`
	MetricsFilePath    string                 `json:"metrics_filepath"`
	CustomVarsFilePath string                 `json:"custom_vars_filepath"`
	IsEvictable        bool                   `json:"is_evictable"`
	IsSharded          bool                   `json:"is_sharded"`
	External           bool                   `json:"external"`
	IsFailed           bool                   `json:"is_failed"`
	IsLocked           bool                   `json:"is_locked"`
	Canceled           bool                   `json:"canceled"`
	IsGranular         bool                   `json:"is_granular"`
	Metrics            map[string]interface{} `json:"metrics"`
}
