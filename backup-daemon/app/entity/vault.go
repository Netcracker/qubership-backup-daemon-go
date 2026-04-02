package entity

type Vault struct {
	Folder             string                 `json:"id"`
	TimeStamp          int64                  `json:"ts"`
	MetricsFilePath    string                 `json:"metrics_filepath"`
	CustomVarsFilePath string                 `json:"custom_vars_filepath"`
	IsEvictable        bool                   `json:"evictable"`
	IsSharded          bool                   `json:"sharded"`
	External           bool                   `json:"external"`
	IsFailed           bool                   `json:"failed"`
	IsLocked           bool                   `json:"locked"`
	Canceled           bool                   `json:"canceled"`
	IsGranular         bool                   `json:"is_granular"`
	Metrics            map[string]interface{} `json:"metrics"`
}

// ToMap converts a Vault to map[string]interface{} using JSON field names as keys.
func (v Vault) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"id":          v.Folder,
		"ts":          v.TimeStamp,
		"evictable":   v.IsEvictable,
		"sharded":     v.IsSharded,
		"external":    v.External,
		"failed":      v.IsFailed,
		"locked":      v.IsLocked,
		"canceled":    v.Canceled,
		"is_granular": v.IsGranular,
	}
}
