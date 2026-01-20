package entity

type Job struct {
	TaskID      string `json:"task_id" db:"task_id"`
	Type        string `json:"type" db:"type"`
	Status      string `json:"status" db:"status"`
	Vault       string `json:"vault" db:"vault"`
	Err         string `json:"err" db:"err"`
	StorageName string `db:"storage_name"`
	BlobPath    string `db:"blob_path"`
	Databases   string `db:"databases"`
}
