package entity

type DatabaseV2Status struct {
	DatabaseName string `json:"databaseName"`
	Status       string `json:"status"`
}

type RestoreDBMap struct {
	PreviousDatabaseName string `json:"previousDatabaseName"`
	DatabaseName         string `json:"databaseName"`
}

type BackupV2Request struct {
	StorageName string   `json:"storageName"`
	BlobPath    string   `json:"blobPath"`
	Databases   []string `json:"databases"`
}

type BackupV2Response struct {
	Status       string             `json:"status"`
	BackupID     string             `json:"backupId"`
	CreationTime string             `json:"creationTime"`
	StorageName  string             `json:"storageName"`
	BlobPath     string             `json:"blobPath"`
	Databases    []DatabaseV2Status `json:"databases"`
}

type RestoreV2Request struct {
	StorageName string         `json:"storageName"`
	BlobPath    string         `json:"blobPath"`
	Databases   []RestoreDBMap `json:"databases"`
	DryRun      bool           `json:"dryRun"`
}

type RestoreV2Response struct {
	Status       string             `json:"status"`
	RestoreID    string             `json:"restoreId"`
	CreationTime string             `json:"creationTime"`
	StorageName  string             `json:"storageName"`
	BlobPath     string             `json:"blobPath"`
	Databases    []DatabaseV2Status `json:"databases"`
}
