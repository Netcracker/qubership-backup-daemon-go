package entity

type DatabaseV2Status struct {
	DatabaseName string `json:"databaseName"`
	Status       string `json:"status"`
	ErrorMessage string `json:"errorMessage"`
	CreationTime string `json:"creationTime"`
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
	Status         string             `json:"status"`
	ErrorMessage   string             `json:"errorMessage"`
	BackupID       string             `json:"backupId"`
	CreationTime   string             `json:"creationTime"`
	CompletionTime string             `json:"completionTime"`
	StorageName    string             `json:"storageName"`
	BlobPath       string             `json:"blobPath"`
	Databases      []DatabaseV2Status `json:"databases"`
}

type RestoreV2Request struct {
	StorageName string         `json:"storageName"`
	BlobPath    string         `json:"blobPath"`
	Databases   []RestoreDBMap `json:"databases"`
	DryRun      bool           `json:"dryRun"`
}

type RestoreDatabaseV2Status struct {
	PreviousDatabaseName string `json:"previousDatabaseName"`
	DatabaseName         string `json:"databaseName"`
	Status               string `json:"status"`
	ErrorMessage         string `json:"errorMessage"`
	CreationTime         string `json:"creationTime"`
}

type RestoreV2Response struct {
	Status         string                    `json:"status"`
	ErrorMessage   string                    `json:"errorMessage"`
	RestoreID      string                    `json:"restoreId"`
	CreationTime   string                    `json:"creationTime"`
	CompletionTime string                    `json:"completionTime"`
	StorageName    string                    `json:"storageName"`
	BlobPath       string                    `json:"blobPath"`
	Databases      []RestoreDatabaseV2Status `json:"databases"`
}
