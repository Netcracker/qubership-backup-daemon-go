package rest

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
)

const (
	NotStarted = "notStarted"
	InProgress = "inProgress"
	Completed  = "completed"
	Failed     = "failed"
	Unknown    = "unknown"
)

func normalizeBlobPath(p string) string {
	p = strings.TrimSpace(p)
	p = strings.Trim(p, "\"'")
	p = strings.TrimSpace(p)
	p = strings.Trim(p, "/")
	return p
}

func validateBlobPath(p string) (string, error) {
	p = normalizeBlobPath(p)
	if p == "" {
		return "", fmt.Errorf("blobPath must be a non-empty string")
	}
	return p, nil
}

func DBEntries(names []string) []entity.DBEntry {
	if len(names) == 0 {
		return nil
	}
	out := make([]entity.DBEntry, 0, len(names))
	for _, n := range names {
		n = strings.TrimSpace(n)
		if n == "" {
			continue
		}
		out = append(out, entity.DBEntry{SimpleName: n})
	}
	return out
}

func DbStatuses(names []string, status string, creationTime string) []entity.DatabaseV2Status {
	if len(names) == 0 {
		return []entity.DatabaseV2Status{}
	}
	out := make([]entity.DatabaseV2Status, 0, len(names))
	for _, n := range names {
		n = strings.TrimSpace(n)
		if n == "" {
			continue
		}
		var errMsg string
		if status == Failed {
			errMsg = "backup failed"
		}
		out = append(out, entity.DatabaseV2Status{
			DatabaseName: n,
			Status:       status,
			ErrorMessage: errMsg,
			CreationTime: creationTime,
		})
	}
	return out
}

func mapJobStatus(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "queued":
		return NotStarted
	case "processing":
		return InProgress
	case "successful":
		return Completed
	case "failed":
		return Failed
	default:
		return Unknown
	}
}

func mapBackupV2ToInternal(req entity.BackupV2Request, procType string) entity.BackupRequest {
	custom := map[string]string{
		"storageName": req.StorageName,
		"blob_path":   req.BlobPath,
	}

	return entity.BackupRequest{
		DBs:           DBEntries(req.Databases),
		AllowEviction: strconv.FormatBool(true),
		Sharded:       false,
		CustomVars:    custom,
		ProcType:      procType,
	}
}

func mapRestoreV2ToInternal(backupID string, req entity.RestoreV2Request, procType string) entity.RestoreRequest {
	custom := map[string]string{
		"storageName": req.StorageName,
		"blob_path":   req.BlobPath,
		"dryRun":      strconv.FormatBool(req.DryRun),
	}

	dbs := make([]entity.DBEntry, 0, len(req.Databases))
	dbmap := make(map[string]string, len(req.Databases))

	for _, m := range req.Databases {
		if strings.TrimSpace(m.PreviousDatabaseName) == "" {
			continue
		}
		dbs = append(dbs, entity.DBEntry{SimpleName: m.PreviousDatabaseName})
		if strings.TrimSpace(m.DatabaseName) != "" && m.DatabaseName != m.PreviousDatabaseName {
			dbmap[m.PreviousDatabaseName] = m.DatabaseName
		}
	}

	return entity.RestoreRequest{
		Vault:         backupID,
		DBs:           dbs,
		ChangeDbNames: dbmap,
		CustomVars:    custom,
		ProcType:      procType,
		RestoreDBMaps: req.Databases,
	}
}

func buildBackupV2Response(req entity.BackupV2Request, backupID string, status string, creationTime string) entity.BackupV2Response {
	var completionTime string
	if status == Completed {
		completionTime = creationTime
	}
	return entity.BackupV2Response{
		Status:         status,
		ErrorMessage:   "",
		BackupID:       backupID,
		CreationTime:   creationTime,
		CompletionTime: completionTime,
		StorageName:    req.StorageName,
		BlobPath:       req.BlobPath,
		Databases:      DbStatuses(req.Databases, status, creationTime),
	}
}

func buildRestoreV2Response(req entity.RestoreV2Request, restoreID string, status string, creationTime string) entity.RestoreV2Response {
	var completionTime string
	if status == Completed {
		completionTime = creationTime
	}
	return entity.RestoreV2Response{
		Status:         status,
		ErrorMessage:   "",
		RestoreID:      restoreID,
		CreationTime:   creationTime,
		CompletionTime: completionTime,
		StorageName:    req.StorageName,
		BlobPath:       req.BlobPath,
		Databases:      RestoreDbStatuses(req.Databases, status, creationTime),
	}
}

func RestoreDBEntries(maps []entity.RestoreDBMap) []entity.DBEntry {
	out := make([]entity.DBEntry, 0, len(maps))
	for _, m := range maps {
		src := strings.TrimSpace(m.PreviousDatabaseName)
		if src == "" {
			continue
		}
		out = append(out, entity.DBEntry{SimpleName: src})
	}
	return out
}

func RestoreDBRenameMap(maps []entity.RestoreDBMap) map[string]string {
	var out map[string]string
	for _, m := range maps {
		src := strings.TrimSpace(m.PreviousDatabaseName)
		dst := strings.TrimSpace(m.DatabaseName)
		if src == "" || dst == "" || src == dst {
			continue
		}
		if out == nil {
			out = make(map[string]string)
		}
		out[src] = dst
	}
	return out
}

func RestoreDbStatuses(maps []entity.RestoreDBMap, status string, creationTime string) []entity.RestoreDatabaseV2Status {
	out := make([]entity.RestoreDatabaseV2Status, 0, len(maps))
	for _, m := range maps {
		prev := strings.TrimSpace(m.PreviousDatabaseName)
		name := strings.TrimSpace(m.DatabaseName)
		if name == "" {
			name = prev
		}
		if name == "" {
			continue
		}
		var errMsg string
		var duration int
		if status == Failed {
			errMsg = "restore failed"
		}
		out = append(out, entity.RestoreDatabaseV2Status{
			MicroserviceName:     m.MicroserviceName,
			Namespace:            m.Namespace,
			Prefix:               m.Prefix,
			PreviousDatabaseName: prev,
			DatabaseName:         name,
			Status:               status,
			Duration:             duration,
			Path:                 m.Path,
			ErrorMessage:         errMsg,
			CreationTime:         creationTime,
		})
	}
	return out
}

func RestoreDbStatusesFromNames(names []string, status string, creationTime string) []entity.RestoreDatabaseV2Status {
	if len(names) == 0 {
		return []entity.RestoreDatabaseV2Status{}
	}
	out := make([]entity.RestoreDatabaseV2Status, 0, len(names))
	for _, n := range names {
		n = strings.TrimSpace(n)
		if n == "" {
			continue
		}
		var errMsg string
		if status == Failed {
			errMsg = "restore failed"
		}
		out = append(out, entity.RestoreDatabaseV2Status{
			DatabaseName: n,
			Status:       status,
			ErrorMessage: errMsg,
			CreationTime: creationTime,
		})
	}
	return out
}
