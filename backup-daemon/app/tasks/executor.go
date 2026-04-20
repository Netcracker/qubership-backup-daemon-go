package tasks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/repo"
	"go.uber.org/zap"
)

var ErrCommandEmpty = errors.New("command is empty")
var ErrFailedToCreateLogFile = errors.New("failed to create log file")
var ErrProcessCmdFailed = errors.New("process cmd failed")
var ErrExecuteCmdFailed = errors.New("execute cmd failed")
var ErrFailedToCloseLogFile = errors.New("failed to close log file")

//go:generate mockgen -source=executor.go -destination=../tasks/executor-mock.go -package=tasks
type CommandExecutor interface {
	ExecuteEvictCmd(vaultFolder string) error
	PerformBackup(vault entity.Vault, dbs []entity.DBEntry, customVars map[string]string) error
	PerformRestore(vaultFolder string, dbs []entity.DBEntry, dbmap map[string]string, customVariables map[string]string, external bool, taskID string) error
	GetBackupDBs(vaultFolder string) ([]string, error)
	// PerformEviction runs the full eviction cycle (list vaults → apply policy → delete obsolete).
	// Called automatically after each successful backup and also from the REST /evict endpoint.
	PerformEviction(ctx context.Context) error
	// SetEvictionPolicy hot-updates the eviction policies (called by UpdateEvictionPolicy REST handler).
	// Pass an empty string to leave a policy unchanged.
	SetEvictionPolicy(full, granular string) error
}

type Executor struct {
	evictCmdTemplate       string
	backupCmdTemplate      string
	restoreCmdTemplate     string
	dbListCmdTemplate      string
	customVars             map[string]string
	databasesKey           string
	dbmapKey               string
	storageRepo            repo.StorageRepository
	dbRepo                 repo.DBRepository
	evictionPolicy         string
	granularEvictionPolicy string
	rules                  map[string][]Rule
	evictionMu             *sync.RWMutex
	logger                 *zap.SugaredLogger
}

func NewExecutor(evictCmdTemplate string, backupCmdTemplate string, restoreCmdTemplate string, dbListCmdTemplate string, customVars map[string]string, databasesKey string, dbmapKey string, storageRepo repo.StorageRepository, dbRepo repo.DBRepository, evictionPolicy string, granularEvictionPolicy string, logger *zap.SugaredLogger) (CommandExecutor, error) {
	rules := map[string][]Rule{}
	if evictionPolicy != "" {
		full, err := parseRules(evictionPolicy)
		if err != nil {
			return nil, err
		}
		rules[repo.FULL] = full
	}

	if granularEvictionPolicy != "" {
		granular, err := parseRules(evictionPolicy)
		if err != nil {
			return nil, err
		}
		rules[repo.GRANULAR] = granular
	}

	return &Executor{
		evictCmdTemplate:       evictCmdTemplate,
		backupCmdTemplate:      backupCmdTemplate,
		restoreCmdTemplate:     restoreCmdTemplate,
		dbListCmdTemplate:      dbListCmdTemplate,
		customVars:             customVars,
		databasesKey:           databasesKey,
		dbmapKey:               dbmapKey,
		storageRepo:            storageRepo,
		dbRepo:                 dbRepo,
		evictionPolicy:         evictionPolicy,
		granularEvictionPolicy: granularEvictionPolicy,
		logger:                 logger,
		rules:                  rules,
		evictionMu:             &sync.RWMutex{},
	}, nil
}

func (e *Executor) ExecuteEvictCmd(vaultFolder string) error {
	if len(e.evictCmdTemplate) == 0 {
		// evict_command is optional; if not configured, skip silently.
		return nil
	}
	cmdProcessed, err := e.processCmd(e.evictCmdTemplate, vaultFolder, nil, nil, nil)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrProcessCmdFailed, err)
	}
	if len(cmdProcessed) == 0 {
		return ErrCommandEmpty
	}

	cmd := exec.Command(cmdProcessed[0], cmdProcessed[1:]...)
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrExecuteCmdFailed, err)
	}
	return nil
}

func (e *Executor) ExecuteTerminationCmd() {

}

func (e *Executor) PerformBackup(vault entity.Vault, dbs []entity.DBEntry, customVars map[string]string) error {
	start := time.Now()
	e.logger.Info("Starting backup", zap.String("vault", vault.Folder), zap.Int("db_count", len(dbs)), zap.Any("custom_vars", customVars))
	if err := os.MkdirAll(vault.Folder, 0o755); err != nil {
		return fmt.Errorf("%w: vault=%s err=%v", ErrFailedToCreateLogFile, vault.Folder, err)
	}

	// Create .lock file to prevent eviction while backup is running.
	lockPath := filepath.Join(vault.Folder, ".lock")
	if err := os.WriteFile(lockPath, []byte{}, 0o644); err != nil {
		return fmt.Errorf("%w: vault=%s err=%v", ErrFailedToCreateLogFile, vault.Folder, err)
	}
	defer func() {
		_ = os.Remove(lockPath)
	}()

	customVarsPath := vault.CustomVarsFilePath
	if strings.TrimSpace(customVarsPath) == "" {
		customVarsPath = filepath.Join(vault.Folder, ".custom_vars")
	}
	// Build effective vars: start from defaults, override with request-time values.
	effectiveVars := make(map[string]string, len(e.customVars))
	for k, v := range e.customVars {
		effectiveVars[k] = v
	}
	if len(effectiveVars) > 0 {
		if b, mErr := json.Marshal(effectiveVars); mErr == nil {
			_ = os.WriteFile(customVarsPath, b, 0o644)
		}
	}

	defer func() {
		var err error
		metricsPath := vault.MetricsFilePath
		if strings.TrimSpace(metricsPath) == "" {
			metricsPath = filepath.Join(vault.Folder, ".metrics")
		}

		sizeBytes, sizeErr := dirSize(vault.Folder)
		if sizeErr != nil {
			e.logger.Errorw("Failed to calculate dir size for metrics", "folder", vault.Folder, "error", sizeErr)
		}

		m := map[string]any{
			"spent_time": int64(time.Since(start) / time.Millisecond),
			"size":       sizeBytes,
		}
		if err != nil {
			m["exception"] = err.Error()
		}

		b, mErr := json.Marshal(m)
		if mErr != nil {
			e.logger.Errorw("Failed to marshal metrics", "error", mErr)
			return
		}
		if writeErr := os.WriteFile(metricsPath, b, 0o644); writeErr != nil {
			e.logger.Errorw("Failed to write metrics file", "path", metricsPath, "error", writeErr)
		}
	}()

	cmdProcessed, err := e.processCmd(e.backupCmdTemplate, vault.Folder, dbs, nil, customVars)
	if err != nil {
		return fmt.Errorf("%w: vault=%s err=%v", ErrProcessCmdFailed, vault.Folder, err)
	}
	if len(cmdProcessed) == 0 {
		return fmt.Errorf("%w: vault=%s", ErrCommandEmpty, vault.Folder)
	}
	logFilePath := vault.Folder + "/.console"

	logFile, err := os.Create(logFilePath)
	if err != nil {
		return fmt.Errorf("%w: vault=%s path=%s err=%v",
			ErrFailedToCreateLogFile, vault.Folder, logFilePath, err)
	}
	defer func() {
		errFile := logFile.Close()
		if errFile != nil && err == nil {
			err = fmt.Errorf("%w: vault=%s path=%s err=%v",
				ErrFailedToCloseLogFile, vault.Folder, logFilePath, errFile)
		}
	}()

	e.logger.Info("Executing backup command", zap.Strings("cmd", cmdProcessed), zap.String("log_file", logFilePath))
	cmd := exec.Command(cmdProcessed[0], cmdProcessed[1:]...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err = cmd.Run(); err != nil {
		_ = logFile.Close()
		logContent, readErr := os.ReadFile(logFilePath)
		if readErr == nil && len(logContent) > 0 {
			return fmt.Errorf("%w: vault=%s cmd=%q err=%v\nScript output:\n%s", ErrExecuteCmdFailed, vault.Folder, strings.Join(cmdProcessed, " "), err, string(logContent))
		}
		return fmt.Errorf("%w: vault=%s cmd=%q err=%v", ErrExecuteCmdFailed, vault.Folder, strings.Join(cmdProcessed, " "), err)
	}
	e.logger.Info("Backup finished successfully", zap.String("vault", vault.Folder))
	return nil
}

func (e *Executor) PerformRestore(vaultFolder string, dbs []entity.DBEntry,
	dbmap map[string]string, customVariables map[string]string, external bool, taskID string) (err error) {
	cmdProcessed, err := e.processCmd(e.restoreCmdTemplate, vaultFolder, dbs, dbmap, customVariables)
	if err != nil {
		return fmt.Errorf("%w: process restore command for vault=%s task=%s: %v", ErrProcessCmdFailed, vaultFolder, taskID, err)
	}
	if len(cmdProcessed) == 0 {
		return fmt.Errorf("%w: restore command empty for vault=%s task=%s", ErrCommandEmpty, vaultFolder, taskID)
	}
	logFilePath := fmt.Sprintf("%s/restore_%s.log", vaultFolder, taskID)
	if !external {
		logsDir := fmt.Sprintf("%s/restore_logs", vaultFolder)
		if err = os.MkdirAll(logsDir, os.ModePerm); err != nil {
			return fmt.Errorf("%w: create restore logs directory=%s for task=%s: %v", ErrFailedToCreateLogFile, logsDir, taskID, err)
		}
		logFilePath = fmt.Sprintf("%s/%s.log", logsDir, taskID)
	}
	logFile, err := os.Create(logFilePath)
	if err != nil {
		return fmt.Errorf("%w: create restore log file=%s for task=%s: %v", ErrFailedToCreateLogFile, logFilePath, taskID, err)
	}
	defer func() {
		errFile := logFile.Close()
		if errFile != nil && err == nil {
			err = fmt.Errorf("%w: close restore log file=%s for task=%s: %v", ErrFailedToCloseLogFile, logFilePath, taskID, errFile)
		}
	}()
	e.logger.Info("starting restore command", zap.Strings("command", cmdProcessed), zap.String("task_id", taskID))
	cmd := exec.Command(cmdProcessed[0], cmdProcessed[1:]...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err = cmd.Run(); err != nil {
		_ = logFile.Close()
		logContent, readErr := os.ReadFile(logFilePath)
		if readErr == nil && len(logContent) > 0 {
			return fmt.Errorf("%w: execute restore command for task=%s cmd=%v: %v\nScript output:\n%s", ErrExecuteCmdFailed, taskID, cmdProcessed, err, string(logContent))
		}
		return fmt.Errorf("%w: execute restore command for task=%s cmd=%v: %v", ErrExecuteCmdFailed, taskID, cmdProcessed, err)
	}
	e.logger.Info("restore command executed successfully", zap.String("task_id", taskID),
		zap.Strings("command", cmdProcessed), zap.String("log_path", logFilePath))
	return nil
}

func (e *Executor) GetBackupDBs(vaultFolder string) ([]string, error) {
	cmdProcessed, err := e.processCmd(e.dbListCmdTemplate, vaultFolder, nil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrProcessCmdFailed, err)
	}
	if len(cmdProcessed) == 0 {
		return nil, ErrCommandEmpty
	}
	cmd := exec.Command(cmdProcessed[0], cmdProcessed[1:]...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("%w: cmd=%v stderr=%s err=%v",
			ErrExecuteCmdFailed, cmdProcessed, strings.TrimSpace(stderr.String()), err)
	}

	lines := strings.Split(stdout.String(), "\n")
	var result []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result, nil
}

func (e *Executor) processCmd(cmdTemplate string, vaultFolder string, dbs []entity.DBEntry,
	dbmap map[string]string, customVariables map[string]string) ([]string, error) {
	e.logger.Info("Processing command template", zap.String("template", cmdTemplate), zap.String("vault_folder", vaultFolder),
		zap.Int("db_count", len(dbs)), zap.Any("custom_vars", customVariables))

	cmdOptions := map[string]string{
		"data_folder": vaultFolder,
		"dbs":         "",
		"dbmap":       "",
	}
	for customVar := range e.customVars {
		if val, ok := customVariables[customVar]; ok && val != "" {
			cmdOptions[customVar] = fmt.Sprintf("-%s %s", customVar, val)
		} else {
			cmdOptions[customVar] = ""
		}
	}
	if len(dbs) > 0 {
		var entries []interface{}
		for _, db := range dbs {
			if db.SimpleName != "" {
				entries = append(entries, db.SimpleName)
			} else if len(db.Object) > 0 {
				entries = append(entries, db.Object)
			}
		}

		if len(entries) > 0 {
			dbsJSON, err := json.Marshal(entries)
			if err != nil {
				return nil, fmt.Errorf("marshal dbs: %w", err)
			}
			cmdOptions["dbs"] = fmt.Sprintf("%s %s", e.databasesKey, string(dbsJSON))
		}
	}

	if len(dbmap) > 0 {
		dbmapJSON, err := json.Marshal(dbmap)
		if err != nil {
			return nil, err
		}
		cmdOptions["dbmap"] = fmt.Sprintf("%s %s", e.dbmapKey, string(dbmapJSON))
	}
	tmpl, err := template.New("cmd").Option("missingkey=zero").Parse(cmdTemplate)
	if err != nil {
		return nil, fmt.Errorf("parse template: %w", err)
	}

	var sb strings.Builder
	if err = tmpl.Execute(&sb, cmdOptions); err != nil {
		return nil, fmt.Errorf("execute template: %w", err)
	}
	cmdProcessed := strings.Fields(sb.String())
	e.logger.Info("Processed command", zap.Strings("cmd", cmdProcessed))
	return cmdProcessed, nil
}

func dirSize(root string) (int64, error) {
	var size int64
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		size += info.Size()
		return nil
	})
	return size, err
}

// SetEvictionPolicy hot-updates eviction policies. An empty string leaves the existing value unchanged.
func (e *Executor) SetEvictionPolicy(full, granular string) error {
	e.evictionMu.Lock()
	defer e.evictionMu.Unlock()
	if full != "" {
		e.evictionPolicy = full
		fullRules, err := parseRules(full)
		if err != nil {
			return err
		}
		e.rules[repo.FULL] = fullRules
	}
	if granular != "" {
		e.granularEvictionPolicy = granular
		granularRules, err := parseRules(full)
		if err != nil {
			return err
		}
		e.rules[repo.GRANULAR] = granularRules
	}
	return nil
}

// PerformEviction mirrors Python's BackupProcessor.perform_evictions():
// lists full and granular vaults, applies retention policies, and removes obsolete ones.
func (e *Executor) PerformEviction(ctx context.Context) error {
	e.evictionMu.RLock()
	evictionPolicy := e.evictionPolicy
	granularEvictionPolicy := e.granularEvictionPolicy
	rules := e.rules
	e.evictionMu.RUnlock()

	if evictionPolicy == "" && granularEvictionPolicy == "" {
		e.logger.Debug("No eviction policies configured, skipping eviction")
		return nil
	}

	excludedFiles, err := e.storageRepo.GetNonEvictableVaults(repo.ALL)
	if err != nil {
		return fmt.Errorf("failed to list non-evictable vaults: %w", err)
	}

	var obsoleteVaults []entity.Vault

	if evictionPolicy != "" {
		e.logger.Infof("Start full eviction process by policy: %s", evictionPolicy)
		fullVaults, listErr := e.storageRepo.List(repo.FULL, "")
		if listErr != nil && !errors.Is(listErr, repo.ErrNoVaults) {
			return fmt.Errorf("failed to list full vaults: %w", listErr)
		}
		if len(fullVaults) > 0 {
			obsoleteFull, evErr := e.evict(fullVaults, rules[repo.FULL], excludedFiles)
			if evErr != nil {
				return fmt.Errorf("failed to calculate obsolete full vaults: %w", evErr)
			}
			e.logger.Infof("Deleting full vaults: %v", vaultNames(obsoleteFull))
			obsoleteVaults = append(obsoleteVaults, obsoleteFull...)
		}
	}

	if granularEvictionPolicy != "" {
		e.logger.Infof("Start granular eviction process by policy: %s", granularEvictionPolicy)
		granularVaults, listErr := e.storageRepo.List(repo.GRANULAR, "")
		if listErr != nil && !errors.Is(listErr, repo.ErrNoVaults) {
			return fmt.Errorf("failed to list granular vaults: %w", listErr)
		}
		if len(granularVaults) > 0 {
			obsoleteGranular, evErr := e.evict(granularVaults, rules[repo.GRANULAR], excludedFiles)
			if evErr != nil {
				return fmt.Errorf("failed to calculate obsolete granular vaults: %w", evErr)
			}
			e.logger.Infof("Deleting granular vaults: %v", vaultNames(obsoleteGranular))
			obsoleteVaults = append(obsoleteVaults, obsoleteGranular...)
		}
	}

	if len(obsoleteVaults) == 0 {
		e.logger.Info("No obsolete vaults to evict")
		return nil
	}

	for _, obsoleteVault := range obsoleteVaults {
		if err := e.ExecuteEvictCmd(obsoleteVault.Folder); err != nil {
			return fmt.Errorf("failed to execute evict command for %s: %w", obsoleteVault.Folder, err)
		}
		if err := e.storageRepo.Evict(obsoleteVault.Folder); err != nil {
			return fmt.Errorf("failed to evict backup %s from storage: %w", obsoleteVault.Folder, err)
		}
		if err := e.dbRepo.RemoveVault(ctx, e.storageRepo.GetName(obsoleteVault.Folder)); err != nil && !errors.Is(err, repo.ErrNoVaults) {
			return fmt.Errorf("failed to remove backup %s from database: %w", obsoleteVault.Folder, err)
		}
	}
	return nil
}

// evict applies the retention rules to items and returns obsolete vaults.
func (e *Executor) evict(items []entity.Vault, parsedRules []Rule, exclude map[int64]bool) ([]entity.Vault, error) {
	if len(items) == 0 {
		return items, nil
	}

	if len(parsedRules) == 0 {
		return nil, fmt.Errorf("evict empty rules")
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].TimeStamp > items[j].TimeStamp
	})
	rule := parsedRules[0]
	var obsolete []entity.Vault

	switch rule.Type {
	case LimitType:
		limit := rule.First
		unique := uniqueVaults(items)
		sort.Slice(unique, func(i, j int) bool {
			return unique[i].TimeStamp > unique[j].TimeStamp
		})
		if limit < int64(len(unique)) {
			obsolete = append(obsolete, unique[limit:]...)
		}
		return obsolete, nil
	case IntervalType:
		to := time.Now().UnixMilli()
		for _, r := range parsedRules {
			var operateVersions []entity.Vault
			for _, x := range items {
				if x.TimeStamp <= to-r.First && !exclude[x.TimeStamp] {
					operateVersions = append(operateVersions, x)
				}
			}
			if r.Second == "delete" {
				obsolete = append(obsolete, operateVersions...)
			} else {
				interval := r.Second.(int64)
				thursday := int64(4 * 24 * 60 * 60 * 1000)
				groups := make(map[int64][]entity.Vault)
				for _, x := range operateVersions {
					key := (x.TimeStamp - thursday) / interval
					groups[key] = append(groups[key], x)
				}
				for _, versions := range groups {
					sort.Slice(versions, func(i, j int) bool {
						return versions[i].TimeStamp < versions[j].TimeStamp
					})
					obsolete = append(obsolete, versions[:len(versions)-1]...)
				}
			}
		}
		return uniqueVaults(obsolete), nil
	}
	return obsolete, nil
}

// uniqueVaults deduplicates by timestamp.
func uniqueVaults(arr []entity.Vault) []entity.Vault {
	seen := make(map[int64]struct{})
	var res []entity.Vault
	for _, v := range arr {
		if _, ok := seen[v.TimeStamp]; !ok {
			seen[v.TimeStamp] = struct{}{}
			res = append(res, v)
		}
	}
	return res
}

// vaultNames extracts folder basenames for logging.
func vaultNames(vaults []entity.Vault) []string {
	names := make([]string, len(vaults))
	for i, v := range vaults {
		names[i] = filepath.Base(v.Folder)
	}
	return names
}
