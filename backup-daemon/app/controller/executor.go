package controller

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/google/shlex"
	"go.uber.org/zap"
)

var ErrCommandEmpty = errors.New("command is empty")
var ErrFailedToCreateLogFile = errors.New("failed to create log file")
var ErrProcessCmdFailed = errors.New("process cmd failed")
var ErrExecuteCmdFailed = errors.New("execute cmd failed")
var ErrFailedToCloseLogFile = errors.New("failed to close log file")

type CommandExecutor interface {
	ExecuteEvictCmd(vaultFolder string) error
	PerformBackup(vault entity.Vault, dbs []entity.DBEntry, customVars map[string]string) error
	PerformRestore(vaultFolder string, dbs []entity.DBEntry, dbmap map[string]string, customVariables map[string]string, external bool, taskID string) error
	GetBackupDBs(vaultFolder string) ([]string, error)
}

type Executor struct {
	evictCmdTemplate   string
	backupCmdTemplate  string
	restoreCmdTemplate string
	dbListCmdTemplate  string
	customVars         []string
	databasesKey       string
	dbmapKey           string
	logger             *zap.SugaredLogger
}

func NewExecutor(evictCmdTemplate string, backupCmdTemplate string, restoreCmdTemplate string,
	dbListCmdTemplate string, customVars []string, databasesKey string, dbmapKey string,
	logger *zap.SugaredLogger) CommandExecutor {
	return &Executor{
		evictCmdTemplate:   evictCmdTemplate,
		backupCmdTemplate:  backupCmdTemplate,
		restoreCmdTemplate: restoreCmdTemplate,
		dbListCmdTemplate:  dbListCmdTemplate,
		customVars:         customVars,
		databasesKey:       databasesKey,
		dbmapKey:           dbmapKey,
		logger:             logger,
	}
}

func (e *Executor) ExecuteEvictCmd(vaultFolder string) error {
	if len(e.evictCmdTemplate) == 0 {
		return fmt.Errorf("evict cmd template is empty")
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

func (e *Executor) PerformBackup(vault entity.Vault, dbs []entity.DBEntry, customVars map[string]string) (err error) {
	start := time.Now()
	e.logger.Info("Starting backup", zap.String("vault", vault.Folder), zap.Int("db_count", len(dbs)), zap.Any("custom_vars", customVars))
	if err := os.MkdirAll(vault.Folder, 0o755); err != nil {
		return fmt.Errorf("%w: vault=%s err=%v", ErrFailedToCreateLogFile, vault.Folder, err)
	}

	customVarsPath := vault.CustomVarsFilePath
	if strings.TrimSpace(customVarsPath) == "" {
		customVarsPath = filepath.Join(vault.Folder, ".custom_vars")
	}
	if len(customVars) > 0 {
		if b, mErr := json.Marshal(customVars); mErr == nil {
			_ = os.WriteFile(customVarsPath, b, 0o644)
		}
	}

	defer func() {
		metricsPath := vault.MetricsFilePath
		if strings.TrimSpace(metricsPath) == "" {
			metricsPath = filepath.Join(vault.Folder, ".metrics")
		}

		sizeBytes, _ := dirSize(vault.Folder)

		m := map[string]any{
			"spent_time": int64(time.Since(start) / time.Millisecond),
			"size":       sizeBytes,
		}
		if err != nil {
			m["exception"] = err.Error()
		}

		if b, mErr := json.Marshal(m); mErr == nil {
			_ = os.WriteFile(metricsPath, b, 0o644)
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
	for _, customVar := range e.customVars {
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
			cmdOptions["dbs"] = fmt.Sprintf("%s '%s'", e.databasesKey, string(dbsJSON))
		}
	}

	if len(dbmap) > 0 {
		dbmapJSON, err := json.Marshal(dbmap)
		if err != nil {
			return nil, err
		}
		cmdOptions["dbmap"] = fmt.Sprintf("%s '%s'", e.dbmapKey, string(dbmapJSON))
	}
	tmpl, err := template.New("cmd").Parse(cmdTemplate)
	if err != nil {
		return nil, fmt.Errorf("parse template: %w", err)
	}

	var sb strings.Builder
	if err := tmpl.Execute(&sb, cmdOptions); err != nil {
		return nil, fmt.Errorf("execute template: %w", err)
	}
	cmdProcessed, err := shlex.Split(sb.String())
	if err != nil {
		return nil, fmt.Errorf("failed to parse command: %w", err)
	}

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
