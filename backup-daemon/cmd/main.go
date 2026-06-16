package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/app"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/config"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/logger"
	"github.com/gurkankaymak/hocon"
	"github.com/jessevdk/go-flags"
	"go.uber.org/zap"
)

type ConfigType string

const (
	Full        ConfigType = "FULL"
	Incremental ConfigType = "INCREMENTAL"

	S3AliasesFile = "s3_aliases.json"
)

func main() {
	var err error

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}
	zapLogger, err := logger.NewZapLogger(logLevel)
	if err != nil {
		panic(fmt.Sprintf("failed to initialize logger: %v", err))
	}
	defer func() {
		if err = zapLogger.Sync(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to sync logger: %v\n", err)
		}
	}()

	structuredLogger := logger.NewStructuredLoggerFromZap(zapLogger.WithOptions(zap.AddCallerSkip(1)))
	fields := logger.NewLogFields()

	structuredLogger.Info("Starting backup-daemon", fields, "log_level", logLevel)

	l := zapLogger.Sugar().With(zap.String("app", "backup-daemon"))

	fullCfg, incrCfg, err := loadConfig(structuredLogger)
	if err != nil {
		structuredLogger.Fatal("failed to load config", fields, "error", err.Error())
	}

	if fullCfg.S3AliasesUsed {
		structuredLogger.Info("S3 aliases will be used", fields)
		aliases, err := loadS3Aliases(fullCfg.AliasesPath)
		if err != nil {
			structuredLogger.Fatal("failed to load S3 aliases", fields, "path", fullCfg.AliasesPath, "error", err.Error())
		}
		fullCfg.S3Aliases = aliases
	}

	a := app.NewApp(l, &fullCfg, &incrCfg)
	a.Run()
}

func loadConfigFile() (*hocon.Config, error) {
	// BACKUP_DAEMON_CONFIG allows overriding the config file path explicitly
	// (useful for local development and non-standard deployments).
	if explicit := os.Getenv("BACKUP_DAEMON_CONFIG"); explicit != "" {
		return hocon.ParseResource(explicit)
	}

	execPath, err := os.Executable()
	if err != nil {
		return nil, err
	}

	defaultConfig := filepath.Join(filepath.Dir(execPath), "backup-daemon.conf")
	etcConfig := "/etc/backup-daemon.conf"

	if _, err = os.Stat(etcConfig); err == nil {
		etcConf, err := hocon.ParseResource(etcConfig)
		if err != nil {
			return nil, err
		}

		defaultConf, err := hocon.ParseResource(defaultConfig)
		if err != nil {
			return nil, err
		}
		return etcConf.WithFallback(defaultConf), nil
	}

	if _, err = os.Stat(defaultConfig); err == nil {
		return hocon.ParseResource(defaultConfig)
	}

	return nil, fmt.Errorf("config file not found")
}

func sanitizeString(s string) string {
	return strings.ReplaceAll(s, "\"", "")
}

func sanitizeSlice(rawVars []string) []string {
	out := make([]string, 0, len(rawVars))
	for _, cv := range rawVars {
		cv = strings.TrimSpace(cv)
		if cv == "" {
			continue
		}
		cv = sanitizeString(cv)
		out = append(out, cv)
	}
	return out
}

func buildConfig(conf *hocon.Config, prefix string) config.Config {
	var cfg config.Config

	_, err := flags.Parse(&cfg)
	if err != nil {
		log.Panicf("Error parsing flags: %v", err)
	}
	cfg.Schedule = sanitizeString(conf.GetString(prefix + "schedule"))
	cfg.EvictionPolicy = sanitizeString(conf.GetString(prefix + "eviction"))
	cfg.GranularEvictionPolicy = sanitizeString(conf.GetString(prefix + "granular_eviction"))
	cfg.StorageRoot = sanitizeString(conf.GetString(prefix + "storage"))
	if cfg.DBPath == "" {
		cfg.DBPath = filepath.Join(cfg.StorageRoot, "database.db")
	}
	cfg.BackupCmd = sanitizeString(conf.GetString(prefix + "command"))
	cfg.RestoreCmd = sanitizeString(conf.GetString(prefix + "restore_command"))
	cfg.DbListCmd = sanitizeString(conf.GetString(prefix + "list_instances_in_vault_command"))
	cfg.CustomVars = sanitizeSlice(conf.GetStringSlice("custom_vars"))

	cfg.GranularSchedule = sanitizeString(conf.GetString("granular_schedule"))
	cfg.ScheduledDBs = sanitizeString(conf.GetString("scheduled_dbs"))
	cfg.EvictCmd = sanitizeString(conf.GetString(prefix + "evict_command"))
	cfg.AllowPrefix = conf.GetBoolean("allow_prefix")

	if v := sanitizeString(conf.GetString("instances_key")); v != "" {
		cfg.DatabasesKey = v
	}
	if v := sanitizeString(conf.GetString("map_key")); v != "" {
		cfg.DbmapKey = v
	}

	if strings.ToLower(sanitizeString(conf.GetString("s3_enabled"))) == "true" {
		cfg.S3Enabled = true
		cfg.S3URL = sanitizeString(conf.GetString("s3_url"))
		cfg.S3SslVerify = conf.GetBoolean("s3_ssl_verify")
		cfg.S3CertsPath = sanitizeString(conf.GetString("s3_certs_path"))
	}

	if strings.ToLower(sanitizeString(conf.GetString("tls_enabled"))) == "true" {
		cfg.TLSEnabled = sanitizeString(conf.GetString("tls_enabled"))
		cfg.CertsPath = sanitizeString(conf.GetString("certs_path"))
		cfg.TLSPort = conf.GetInt("tls_port")
	}

	return cfg
}

func fetchConfig(conf *hocon.Config, config_type ConfigType) config.Config {
	prefix := ""
	if config_type == Incremental {
		prefix = "incremental_"
	}
	return buildConfig(conf, prefix)
}

func loadConfig(log *logger.StructuredLogger) (fullCfg config.Config, incrCfg config.Config, err error) {

	var conf *hocon.Config

	conf, err = loadConfigFile()
	if err != nil {
		log.Error("failed to load config file", logger.NewLogFields(), err)
		if _, err = flags.Parse(&fullCfg); err != nil {
			return fullCfg, incrCfg, err
		}
		return fullCfg, fullCfg, err
	}

	if conf != nil {
		fullCfg = fetchConfig(conf, "")
		incrCfg = fetchConfig(conf, Incremental)
		return fullCfg, incrCfg, nil
	}

	return fullCfg, fullCfg, err
}

func loadS3Aliases(aliasesPath string) (map[string]config.Alias, error) {
	path := filepath.Join(aliasesPath, S3AliasesFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read s3 aliases file %q: %w", path, err)
	}

	var aliases map[string]config.Alias
	if err = json.Unmarshal(data, &aliases); err != nil {
		return nil, fmt.Errorf("parse s3 aliases file %q: %w", path, err)
	}

	for name, a := range aliases {
		a.Name = name
		aliases[name] = a
	}
	return aliases, nil
}
