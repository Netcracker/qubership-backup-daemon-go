package main

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/app"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/config"
	"github.com/gurkankaymak/hocon"
	"github.com/jessevdk/go-flags"
	"go.uber.org/zap"
)

type ConfigType string

const (
	Full        ConfigType = "FULL"
	Incremental ConfigType = "INCREMENTAL"
)

func main() {
	logger, _ := zap.NewProduction()

	l := logger.Sugar()
	l = l.With(zap.String("app", "backup-daemon"))
	defer func() {
		if err := logger.Sync(); err != nil {
			l.Errorf("failed to sync logger: %v", err)
		}
	}()

	fullCfg, incrCfg, err := loadConfig()
	if err != nil {
		l.Fatalf("failed to load config err: %v", err)
	}

	a := app.NewApp(l, &fullCfg, &incrCfg)
	a.Run()
}

func loadConfigFile() (*hocon.Config, error) {

	execPath, err := os.Executable()
	if err != nil {
		return nil, err
	}

	defaultConfig := filepath.Join(filepath.Dir(execPath), "backup-daemon.conf")
	etcConfig := "/etc/backup-daemon.conf"

	if _, err := os.Stat(etcConfig); err == nil {
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

	if _, err := os.Stat(defaultConfig); err == nil {
		return hocon.ParseResource(defaultConfig)
	}

	return nil, nil
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

	}
	cfg.Schedule = sanitizeString(conf.GetString(prefix + "schedule"))
	cfg.EvictionPolicy = sanitizeString(conf.GetString(prefix + "eviction"))
	cfg.GranularEvictionPolicy = sanitizeString(conf.GetString(prefix + "granular_eviction"))
	cfg.StorageRoot = sanitizeString(conf.GetString(prefix + "storage"))
	cfg.BackupCmd = sanitizeString(conf.GetString(prefix + "command"))
	cfg.RestoreCmd = sanitizeString(conf.GetString(prefix + "restore_command"))
	cfg.DbListCmd = sanitizeString(conf.GetString(prefix + "list_instances_in_vault_command"))
	cfg.CustomVars = sanitizeSlice(conf.GetStringSlice("custom_vars"))

	cfg.GranularSchedule = sanitizeString(conf.GetString("granular_schedule"))
	cfg.ScheduledDBs = sanitizeString(conf.GetString("scheduled_dbs"))
	cfg.EvictCmd = sanitizeString(conf.GetString(prefix + "evict_command"))
	cfg.AllowPrefix = conf.GetBoolean("allow_prefix")

	if strings.ToLower(sanitizeString(conf.GetString("s3_enabled"))) == "true" {
		cfg.S3Enabled = true
		cfg.S3URL = sanitizeString(conf.GetString("s3_url"))
		cfg.S3SslVerify = conf.GetBoolean("s3_ssl_verify")
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

func loadConfig() (fullCfg config.Config, incrCfg config.Config, err error) {

	var conf *hocon.Config

	conf, err = loadConfigFile()
	if err != nil {
		if _, err = flags.Parse(&fullCfg); err != nil {
			return fullCfg, incrCfg, err
		}
		return fullCfg, fullCfg, err
	}

	if conf != nil {
		fullCfg = fetchConfig(conf, Full)
		incrCfg = fetchConfig(conf, Incremental)
		return fullCfg, incrCfg, nil
	}

	return fullCfg, fullCfg, err
}
