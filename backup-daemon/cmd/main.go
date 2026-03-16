package main

import (
	"os"
	"path/filepath"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/app"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/config"
	"github.com/jessevdk/go-flags"
	"go.uber.org/zap"
	"github.com/gurkankaymak/hocon"
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

	cfg, err := loadConfig()
	if err != nil {
		l.Fatalf("failed to load config err: %v", err)
	}

	app := app.NewApp(l, &cfg)
	app.Run()
}

func loadConfigFile() (config.Config, error) {
	var cfg config.Config

	execPath, err := os.Executable()
	if err != nil {
		return cfg, err
	}

	defaultConfig := filepath.Join(filepath.Dir(execPath), "backup-daemon.conf")
	etcConfig := "/etc/backup-daemon.conf"

	var conf *hocon.Config

	if _, err := os.Stat(etcConfig); err == nil {
		etcConf, err := hocon.ParseResource(etcConfig)
		if err != nil {
			return cfg, err
		}

		defaultConf, err := hocon.ParseResource(defaultConfig)
		if err != nil {
			return cfg, err
		}

		conf = etcConf.WithFallback(defaultConf)
		
	} else if _, err := os.Stat(defaultConfig); err == nil {
		conf, err = hocon.ParseResource(defaultConfig)
		if err != nil {
			return cfg, err
		}
	} else {
		return cfg, nil
	}

	cfg.Schedule = conf.GetString("schedule")
	cfg.EvictionPolicy = conf.GetString("eviction")
	cfg.GranularEvictionPolicy = conf.GetString("granular_eviction")
	cfg.StorageRoot = conf.GetString("storage")
	cfg.BackupCmd = conf.GetString("command")
	cfg.RestoreCmd = conf.GetString("restore_command")
	cfg.DbListCmd = conf.GetString("list_instances_in_vault_command")
	cfg.CustomVars = conf.GetStringSlice("custom_vars")

	cfg.GranularSchedule = conf.GetString("granular_schedule")
	cfg.ScheduledDBs = conf.GetString("scheduled_dbs")
	cfg.EvictCmd = conf.GetString("evict_command")
	cfg.AllowPrefix = conf.GetBoolean("allow_prefix")

	return cfg, nil
}

func loadConfig() (config config.Config, err error) {

	config, err = loadConfigFile()
	if err != nil {
		_, err = flags.Parse(&config)
	}
	return config, err
}