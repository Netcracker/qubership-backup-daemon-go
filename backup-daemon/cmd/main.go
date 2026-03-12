package main

import (
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

func loadConfig() (config config.Config, err error) {
	_, err = flags.Parse(&config)
	if err != nil {
		return config, err
	}

	if config.ConfigFile != "" {
		conf, err := hocon.ParseResource(config.ConfigFile)
		if err != nil {
			return config, err
		}
		config.Schedule = conf.GetString("schedule")
		config.EvictionPolicy = conf.GetString("eviction")
		config.GranularEvictionPolicy = conf.GetString("granular_eviction")
		config.StorageRoot = conf.GetString("storage")
		config.BackupCmd = conf.GetString("command")
		config.RestoreCmd = conf.GetString("restore_command")
		config.DbListCmd = conf.GetString("list_instances_in_vault_command")
		config.CustomVars = conf.GetStringSlice("custom_vars")

	}
	return config, nil
}