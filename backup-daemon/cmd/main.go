package main

import (
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/applicator"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/config"
	"github.com/jessevdk/go-flags"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()

	l := logger.Sugar()
	l = l.With(zap.String("applicator", "backup-daemon"))
	defer func() {
		if err := logger.Sync(); err != nil {
			l.Errorf("failed to sync logger: %v", err)
		}
	}()

	cfg, err := loadConfig()
	if err != nil {
		l.Fatalf("failed to load config err: %v", err)
	}

	app := applicator.NewApp(l,&cfg)
	app.Run()
}

func loadConfig() (config config.Config, err error) {
	_, err = flags.Parse(&config)
	if err != nil {
		return config, err
	}
	return config, nil
}