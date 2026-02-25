package applicator

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/config"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/controller"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/db"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/repo"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/rest"
	"go.uber.org/zap"
)

type App struct {
	logger *zap.SugaredLogger
	config *config.Config
}

func NewApp(logger *zap.SugaredLogger, config *config.Config) *App {
	return &App{
		logger: logger,
		config: config,
	}
}

func (a *App) Run() {
	var cfg = a.config
	var l = a.logger

	ctx, cancel := context.WithCancel(context.TODO())
	_ = ctx

	dbConnections, err := db.NewConnection(cfg.DBPath)
	if err != nil {
		l.Fatalf("could not connect to database %w", err)
	}
	defer func() {
		if errDb := dbConnections.Close(); errDb != nil {
			l.Fatalf("could not close database %w", err)
		}
	}()

	dbRepo := repo.NewDBRepo(dbConnections)
	l.Info("AllowPrefix: %s", cfg.AllowPrefix)

	storageRepo := repo.NewStorageRepo(cfg.StorageRoot, cfg.ExternalRoot, cfg.Namespace, cfg.AllowPrefix)

	customVarsMap := make(map[string]string)
	for _, cv := range cfg.CustomVars {
		if idx := strings.Index(cv, "="); idx > 0 {
			key := cv[:idx]
			value := cv[idx+1:]
			customVarsMap[key] = value
		}
	}
	scheduledDBs := []string{}
	if cfg.ScheduledDBs != "" {
		normalized := strings.ReplaceAll(strings.ReplaceAll(cfg.ScheduledDBs, ",", " "), "  ", " ")
		for _, db := range strings.Fields(strings.TrimSpace(normalized)) {
			if db != "" {
				scheduledDBs = append(scheduledDBs, db)
			}
		}
		l.Infof("Parsed scheduled databases: input=%q output=%v", cfg.ScheduledDBs, scheduledDBs)
	}

	executor := controller.NewExecutor(cfg.EvictCmd, cfg.BackupCmd, cfg.RestoreCmd, cfg.DbListCmd, cfg.CustomVars, cfg.DatabasesKey, cfg.DbmapKey, l)

	s3Client, err := controller.NewS3Client(ctx, cfg.S3URL, cfg.AccessKeyID, cfg.AccessKeySecret, cfg.BucketName, cfg.Region, cfg.S3SslVerify)
	if err != nil {
		l.Fatalf("could not connect to s3 client %v", err)
	}

	scheduler := controller.NewScheduler(storageRepo, executor, dbRepo, s3Client, cfg.S3Enabled, l, 5, cfg.Schedule, cfg.GranularSchedule, cfg.IncrementalSchedule, scheduledDBs, customVarsMap)

	backupDaemon := controller.NewBackupDaemon(storageRepo, dbRepo, scheduler, s3Client, executor, cfg.S3Enabled, l, cfg.EvictionPolicy, cfg.GranularEvictionPolicy)

	scheduler.SetBackupDaemon(backupDaemon)
	serverPort := cfg.Port
	var certPath string
	var keyPath string

	if cfg.TLSEnabled == "true" {
		serverPort = cfg.TLSPort
		base := strings.TrimRight(cfg.CertsPath, "/")
		certPath = fmt.Sprintf("%s/tls.crt", base)
		keyPath = fmt.Sprintf("%s/tls.key", base)
	}

	endpointHandler := rest.NewEndpointHandler(backupDaemon, l)

	router := rest.NewRouter()

	server, err := rest.NewServer(serverPort, cfg.ShutdownTimeout, router, l, endpointHandler, certPath, keyPath)
	if err != nil {
		l.Fatalf("failed to create server err: %v", err)
	}

	server.Run()
	defer func() {
		if err := server.Stop(); err != nil {
			l.Panicf("failed close server err: %v", err)
		}
		l.Info("server closed")
	}()

	a.gracefulShutdown(cancel)
}

func (a *App) gracefulShutdown(cancel context.CancelFunc) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	<-ch
	signal.Stop(ch)
	cancel()
}
