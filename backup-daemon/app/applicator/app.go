package applicator

import (
	"context"
	"os"
	"os/signal"
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

	storageRepo := repo.NewStorageRepo(cfg.StorageRoot, cfg.ExternalRoot, cfg.Namespace, cfg.AllowPrefix)

	scheduler := controller.NewScheduler()

	s3Client, err := controller.NewS3Client(ctx, cfg.S3URL, cfg.AccessKeyID, cfg.AccessKeySecret, cfg.BucketName, cfg.Region, cfg.S3SslVerify)
	if err != nil {
		l.Fatalf("could not connect to s3 client %v", err)
	}

	executor := controller.NewExecutor(cfg.EvictCmd, cfg.BackupCmd, cfg.RestoreCmd, cfg.DbListCmd, cfg.CustomVars, cfg.DatabasesKey, cfg.DbmapKey, l)

	backupDaemon := controller.NewBackupDaemon(storageRepo, dbRepo, scheduler, s3Client, executor, cfg.S3Enabled, l, cfg.EvictionPolicy, cfg.GranularEvictionPolicy)

	endpointHandler := rest.NewEndpointHandler(backupDaemon, l)

	router := rest.NewRouter()

	server, err := rest.NewServer(cfg.Port, cfg.ShutdownTimeout, router, l, endpointHandler)
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
