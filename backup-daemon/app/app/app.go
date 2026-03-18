package app

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
	logger     *zap.SugaredLogger
	config     *config.Config
	incrConfig *config.Config
}

func NewApp(logger *zap.SugaredLogger, config *config.Config, incrConfig *config.Config) *App {
	return &App{
		logger:     logger,
		config:     config,
		incrConfig: incrConfig,
	}
}

// parseCustomVars converts a slice of "KEY=VALUE" strings into a map.
func parseCustomVars(rawVars []string) map[string]string {
	m := make(map[string]string)
	for _, cv := range rawVars {
		cv = strings.TrimSpace(cv)
		if cv == "" {
			continue
		}
		if idx := strings.Index(cv, "="); idx > 0 {
			m[cv[:idx]] = cv[idx+1:]
		} else {
			m[cv] = ""
		}
	}
	return m
}

// parseScheduledDBs splits a comma/space-separated list of DB names.
func parseScheduledDBs(raw string) []string {
	if raw == "" {
		return []string{}
	}
	normalized := strings.ReplaceAll(strings.ReplaceAll(raw, ",", " "), "  ", " ")
	var out []string
	for _, d := range strings.Fields(strings.TrimSpace(normalized)) {
		if d != "" {
			out = append(out, d)
		}
	}
	return out
}

func (a *App) Run() {
	var cfg = a.config
	var incrCfg = a.incrConfig
	var l = a.logger

	ctx, cancel := context.WithCancel(context.TODO())

	// Shared DB connection — one connection for both full and incremental processors.
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

	// ── FULL processor ──────────────────────────────────────────────────────
	fullDaemon, fullStorageRepo, fullScheduler := a.prepareExecutor(ctx, cfg, dbRepo)

	// ── INCREMENTAL processor ────────────────────────────────────────────────
	incrDaemon, incrStorageRepo, incrScheduler := a.prepareExecutor(ctx, incrCfg, dbRepo)

	// ── BackupExecutor (router) ──────────────────────────────────────────────
	// BackupExecutor routes calls to full or incremental daemon based on ProcType.
	// Both schedulers receive it so their cron jobs trigger through the router.
	backupExecutor := controller.NewBackupExecutor(fullDaemon, incrDaemon, fullStorageRepo, incrStorageRepo)
	fullScheduler.SetBackupDaemon(backupExecutor)
	incrScheduler.SetBackupDaemon(backupExecutor)

	// ── REST server ──────────────────────────────────────────────────────────
	serverPort := cfg.Port
	var certPath string
	var keyPath string

	if cfg.TLSEnabled == "true" {
		serverPort = cfg.TLSPort
		base := strings.TrimRight(cfg.CertsPath, "/")
		certPath = fmt.Sprintf("%s/tls.crt", base)
		keyPath = fmt.Sprintf("%s/tls.key", base)
	}

	endpointHandler := rest.NewEndpointHandler(backupExecutor, l, cfg.CustomVars...)

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

// prepareExecutor builds all components for one backup processor (full or incremental)
// from the given config, sharing the provided dbRepo connection.
// Returns the BackupDaemonUseCase, its StorageRepository, and its Scheduler.
// NOTE: SetBackupDaemon must be called on the returned scheduler after the
// BackupExecutor has been created, so cron jobs can route through it.
func (a *App) prepareExecutor(
	ctx context.Context,
	cfg *config.Config,
	dbRepo repo.DBRepository,
) (controller.BackupDaemonUseCase, repo.StorageRepository, controller.SchedulerRepository) {
	customVarsMap := parseCustomVars(cfg.CustomVars)
	scheduledDBs := parseScheduledDBs(cfg.ScheduledDBs)

	storageRepo := repo.NewStorageRepo(cfg.StorageRoot, cfg.ExternalRoot, cfg.Namespace, cfg.AllowPrefix)
	executor := controller.NewExecutor(cfg.EvictCmd, cfg.BackupCmd, cfg.RestoreCmd, cfg.DbListCmd, customVarsMap, cfg.DatabasesKey, cfg.DbmapKey, a.logger)

	s3Client, err := controller.NewS3Client(ctx, cfg.S3URL, cfg.AccessKeyID, cfg.AccessKeySecret, cfg.BucketName, cfg.Region, cfg.S3SslVerify)
	if err != nil {
		a.logger.Fatalf("could not connect to s3 client: %v", err)
	}

	// Scheduler owns the worker pool that physically executes backup/restore commands
	// via this config's executor and s3Client. Cron jobs trigger via backupDaemon
	// (set later via SetBackupDaemon).
	scheduler := controller.NewScheduler(
		storageRepo, executor, dbRepo, s3Client, cfg.S3Enabled, a.logger, 5,
		cfg.Schedule, cfg.GranularSchedule, cfg.IncrementalSchedule,
		scheduledDBs, customVarsMap,
	)

	daemon := controller.NewBackupDaemon(
		storageRepo, dbRepo, scheduler, s3Client, executor,
		cfg.S3Enabled, a.logger, cfg.EvictionPolicy, cfg.GranularEvictionPolicy,
	)

	return daemon, storageRepo, scheduler
}

func (a *App) gracefulShutdown(cancel context.CancelFunc) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	<-ch
	signal.Stop(ch)
	cancel()
}
