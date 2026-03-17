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

func (a *App) Run() {
	var cfg = a.config
	var incrCfg = a.incrConfig
	var l = a.logger

	ctx, cancel := context.WithCancel(context.TODO())
	_ = ctx

	// Shared DB connection (port, TLS, DB path come from full config)
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

	// --- helpers ---
	parseCustomVars := func(vars []string) map[string]string {
		m := make(map[string]string)
		for _, cv := range vars {
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

	parseScheduledDBs := func(raw string) []string {
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

	// ── FULL processor ──────────────────────────────────────────────────────
	fullCustomVars := parseCustomVars(cfg.CustomVars)
	fullScheduledDBs := parseScheduledDBs(cfg.ScheduledDBs)

	fullStorageRepo := repo.NewStorageRepo(cfg.StorageRoot, cfg.ExternalRoot, cfg.Namespace, cfg.AllowPrefix)
	fullExecutor := controller.NewExecutor(cfg.EvictCmd, cfg.BackupCmd, cfg.RestoreCmd, cfg.DbListCmd, fullCustomVars, cfg.DatabasesKey, cfg.DbmapKey, l)

	fullS3Client, err := controller.NewS3Client(ctx, cfg.S3URL, cfg.AccessKeyID, cfg.AccessKeySecret, cfg.BucketName, cfg.Region, cfg.S3SslVerify)
	if err != nil {
		l.Fatalf("could not connect to full s3 client %v", err)
	}

	fullScheduler := controller.NewScheduler(fullStorageRepo, fullExecutor, dbRepo, fullS3Client, cfg.S3Enabled, l, 5,
		cfg.Schedule, cfg.GranularSchedule, cfg.IncrementalSchedule, fullScheduledDBs, fullCustomVars)

	fullDaemon := controller.NewBackupDaemon(fullStorageRepo, dbRepo, fullScheduler, fullS3Client, fullExecutor,
		cfg.S3Enabled, l, cfg.EvictionPolicy, cfg.GranularEvictionPolicy)

	// ── INCREMENTAL processor ────────────────────────────────────────────────
	incrCustomVars := parseCustomVars(incrCfg.CustomVars)
	incrScheduledDBs := parseScheduledDBs(incrCfg.ScheduledDBs)

	incrStorageRepo := repo.NewStorageRepo(incrCfg.StorageRoot, incrCfg.ExternalRoot, incrCfg.Namespace, incrCfg.AllowPrefix)
	incrExecutor := controller.NewExecutor(incrCfg.EvictCmd, incrCfg.BackupCmd, incrCfg.RestoreCmd, incrCfg.DbListCmd, incrCustomVars, incrCfg.DatabasesKey, incrCfg.DbmapKey, l)

	incrS3Client, err := controller.NewS3Client(ctx, incrCfg.S3URL, incrCfg.AccessKeyID, incrCfg.AccessKeySecret, incrCfg.BucketName, incrCfg.Region, incrCfg.S3SslVerify)
	if err != nil {
		l.Fatalf("could not connect to incremental s3 client %v", err)
	}

	incrScheduler := controller.NewScheduler(incrStorageRepo, incrExecutor, dbRepo, incrS3Client, incrCfg.S3Enabled, l, 5,
		incrCfg.Schedule, incrCfg.GranularSchedule, incrCfg.IncrementalSchedule, incrScheduledDBs, incrCustomVars)

	incrDaemon := controller.NewBackupDaemon(incrStorageRepo, dbRepo, incrScheduler, incrS3Client, incrExecutor,
		incrCfg.S3Enabled, l, incrCfg.EvictionPolicy, incrCfg.GranularEvictionPolicy)

	// ── BackupExecutor (router) ──────────────────────────────────────────────
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

func (a *App) gracefulShutdown(cancel context.CancelFunc) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	<-ch
	signal.Stop(ch)
	cancel()
}
