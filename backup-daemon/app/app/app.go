package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/tasks"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/utils"

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

// parseCustomVars converts a slice of raw HOCON list elements into a map.
// HOCON may produce elements in several forms:
//   - plain identifier: `mode`                  → key="mode",  value=""
//   - quoted string:    `"topic_regex"`          → key="topic_regex", value=""
//   - KEY=VALUE pair:   `mode=full`              → key="mode", value="full"
//   - HOCON object:     `{region: "us-east-1"}` → key="region", value="us-east-1"
func parseCustomVars(rawVars []string) map[string]string {
	m := make(map[string]string)
	for _, cv := range rawVars {
		cv = strings.TrimSpace(cv)
		if cv == "" {
			continue
		}
		// HOCON object element: {key: value} or {key: "value"}
		if strings.HasPrefix(cv, "{") && strings.HasSuffix(cv, "}") {
			inner := strings.TrimSpace(cv[1 : len(cv)-1])
			if colonIdx := strings.Index(inner, ":"); colonIdx > 0 {
				key := strings.TrimSpace(inner[:colonIdx])
				key = strings.Trim(key, `"`)
				val := strings.TrimSpace(inner[colonIdx+1:])
				val = strings.Trim(val, `"`)
				if key != "" {
					m[key] = val
				}
			}
			continue
		}
		// KEY=VALUE pair (env-style)
		if idx := strings.Index(cv, "="); idx > 0 {
			key := strings.Trim(strings.TrimSpace(cv[:idx]), `"`)
			val := strings.Trim(strings.TrimSpace(cv[idx+1:]), `"`)
			m[key] = val
			continue
		}
		// Plain identifier or quoted string — strip surrounding quotes
		key := strings.Trim(cv, `"`)
		if key != "" {
			m[key] = ""
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

	ctx, cancel := context.WithCancel(context.Background())

	// Shared DB connection — one connection for both full and incremental processors.
	dbConnections, err := db.NewConnection(cfg.DBPath)
	if err != nil {
		l.Panicf("could not connect to database %v", err)
	}
	defer func() {
		if errDb := dbConnections.Close(); errDb != nil {
			l.Panicf("could not close database %v", err)
		}
	}()

	dbRepo := repo.NewDBRepo(dbConnections)

	// Storage repos must be created before executors because executors own eviction logic.
	fullStorageRepo := repo.NewStorageRepo(cfg.StorageRoot, cfg.ExternalRoot, cfg.Namespace, cfg.AllowPrefix)
	incrStorageRepo := repo.NewStorageRepo(incrCfg.StorageRoot, incrCfg.ExternalRoot, incrCfg.Namespace, incrCfg.AllowPrefix)

	// Build full executor from full config.
	fullCustomVars := parseCustomVars(cfg.CustomVars)
	fullExecutor := tasks.NewExecutor(
		cfg.EvictCmd, cfg.BackupCmd, cfg.RestoreCmd, cfg.DbListCmd,
		fullCustomVars, cfg.DatabasesKey, cfg.DbmapKey,
		fullStorageRepo, dbRepo, cfg.EvictionPolicy, cfg.GranularEvictionPolicy, l,
	)

	// Build incremental executor from incremental config.
	incrCustomVars := parseCustomVars(incrCfg.CustomVars)
	incrExecutor := tasks.NewExecutor(
		incrCfg.EvictCmd, incrCfg.BackupCmd, incrCfg.RestoreCmd, incrCfg.DbListCmd,
		incrCustomVars, incrCfg.DatabasesKey, incrCfg.DbmapKey,
		incrStorageRepo, dbRepo, incrCfg.EvictionPolicy, incrCfg.GranularEvictionPolicy, l,
	)

	// Shared S3 client — both daemons use the same bucket.
	s3Client, err := utils.NewS3Client(ctx, cfg.S3URL, cfg.AccessKeyID, cfg.AccessKeySecret, cfg.BucketName, cfg.Region, cfg.S3SslVerify, cfg.S3CertsPath)
	if err != nil {
		l.Panicf("could not connect to s3 client: %v", err)
	}

	// Single TaskPool with one TaskExecutor that routes by Task.ProcType.
	taskPool := tasks.NewTaskPool(
		ctx, 100,
		fullExecutor, incrExecutor,
		dbRepo, s3Client, cfg.S3Enabled, l,
	)

	// Full storage + daemon.
	fullDaemon := controller.NewBackupDaemon(
		fullStorageRepo, dbRepo, taskPool, s3Client, fullExecutor,
		cfg.S3Enabled, l,
	)

	// Incremental storage + daemon.
	incrDaemon := controller.NewBackupDaemon(
		incrStorageRepo, dbRepo, taskPool, s3Client, incrExecutor,
		incrCfg.S3Enabled, l,
	)

	// Scheduler uses fullDaemon (cron triggers full backups).
	scheduledDBs := parseScheduledDBs(cfg.ScheduledDBs)
	scheduler := controller.NewScheduler(ctx, l, cfg.Schedule, cfg.GranularSchedule, cfg.IncrementalSchedule, scheduledDBs, fullCustomVars)
	scheduler.SetBackupDaemon(fullDaemon)

	serverPort := cfg.Port
	var certPath string
	var keyPath string

	if cfg.TLSEnabled == "true" {
		serverPort = cfg.TLSPort
		base := strings.TrimRight(cfg.CertsPath, "/")
		certPath = fmt.Sprintf("%s/tls.crt", base)
		keyPath = fmt.Sprintf("%s/tls.key", base)
	}

	endpointHandler := rest.NewEndpointHandler(fullDaemon, incrDaemon, l, cfg.CustomVars...)

	router := rest.NewRouter()

	server, err := rest.NewServer(serverPort, cfg.ShutdownTimeout, router, l, endpointHandler, certPath, keyPath)
	if err != nil {
		l.Panicf("failed to create server err: %v", err)
	}

	server.Run()
	defer func() {
		if err = server.Stop(); err != nil {
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
