package controller

import (
	"context"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

type SchedulerRepository interface {
	SetBackupDaemon(backupDaemon BackupDaemonUseCase)
}

type Scheduler struct {
	logger              *zap.SugaredLogger
	cron                *cron.Cron
	backupDaemon        BackupDaemonUseCase
	customVars          map[string]string
	scheduledDBs        []string
	schedule            string
	granularSchedule    string
	incrementalSchedule string
}

func NewScheduler(
	logger *zap.SugaredLogger,
	schedule string,
	granularSchedule string,
	incrementalSchedule string,
	scheduledDBs []string,
	customVars map[string]string,
) SchedulerRepository {
	s := &Scheduler{
		logger:              logger,
		cron:                cron.New(),
		customVars:          customVars,
		scheduledDBs:        scheduledDBs,
		schedule:            schedule,
		granularSchedule:    granularSchedule,
		incrementalSchedule: incrementalSchedule,
	}

	s.logger.Info("Scheduler initialized",
		zap.String("schedule", schedule),
		zap.String("granularSchedule", granularSchedule),
		zap.String("incrementalSchedule", incrementalSchedule),
		zap.Strings("scheduledDBs", scheduledDBs))

	if s.schedule != "" {
		if _, err := s.cron.AddFunc(s.schedule, func() { s.enqueueCronBackup(FULL) }); err != nil {
			s.logger.Error("Failed to add full backup cron job", zap.Error(err))
		} else {
			s.logger.Info("Added full backup cron job", zap.String("schedule", s.schedule))
		}
	}
	if s.granularSchedule != "" && len(s.scheduledDBs) > 0 {
		if _, err := s.cron.AddFunc(s.granularSchedule, func() { s.enqueueCronBackup(GRANULAR) }); err != nil {
			s.logger.Error("Failed to add granular backup cron job", zap.Error(err))
		} else {
			s.logger.Info("Added granular backup cron job",
				zap.String("schedule", s.granularSchedule),
				zap.Strings("databases", s.scheduledDBs))
		}
	}
	if s.incrementalSchedule != "" {
		if _, err := s.cron.AddFunc(s.incrementalSchedule, func() { s.enqueueCronBackup(INCREMENTAL) }); err != nil {
			s.logger.Error("Failed to add incremental backup cron job", zap.Error(err))
		} else {
			s.logger.Info("Added incremental backup cron job", zap.String("schedule", s.incrementalSchedule))
		}
	}

	return s
}

func (s *Scheduler) SetBackupDaemon(backupDaemon BackupDaemonUseCase) {
	s.backupDaemon = backupDaemon
	s.cron.Start()
	s.logger.Info("BackupDaemon set for scheduler - cron jobs started")
}

func (s *Scheduler) enqueueCronBackup(jobType string) {
	s.logger.Info("Cron backup triggered", zap.String("jobType", jobType))

	if s.backupDaemon == nil {
		s.logger.Warn("BackupDaemon not set, skipping cron backup")
		return
	}

	ctx := context.Background()
	request := entity.BackupRequest{
		AllowEviction: "true",
		CustomVars:    s.customVars,
	}

	switch jobType {
	case GRANULAR:
		for _, dbName := range s.scheduledDBs {
			request.DBs = append(request.DBs, entity.DBEntry{Name: dbName, SimpleName: dbName})
		}
		s.logger.Info("Enqueuing granular cron backup",
			zap.Strings("databases", s.scheduledDBs),
			zap.Int("dbCount", len(request.DBs)))
	case INCREMENTAL:
		request.ProcType = INCREMENTAL
		s.logger.Info("Enqueuing incremental cron backup")
	default:
		s.logger.Info("Enqueuing full cron backup")
	}

	if _, err := s.backupDaemon.EnqueueBackup(ctx, request); err != nil {
		s.logger.Error("Failed to enqueue cron backup", zap.Error(err), zap.String("jobType", jobType))
	} else {
		s.logger.Info("Successfully enqueued cron backup", zap.String("jobType", jobType))
	}
}
