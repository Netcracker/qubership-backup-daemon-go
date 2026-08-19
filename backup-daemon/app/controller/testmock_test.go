package controller

import (
	"context"
	"reflect"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/tasks"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"go.uber.org/mock/gomock"
)

// --- MockDBRepository ---

type MockDBRepository struct {
	ctrl     *gomock.Controller
	recorder *MockDBRepositoryRecorder
}

type MockDBRepositoryRecorder struct {
	mock *MockDBRepository
}

func NewMockDBRepository(ctrl *gomock.Controller) *MockDBRepository {
	mock := &MockDBRepository{ctrl: ctrl}
	mock.recorder = &MockDBRepositoryRecorder{mock}
	return mock
}

func (m *MockDBRepository) EXPECT() *MockDBRepositoryRecorder { return m.recorder }

func (m *MockDBRepository) UpdateJob(ctx context.Context, job entity.Job) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateJob", ctx, job)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockDBRepositoryRecorder) UpdateJob(ctx, job any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateJob", reflect.TypeOf((*MockDBRepository)(nil).UpdateJob), ctx, job)
}

func (m *MockDBRepository) RemoveJob(ctx context.Context, taskID string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemoveJob", ctx, taskID)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockDBRepositoryRecorder) RemoveJob(ctx, taskID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemoveJob", reflect.TypeOf((*MockDBRepository)(nil).RemoveJob), ctx, taskID)
}

func (m *MockDBRepository) RemoveVault(ctx context.Context, vault string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemoveVault", ctx, vault)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockDBRepositoryRecorder) RemoveVault(ctx, vault any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemoveVault", reflect.TypeOf((*MockDBRepository)(nil).RemoveVault), ctx, vault)
}

func (m *MockDBRepository) SelectEverything(ctx context.Context, taskID string) (entity.Job, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SelectEverything", ctx, taskID)
	ret0, _ := ret[0].(entity.Job)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockDBRepositoryRecorder) SelectEverything(ctx, taskID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SelectEverything", reflect.TypeOf((*MockDBRepository)(nil).SelectEverything), ctx, taskID)
}

func (m *MockDBRepository) ListVaultNames(ctx context.Context) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListVaultNames", ctx)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockDBRepositoryRecorder) ListVaultNames(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListVaultNames", reflect.TypeOf((*MockDBRepository)(nil).ListVaultNames), ctx)
}

// --- MockSchedulerRepository ---

type MockSchedulerRepository struct {
	ctrl     *gomock.Controller
	recorder *MockSchedulerRepositoryRecorder
}

type MockSchedulerRepositoryRecorder struct {
	mock *MockSchedulerRepository
}

func NewMockSchedulerRepository(ctrl *gomock.Controller) *MockSchedulerRepository {
	mock := &MockSchedulerRepository{ctrl: ctrl}
	mock.recorder = &MockSchedulerRepositoryRecorder{mock}
	return mock
}

func (m *MockSchedulerRepository) EXPECT() *MockSchedulerRepositoryRecorder { return m.recorder }

func (m *MockSchedulerRepository) SetBackupDaemon(backupDaemon BackupDaemonUseCase) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetBackupDaemon", backupDaemon)
}

func (mr *MockSchedulerRepositoryRecorder) SetBackupDaemon(backupDaemon any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetBackupDaemon", reflect.TypeOf((*MockSchedulerRepository)(nil).SetBackupDaemon), backupDaemon)
}

// --- MockTaskPoolRepository ---

type MockTaskPoolRepository struct {
	ctrl      *gomock.Controller
	recorder  *MockTaskPoolRepositoryRecorder
	collected []tasks.Task
}

type MockTaskPoolRepositoryRecorder struct {
	mock *MockTaskPoolRepository
}

func NewMockTaskPoolRepository(ctrl *gomock.Controller) *MockTaskPoolRepository {
	mock := &MockTaskPoolRepository{ctrl: ctrl}
	mock.recorder = &MockTaskPoolRepositoryRecorder{mock}
	return mock
}

func (m *MockTaskPoolRepository) EXPECT() *MockTaskPoolRepositoryRecorder { return m.recorder }

func (m *MockTaskPoolRepository) EnqueueTask(task tasks.Task) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "EnqueueTask", task)
	m.collected = append(m.collected, task)
}

func (mr *MockTaskPoolRepositoryRecorder) EnqueueTask(task any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "EnqueueTask", reflect.TypeOf((*MockTaskPoolRepository)(nil).EnqueueTask), task)
}

func (m *MockTaskPoolRepository) QueueSize() int {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueueSize")
	ret0, _ := ret[0].(int)
	return ret0
}

func (mr *MockTaskPoolRepositoryRecorder) QueueSize() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueueSize", reflect.TypeOf((*MockTaskPoolRepository)(nil).QueueSize))
}

// --- MockBackupDaemonUseCase ---

type MockBackupDaemonUseCase struct {
	ctrl     *gomock.Controller
	recorder *MockBackupDaemonUseCaseRecorder
}

type MockBackupDaemonUseCaseRecorder struct {
	mock *MockBackupDaemonUseCase
}

func NewMockBackupDaemonUseCase(ctrl *gomock.Controller) *MockBackupDaemonUseCase {
	mock := &MockBackupDaemonUseCase{ctrl: ctrl}
	mock.recorder = &MockBackupDaemonUseCaseRecorder{mock}
	return mock
}

func (m *MockBackupDaemonUseCase) EXPECT() *MockBackupDaemonUseCaseRecorder { return m.recorder }

func (m *MockBackupDaemonUseCase) EnqueueBackup(ctx context.Context, request entity.BackupRequest) (entity.BackupResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "EnqueueBackup", ctx, request)
	ret0, _ := ret[0].(entity.BackupResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (m *MockBackupDaemonUseCase) GetBackupStats(ctx context.Context, vaultName, ts, backupPath, procType string) (map[string]interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBackupStats", ctx, vaultName, ts, backupPath, procType)
	ret0, _ := ret[0].(map[string]interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockBackupDaemonUseCaseRecorder) GetBackupStats(ctx, vaultName, ts, backupPath, procType any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"GetBackupStats",
		reflect.TypeOf((*MockBackupDaemonUseCase)(nil).GetBackupStats),
		ctx, vaultName, ts, backupPath, procType,
	)
}

func (mr *MockBackupDaemonUseCaseRecorder) EnqueueBackup(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "EnqueueBackup", reflect.TypeOf((*MockBackupDaemonUseCase)(nil).EnqueueBackup), ctx, request)
}

func (m *MockBackupDaemonUseCase) RestoreBackup(ctx context.Context, request entity.RestoreRequest) (entity.RestoreResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RestoreBackup", ctx, request)
	ret0, _ := ret[0].(entity.RestoreResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockBackupDaemonUseCaseRecorder) RestoreBackup(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RestoreBackup", reflect.TypeOf((*MockBackupDaemonUseCase)(nil).RestoreBackup), ctx, request)
}

func (m *MockBackupDaemonUseCase) EnqueueEviction(ctx context.Context, request entity.EvictRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "EnqueueEviction", ctx, request)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockBackupDaemonUseCaseRecorder) EnqueueEviction(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "EnqueueEviction", reflect.TypeOf((*MockBackupDaemonUseCase)(nil).EnqueueEviction), ctx, request)
}

func (m *MockBackupDaemonUseCase) RemoveBackup(ctx context.Context, request entity.EvictByVaultRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemoveBackup", ctx, request)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockBackupDaemonUseCaseRecorder) RemoveBackup(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemoveBackup", reflect.TypeOf((*MockBackupDaemonUseCase)(nil).RemoveBackup), ctx, request)
}

func (m *MockBackupDaemonUseCase) RemoveBackupV2(ctx context.Context, request entity.EvictByVaultV2Request) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemoveBackupV2", ctx, request)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockBackupDaemonUseCaseRecorder) RemoveBackupV2(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemoveBackupV2", reflect.TypeOf((*MockBackupDaemonUseCase)(nil).RemoveBackupV2), ctx, request)
}

func (m *MockBackupDaemonUseCase) RemoveRestoreV2(ctx context.Context, request entity.EvictByVaultV2Request) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemoveRestoreV2", ctx, request)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockBackupDaemonUseCaseRecorder) RemoveRestoreV2(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemoveRestoreV2", reflect.TypeOf((*MockBackupDaemonUseCase)(nil).RemoveRestoreV2), ctx, request)
}

func (m *MockBackupDaemonUseCase) GetJobStatus(ctx context.Context, request entity.JobStatusRequest) (entity.JobStatusResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetJobStatus", ctx, request)
	ret0, _ := ret[0].(entity.JobStatusResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockBackupDaemonUseCaseRecorder) GetJobStatus(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetJobStatus", reflect.TypeOf((*MockBackupDaemonUseCase)(nil).GetJobStatus), ctx, request)
}

func (m *MockBackupDaemonUseCase) CreateS3PresignedURL(ctx context.Context, request entity.S3PresignedURLRequest) (entity.S3PresignedURLResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateS3PresignedURL", ctx, request)
	ret0, _ := ret[0].(entity.S3PresignedURLResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockBackupDaemonUseCaseRecorder) CreateS3PresignedURL(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateS3PresignedURL", reflect.TypeOf((*MockBackupDaemonUseCase)(nil).CreateS3PresignedURL), ctx, request)
}

func (m *MockBackupDaemonUseCase) ListBackups(ctx context.Context, procType string) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListBackups", ctx, procType)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockBackupDaemonUseCaseRecorder) ListBackups(ctx, procType any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"ListBackups",
		reflect.TypeOf((*MockBackupDaemonUseCase)(nil).ListBackups),
		ctx, procType,
	)
}

func (m *MockBackupDaemonUseCase) ListBackup(ctx context.Context, procType string, vaultPath string) (map[string]interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListBackup", ctx, procType, vaultPath)
	ret0, _ := ret[0].(map[string]interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockBackupDaemonUseCaseRecorder) ListBackup(ctx, procType, vaultPath any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"ListBackup",
		reflect.TypeOf((*MockBackupDaemonUseCase)(nil).ListBackup),
		ctx, procType, vaultPath,
	)
}

func (m *MockBackupDaemonUseCase) GetHealth(ctx context.Context, procType string) (entity.HealthResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetHealth", ctx, procType)
	ret0, _ := ret[0].(entity.HealthResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockBackupDaemonUseCaseRecorder) GetHealth(ctx, procType any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetHealth", reflect.TypeOf((*MockBackupDaemonUseCase)(nil).GetHealth), ctx, procType)
}

func (m *MockBackupDaemonUseCase) Ready(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Ready", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockBackupDaemonUseCaseRecorder) Ready(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ready", reflect.TypeOf((*MockBackupDaemonUseCase)(nil).Ready), ctx)
}

func (m *MockBackupDaemonUseCase) Find(ctx context.Context, request entity.FindRequest) (map[string]interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Find", ctx, request)
	ret0, _ := ret[0].(map[string]interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockBackupDaemonUseCaseRecorder) Find(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Find", reflect.TypeOf((*MockBackupDaemonUseCase)(nil).Find), ctx, request)
}

func (m *MockBackupDaemonUseCase) UpdateEvictionPolicy(ctx context.Context, request entity.EvictionPolicyRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateEvictionPolicy", ctx, request)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockBackupDaemonUseCaseRecorder) UpdateEvictionPolicy(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateEvictionPolicy", reflect.TypeOf((*MockBackupDaemonUseCase)(nil).UpdateEvictionPolicy), ctx, request)
}

func (m *MockBackupDaemonUseCase) TerminateBackup(ctx context.Context, request entity.TerminateRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TerminateBackup", ctx, request)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockBackupDaemonUseCaseRecorder) TerminateBackup(ctx, request any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TerminateBackup", reflect.TypeOf((*MockBackupDaemonUseCase)(nil).TerminateBackup), ctx, request)
}

func (m *MockBackupDaemonUseCase) GetQueueSize() int {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetQueueSize")
	ret0, _ := ret[0].(int)
	return ret0
}

func (mr *MockBackupDaemonUseCaseRecorder) GetQueueSize() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetQueueSize", reflect.TypeOf((*MockBackupDaemonUseCase)(nil).GetQueueSize))
}

func (m *MockBackupDaemonUseCase) DownloadBackup(ctx context.Context, backupID string) (string, func(), error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DownloadBackup", ctx, backupID)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(func())
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

func (mr *MockBackupDaemonUseCaseRecorder) DownloadBackup(ctx, backupID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DownloadBackup", reflect.TypeOf((*MockBackupDaemonUseCase)(nil).DownloadBackup), ctx, backupID)
}
