package controller

import (
	"context"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/tasks"
	"os"
	"reflect"

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

// --- MockStorageRepository ---

type MockStorageRepository struct {
	ctrl     *gomock.Controller
	recorder *MockStorageRepositoryRecorder
}

type MockStorageRepositoryRecorder struct {
	mock *MockStorageRepository
}

func NewMockStorageRepository(ctrl *gomock.Controller) *MockStorageRepository {
	mock := &MockStorageRepository{ctrl: ctrl}
	mock.recorder = &MockStorageRepositoryRecorder{mock}
	return mock
}

func (m *MockStorageRepository) EXPECT() *MockStorageRepositoryRecorder { return m.recorder }

func (m *MockStorageRepository) GetVault(vaultName string, external bool, vaultPath string, blobPath string, skipFSCheck bool) entity.Vault {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetVault", vaultName, external, vaultPath, blobPath, skipFSCheck)
	ret0, _ := ret[0].(entity.Vault)
	return ret0
}

func (mr *MockStorageRepositoryRecorder) GetVault(vaultName, external, vaultPath, blobPath, skipFSCheck any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetVault", reflect.TypeOf((*MockStorageRepository)(nil).GetVault), vaultName, external, vaultPath, blobPath, skipFSCheck)
}

func (m *MockStorageRepository) FindByTS(timestamp string, typeOfBackup string, storagePath string) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FindByTS", timestamp, typeOfBackup, storagePath)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockStorageRepositoryRecorder) FindByTS(timestamp, typeOfBackup, storagePath any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FindByTS", reflect.TypeOf((*MockStorageRepository)(nil).FindByTS), timestamp, typeOfBackup, storagePath)
}

func (m *MockStorageRepository) OpenVault(vaultName string, allowEviction bool, isGranular bool, isSharded bool, isExternal bool, vaultPath string, backupPrefix string, blobPath string) entity.Vault {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OpenVault", vaultName, allowEviction, isGranular, isSharded, isExternal, vaultPath, backupPrefix, blobPath)
	ret0, _ := ret[0].(entity.Vault)
	return ret0
}

func (mr *MockStorageRepositoryRecorder) OpenVault(vaultName, allowEviction, isGranular, isSharded, isExternal, vaultPath, backupPrefix, blobPath any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OpenVault", reflect.TypeOf((*MockStorageRepository)(nil).OpenVault), vaultName, allowEviction, isGranular, isSharded, isExternal, vaultPath, backupPrefix, blobPath)
}

func (m *MockStorageRepository) Evict(vaultName string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Evict", vaultName)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockStorageRepositoryRecorder) Evict(vaultName any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Evict", reflect.TypeOf((*MockStorageRepository)(nil).Evict), vaultName)
}

func (m *MockStorageRepository) ProtGetAsStream(backupID string, archiveFile string) (*os.File, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ProtGetAsStream", backupID, archiveFile)
	ret0, _ := ret[0].(*os.File)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockStorageRepositoryRecorder) ProtGetAsStream(backupID, archiveFile any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ProtGetAsStream", reflect.TypeOf((*MockStorageRepository)(nil).ProtGetAsStream), backupID, archiveFile)
}

func (m *MockStorageRepository) List(typeOfBackup string, storagePath string) ([]entity.Vault, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "List", typeOfBackup, storagePath)
	ret0, _ := ret[0].([]entity.Vault)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockStorageRepositoryRecorder) List(typeOfBackup, storagePath any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "List", reflect.TypeOf((*MockStorageRepository)(nil).List), typeOfBackup, storagePath)
}

func (m *MockStorageRepository) ListVaultNames(convertToTs bool, typeOfBackup string, storagePath string) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListVaultNames", convertToTs, typeOfBackup, storagePath)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockStorageRepositoryRecorder) ListVaultNames(convertToTs, typeOfBackup, storagePath any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListVaultNames", reflect.TypeOf((*MockStorageRepository)(nil).ListVaultNames), convertToTs, typeOfBackup, storagePath)
}

func (m *MockStorageRepository) GetNonEvictableVaults(typeOfBackup string) (map[int64]bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNonEvictableVaults", typeOfBackup)
	ret0, _ := ret[0].(map[int64]bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockStorageRepositoryRecorder) GetNonEvictableVaults(typeOfBackup any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNonEvictableVaults", reflect.TypeOf((*MockStorageRepository)(nil).GetNonEvictableVaults), typeOfBackup)
}

func (m *MockStorageRepository) GetName(folder string) string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetName", folder)
	ret0, _ := ret[0].(string)
	return ret0
}

func (mr *MockStorageRepositoryRecorder) GetName(folder any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetName", reflect.TypeOf((*MockStorageRepository)(nil).GetName), folder)
}

// --- MockCommandExecutor ---

type MockCommandExecutor struct {
	ctrl     *gomock.Controller
	recorder *MockCommandExecutorRecorder
}

type MockCommandExecutorRecorder struct {
	mock *MockCommandExecutor
}

func NewMockCommandExecutor(ctrl *gomock.Controller) *MockCommandExecutor {
	mock := &MockCommandExecutor{ctrl: ctrl}
	mock.recorder = &MockCommandExecutorRecorder{mock}
	return mock
}

func (m *MockCommandExecutor) EXPECT() *MockCommandExecutorRecorder { return m.recorder }

func (m *MockCommandExecutor) ExecuteEvictCmd(vaultFolder string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ExecuteEvictCmd", vaultFolder)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockCommandExecutorRecorder) ExecuteEvictCmd(vaultFolder any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecuteEvictCmd", reflect.TypeOf((*MockCommandExecutor)(nil).ExecuteEvictCmd), vaultFolder)
}

func (m *MockCommandExecutor) PerformBackup(vault entity.Vault, dbs []entity.DBEntry, customVars map[string]string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PerformBackup", vault, dbs, customVars)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockCommandExecutorRecorder) PerformBackup(vault, dbs, customVars any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PerformBackup", reflect.TypeOf((*MockCommandExecutor)(nil).PerformBackup), vault, dbs, customVars)
}

func (m *MockCommandExecutor) PerformRestore(vaultFolder string, dbs []entity.DBEntry, dbmap map[string]string, customVariables map[string]string, external bool, taskID string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PerformRestore", vaultFolder, dbs, dbmap, customVariables, external, taskID)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockCommandExecutorRecorder) PerformRestore(vaultFolder, dbs, dbmap, customVariables, external, taskID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PerformRestore", reflect.TypeOf((*MockCommandExecutor)(nil).PerformRestore), vaultFolder, dbs, dbmap, customVariables, external, taskID)
}

func (m *MockCommandExecutor) GetBackupDBs(vaultFolder string) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBackupDBs", vaultFolder)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockCommandExecutorRecorder) GetBackupDBs(vaultFolder any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBackupDBs", reflect.TypeOf((*MockCommandExecutor)(nil).GetBackupDBs), vaultFolder)
}

// --- MockSchedulerRepository ---

type MockSchedulerRepository struct {
	ctrl     *gomock.Controller
	recorder *MockSchedulerRepositoryRecorder
	tasks    []tasks.Task
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

func (m *MockSchedulerRepository) EnqueueTask(task tasks.Task) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "EnqueueTask", task)
	m.tasks = append(m.tasks, task)
}

func (mr *MockSchedulerRepositoryRecorder) EnqueueTask(task any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "EnqueueTask", reflect.TypeOf((*MockSchedulerRepository)(nil).EnqueueTask), task)
}

func (m *MockSchedulerRepository) SetBackupDaemon(backupDaemon BackupDaemonUseCase) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetBackupDaemon", backupDaemon)
}

func (mr *MockSchedulerRepositoryRecorder) SetBackupDaemon(backupDaemon any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetBackupDaemon", reflect.TypeOf((*MockSchedulerRepository)(nil).SetBackupDaemon), backupDaemon)
}

func (m *MockSchedulerRepository) QueueSize() int {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueueSize")
	ret0, _ := ret[0].(int)
	return ret0
}

func (mr *MockSchedulerRepositoryRecorder) QueueSize() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueueSize", reflect.TypeOf((*MockSchedulerRepository)(nil).QueueSize))
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

func (m *MockBackupDaemonUseCase) DownloadBackup(ctx context.Context, backupID string) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DownloadBackup", ctx, backupID)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (mr *MockBackupDaemonUseCaseRecorder) DownloadBackup(ctx, backupID any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DownloadBackup", reflect.TypeOf((*MockBackupDaemonUseCase)(nil).DownloadBackup), ctx, backupID)
}
