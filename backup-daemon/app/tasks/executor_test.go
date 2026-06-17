package tasks

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/repo"
	"go.uber.org/zap"
)

func TestExecutor_ExecuteEvictCmd(t *testing.T) {
	type fields struct {
		evictCmdTemplate       string
		backupCmdTemplate      string
		restoreCmdTemplate     string
		dbListCmdTemplate      string
		customVars             map[string]string
		databasesKey           string
		dbmapKey               string
		storageRepo            repo.StorageRepository
		dbRepo                 repo.DBRepository
		evictionPolicy         string
		granularEvictionPolicy string
		evictionMu             *sync.RWMutex
		logger                 *zap.SugaredLogger
	}
	type args struct {
		vaultFolder string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Executor{
				evictCmdTemplate:       tt.fields.evictCmdTemplate,
				backupCmdTemplate:      tt.fields.backupCmdTemplate,
				restoreCmdTemplate:     tt.fields.restoreCmdTemplate,
				dbListCmdTemplate:      tt.fields.dbListCmdTemplate,
				customVars:             tt.fields.customVars,
				databasesKey:           tt.fields.databasesKey,
				dbmapKey:               tt.fields.dbmapKey,
				storageRepo:            tt.fields.storageRepo,
				dbRepo:                 tt.fields.dbRepo,
				evictionPolicy:         tt.fields.evictionPolicy,
				granularEvictionPolicy: tt.fields.granularEvictionPolicy,
				evictionMu:             tt.fields.evictionMu,
				logger:                 tt.fields.logger,
			}
			if err := e.ExecuteEvictCmd(tt.args.vaultFolder); (err != nil) != tt.wantErr {
				t.Errorf("ExecuteEvictCmd() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExecutor_ExecuteTerminationCmd(t *testing.T) {
	type fields struct {
		evictCmdTemplate       string
		backupCmdTemplate      string
		restoreCmdTemplate     string
		dbListCmdTemplate      string
		customVars             map[string]string
		databasesKey           string
		dbmapKey               string
		storageRepo            repo.StorageRepository
		dbRepo                 repo.DBRepository
		evictionPolicy         string
		granularEvictionPolicy string
		evictionMu             *sync.RWMutex
		logger                 *zap.SugaredLogger
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Executor{
				evictCmdTemplate:       tt.fields.evictCmdTemplate,
				backupCmdTemplate:      tt.fields.backupCmdTemplate,
				restoreCmdTemplate:     tt.fields.restoreCmdTemplate,
				dbListCmdTemplate:      tt.fields.dbListCmdTemplate,
				customVars:             tt.fields.customVars,
				databasesKey:           tt.fields.databasesKey,
				dbmapKey:               tt.fields.dbmapKey,
				storageRepo:            tt.fields.storageRepo,
				dbRepo:                 tt.fields.dbRepo,
				evictionPolicy:         tt.fields.evictionPolicy,
				granularEvictionPolicy: tt.fields.granularEvictionPolicy,
				evictionMu:             tt.fields.evictionMu,
				logger:                 tt.fields.logger,
			}
			e.ExecuteTerminationCmd()
		})
	}
}

func TestExecutor_GetBackupDBs(t *testing.T) {
	type fields struct {
		evictCmdTemplate       string
		backupCmdTemplate      string
		restoreCmdTemplate     string
		dbListCmdTemplate      string
		customVars             map[string]string
		databasesKey           string
		dbmapKey               string
		storageRepo            repo.StorageRepository
		dbRepo                 repo.DBRepository
		evictionPolicy         string
		granularEvictionPolicy string
		evictionMu             *sync.RWMutex
		logger                 *zap.SugaredLogger
	}
	type args struct {
		vaultFolder string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Executor{
				evictCmdTemplate:       tt.fields.evictCmdTemplate,
				backupCmdTemplate:      tt.fields.backupCmdTemplate,
				restoreCmdTemplate:     tt.fields.restoreCmdTemplate,
				dbListCmdTemplate:      tt.fields.dbListCmdTemplate,
				customVars:             tt.fields.customVars,
				databasesKey:           tt.fields.databasesKey,
				dbmapKey:               tt.fields.dbmapKey,
				storageRepo:            tt.fields.storageRepo,
				dbRepo:                 tt.fields.dbRepo,
				evictionPolicy:         tt.fields.evictionPolicy,
				granularEvictionPolicy: tt.fields.granularEvictionPolicy,
				evictionMu:             tt.fields.evictionMu,
				logger:                 tt.fields.logger,
			}
			got, err := e.GetBackupDBs(tt.args.vaultFolder)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetBackupDBs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetBackupDBs() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExecutor_PerformBackup(t *testing.T) {
	type fields struct {
		evictCmdTemplate       string
		backupCmdTemplate      string
		restoreCmdTemplate     string
		dbListCmdTemplate      string
		customVars             map[string]string
		databasesKey           string
		dbmapKey               string
		storageRepo            repo.StorageRepository
		dbRepo                 repo.DBRepository
		evictionPolicy         string
		granularEvictionPolicy string
		evictionMu             *sync.RWMutex
		logger                 *zap.SugaredLogger
	}
	type args struct {
		vault      entity.Vault
		dbs        []entity.DBEntry
		customVars map[string]string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Executor{
				evictCmdTemplate:       tt.fields.evictCmdTemplate,
				backupCmdTemplate:      tt.fields.backupCmdTemplate,
				restoreCmdTemplate:     tt.fields.restoreCmdTemplate,
				dbListCmdTemplate:      tt.fields.dbListCmdTemplate,
				customVars:             tt.fields.customVars,
				databasesKey:           tt.fields.databasesKey,
				dbmapKey:               tt.fields.dbmapKey,
				storageRepo:            tt.fields.storageRepo,
				dbRepo:                 tt.fields.dbRepo,
				evictionPolicy:         tt.fields.evictionPolicy,
				granularEvictionPolicy: tt.fields.granularEvictionPolicy,
				evictionMu:             tt.fields.evictionMu,
				logger:                 tt.fields.logger,
			}
			if err := e.PerformBackup(tt.args.vault, tt.args.dbs, tt.args.customVars); (err != nil) != tt.wantErr {
				t.Errorf("PerformBackup() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExecutor_PerformEviction(t *testing.T) {
	type fields struct {
		evictCmdTemplate       string
		backupCmdTemplate      string
		restoreCmdTemplate     string
		dbListCmdTemplate      string
		customVars             map[string]string
		databasesKey           string
		dbmapKey               string
		storageRepo            repo.StorageRepository
		dbRepo                 repo.DBRepository
		evictionPolicy         string
		granularEvictionPolicy string
		evictionMu             *sync.RWMutex
		logger                 *zap.SugaredLogger
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Executor{
				evictCmdTemplate:       tt.fields.evictCmdTemplate,
				backupCmdTemplate:      tt.fields.backupCmdTemplate,
				restoreCmdTemplate:     tt.fields.restoreCmdTemplate,
				dbListCmdTemplate:      tt.fields.dbListCmdTemplate,
				customVars:             tt.fields.customVars,
				databasesKey:           tt.fields.databasesKey,
				dbmapKey:               tt.fields.dbmapKey,
				storageRepo:            tt.fields.storageRepo,
				dbRepo:                 tt.fields.dbRepo,
				evictionPolicy:         tt.fields.evictionPolicy,
				granularEvictionPolicy: tt.fields.granularEvictionPolicy,
				evictionMu:             tt.fields.evictionMu,
				logger:                 tt.fields.logger,
			}
			if err := e.PerformEviction(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("PerformEviction() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExecutor_PerformRestore(t *testing.T) {
	type fields struct {
		evictCmdTemplate       string
		backupCmdTemplate      string
		restoreCmdTemplate     string
		dbListCmdTemplate      string
		customVars             map[string]string
		databasesKey           string
		dbmapKey               string
		storageRepo            repo.StorageRepository
		dbRepo                 repo.DBRepository
		evictionPolicy         string
		granularEvictionPolicy string
		evictionMu             *sync.RWMutex
		logger                 *zap.SugaredLogger
	}
	type args struct {
		vaultFolder     string
		dbs             []entity.DBEntry
		dbmap           map[string]string
		customVariables map[string]string
		external        bool
		taskID          string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Executor{
				evictCmdTemplate:       tt.fields.evictCmdTemplate,
				backupCmdTemplate:      tt.fields.backupCmdTemplate,
				restoreCmdTemplate:     tt.fields.restoreCmdTemplate,
				dbListCmdTemplate:      tt.fields.dbListCmdTemplate,
				customVars:             tt.fields.customVars,
				databasesKey:           tt.fields.databasesKey,
				dbmapKey:               tt.fields.dbmapKey,
				storageRepo:            tt.fields.storageRepo,
				dbRepo:                 tt.fields.dbRepo,
				evictionPolicy:         tt.fields.evictionPolicy,
				granularEvictionPolicy: tt.fields.granularEvictionPolicy,
				evictionMu:             tt.fields.evictionMu,
				logger:                 tt.fields.logger,
			}
			if err := e.PerformRestore(tt.args.vaultFolder, tt.args.dbs, tt.args.dbmap, tt.args.customVariables, tt.args.external, tt.args.taskID); (err != nil) != tt.wantErr {
				t.Errorf("PerformRestore() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestExecutor_SetEvictionPolicy(t *testing.T) {
	type fields struct {
		evictCmdTemplate       string
		backupCmdTemplate      string
		restoreCmdTemplate     string
		dbListCmdTemplate      string
		customVars             map[string]string
		databasesKey           string
		dbmapKey               string
		storageRepo            repo.StorageRepository
		dbRepo                 repo.DBRepository
		evictionPolicy         string
		granularEvictionPolicy string
		evictionMu             *sync.RWMutex
		logger                 *zap.SugaredLogger
	}
	type args struct {
		full     string
		granular string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Executor{
				evictCmdTemplate:       tt.fields.evictCmdTemplate,
				backupCmdTemplate:      tt.fields.backupCmdTemplate,
				restoreCmdTemplate:     tt.fields.restoreCmdTemplate,
				dbListCmdTemplate:      tt.fields.dbListCmdTemplate,
				customVars:             tt.fields.customVars,
				databasesKey:           tt.fields.databasesKey,
				dbmapKey:               tt.fields.dbmapKey,
				storageRepo:            tt.fields.storageRepo,
				dbRepo:                 tt.fields.dbRepo,
				evictionPolicy:         tt.fields.evictionPolicy,
				granularEvictionPolicy: tt.fields.granularEvictionPolicy,
				evictionMu:             tt.fields.evictionMu,
				logger:                 tt.fields.logger,
			}
			err := e.SetEvictionPolicy(tt.args.full, tt.args.granular)
			if err != nil {
				t.Errorf("SetEvictionPolicy() error = %v", err)
			}
		})
	}
}

func vaultAt(folder string, ageSeconds int64) entity.Vault {
	ts := time.Now().UnixMilli() - ageSeconds
	return entity.Vault{
		Folder:    folder,
		TimeStamp: ts,
	}
}

func TestExecutor_evict(t *testing.T) {
	// policy "1h/1d,7d/delete":
	//   rule 1 — items older than 1 h are grouped by 1-day buckets; all but the
	//             newest in each bucket become obsolete.
	//   rule 2 — items older than 7 d are unconditionally deleted.

	oneHour := int64(3600)
	oneDay := int64(86400)

	type fields struct {
		evictCmdTemplate       string
		backupCmdTemplate      string
		restoreCmdTemplate     string
		dbListCmdTemplate      string
		customVars             map[string]string
		databasesKey           string
		dbmapKey               string
		storageRepo            repo.StorageRepository
		dbRepo                 repo.DBRepository
		evictionPolicy         string
		granularEvictionPolicy string
		evictionMu             *sync.RWMutex
		logger                 *zap.SugaredLogger
	}
	type args struct {
		items   []entity.Vault
		rules   string
		exclude map[int64]bool
	}

	// Helper that builds a minimal *zap.SugaredLogger (discard output).
	logger := zap.NewNop().Sugar()

	recentVault1 := vaultAt("20060102T150405", (oneHour/2)*1000)         // 30 min ago
	recentVault2 := vaultAt("20060103T150405", (oneHour*3/4)*1000)       // 45 min ago
	day2Newer := vaultAt("20060104T120000", (2*oneDay)*1000)             // 2 d ago  (newer in group)
	day2Older := vaultAt("20060104T100000", (2*oneDay+2*oneHour)*1000)   // 2 d + 2 h ago (older in same bucket)
	day4Vault := vaultAt("20060106T150405", (4*oneDay)*1000)             // 4 d ago  (sole member of its bucket)
	old8d := vaultAt("20060108T150405", (8*oneDay)*1000)                 // 8 d ago  → rule 2 deletes
	old10d := vaultAt("20060110T150405", (10*oneDay)*1000)               // 10 d ago → rule 2 deletes
	old8dExcluded := vaultAt("20060109T150405", (8*oneDay+oneHour)*1000) // 8 d + 1 h ago, excluded

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []entity.Vault
		wantErr bool
	}{
		{
			name:   "empty input returns empty slice",
			fields: fields{logger: logger},
			args: args{
				items:   []entity.Vault{},
				rules:   "1h/1d,7d/delete",
				exclude: nil,
			},
			want:    []entity.Vault{},
			wantErr: false,
		},
		{
			name:   "all vaults younger than 1h — nothing evicted",
			fields: fields{logger: logger},
			args: args{
				items:   []entity.Vault{recentVault1, recentVault2},
				rules:   "1h/1d,7d/delete",
				exclude: nil,
			},
			want:    nil,
			wantErr: false,
		},
		{
			name:   "one vault per day bucket (1h–7d) — nothing evicted",
			fields: fields{logger: logger},
			args: args{
				items:   []entity.Vault{day2Newer, day4Vault},
				rules:   "1h/1d,7d/delete",
				exclude: nil,
			},
			want:    nil,
			wantErr: false,
		},
		{
			name:   "two vaults same day bucket — older one evicted",
			fields: fields{logger: logger},
			args: args{
				items:   []entity.Vault{day2Newer, day2Older},
				rules:   "1h/1d,7d/delete",
				exclude: nil,
			},
			want:    []entity.Vault{day2Older},
			wantErr: false,
		},
		{
			name:   "vaults older than 7d — all deleted by rule 2",
			fields: fields{logger: logger},
			args: args{
				items:   []entity.Vault{old8d, old10d},
				rules:   "1h/1d,7d/delete",
				exclude: nil,
			},
			want:    []entity.Vault{old8d, old10d},
			wantErr: false,
		},
		{
			name:   "excluded vault not evicted even if older than 7d",
			fields: fields{logger: logger},
			args: args{
				items:   []entity.Vault{old8dExcluded, old10d},
				rules:   "1h/1d,7d/delete",
				exclude: map[int64]bool{old8dExcluded.TimeStamp: true},
			},
			want:    []entity.Vault{old10d},
			wantErr: false,
		},
		{
			name:   "invalid rule returns error",
			fields: fields{logger: logger},
			args: args{
				items:   []entity.Vault{day2Newer},
				rules:   "badpolicy",
				exclude: nil,
			},
			want:    nil,
			wantErr: true,
		},
		// "0/1d,7d/delete": "0" means "all vaults" (start from 0ms ago).
		// A brand-new vault must NOT be deleted.
		{
			name:   "0/1d,7d/delete — fresh vault is NOT evicted",
			fields: fields{logger: logger},
			args: args{
				items:   []entity.Vault{vaultAt("20260101T000000", 1000)}, // 1 second old
				rules:   "0/1d,7d/delete",
				exclude: nil,
			},
			want:    nil, // nothing obsolete
			wantErr: false,
		},
		{
			name:   "0/1d,7d/delete — two vaults same day, older one evicted",
			fields: fields{logger: logger},
			args: args{
				items:   []entity.Vault{day2Newer, day2Older},
				rules:   "0/1d,7d/delete",
				exclude: nil,
			},
			want:    []entity.Vault{day2Older},
			wantErr: false,
		},
		{
			name:   "0/1d,7d/delete — vault older than 7d is deleted",
			fields: fields{logger: logger},
			args: args{
				items:   []entity.Vault{old8d},
				rules:   "0/1d,7d/delete",
				exclude: nil,
			},
			want:    []entity.Vault{old8d},
			wantErr: false,
		},
		{
			name:   "0/1d,7d/delete — one vault per day within 7d, nothing evicted",
			fields: fields{logger: logger},
			args: args{
				items:   []entity.Vault{day2Newer, day4Vault},
				rules:   "0/1d,7d/delete",
				exclude: nil,
			},
			want:    nil,
			wantErr: false,
		},
		// count-based: "3/delete" keeps 3 newest, all others are obsolete.
		{
			name:   "3/delete — 2 vaults total, nothing evicted (below limit)",
			fields: fields{logger: logger},
			args: args{
				items:   []entity.Vault{recentVault1, recentVault2},
				rules:   "3/delete",
				exclude: nil,
			},
			want:    nil,
			wantErr: false,
		},
		{
			name:   "3/delete — 5 vaults, 2 oldest are evicted",
			fields: fields{logger: logger},
			args: args{
				items:   []entity.Vault{recentVault1, recentVault2, day2Newer, old8d, old10d},
				rules:   "3/delete",
				exclude: nil,
			},
			// evict() returns unique deduplicated slice; two oldest after sorting
			want:    []entity.Vault{old8d, old10d},
			wantErr: false,
		},
		{
			name:   "3/delete — excluded vault still not evicted even when below limit position",
			fields: fields{logger: logger},
			args: args{
				items:   []entity.Vault{recentVault1, recentVault2, day2Newer, old8dExcluded, old10d},
				rules:   "3/delete",
				exclude: map[int64]bool{old8dExcluded.TimeStamp: true},
			},
			// old8dExcluded is protected; only old10d is evicted
			want:    []entity.Vault{old10d},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Executor{
				evictCmdTemplate:       tt.fields.evictCmdTemplate,
				backupCmdTemplate:      tt.fields.backupCmdTemplate,
				restoreCmdTemplate:     tt.fields.restoreCmdTemplate,
				dbListCmdTemplate:      tt.fields.dbListCmdTemplate,
				customVars:             tt.fields.customVars,
				databasesKey:           tt.fields.databasesKey,
				dbmapKey:               tt.fields.dbmapKey,
				storageRepo:            tt.fields.storageRepo,
				dbRepo:                 tt.fields.dbRepo,
				evictionPolicy:         tt.fields.evictionPolicy,
				granularEvictionPolicy: tt.fields.granularEvictionPolicy,
				evictionMu:             tt.fields.evictionMu,
				logger:                 tt.fields.logger,
			}
			parsedRules, err := parseRules(tt.args.rules)
			if err != nil {
				t.Logf("parseRules() error = %v", err)
			}
			got, err := e.evict(tt.args.items, parsedRules, tt.args.exclude)
			if (err != nil) != tt.wantErr {
				t.Errorf("evict() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("evict() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExecutor_processCmd(t *testing.T) {
	type fields struct {
		evictCmdTemplate       string
		backupCmdTemplate      string
		restoreCmdTemplate     string
		dbListCmdTemplate      string
		customVars             map[string]string
		databasesKey           string
		dbmapKey               string
		storageRepo            repo.StorageRepository
		dbRepo                 repo.DBRepository
		evictionPolicy         string
		granularEvictionPolicy string
		evictionMu             *sync.RWMutex
		logger                 *zap.SugaredLogger
	}
	type args struct {
		cmdTemplate     string
		vaultFolder     string
		dbs             []entity.DBEntry
		dbmap           map[string]string
		customVariables map[string]string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Executor{
				evictCmdTemplate:       tt.fields.evictCmdTemplate,
				backupCmdTemplate:      tt.fields.backupCmdTemplate,
				restoreCmdTemplate:     tt.fields.restoreCmdTemplate,
				dbListCmdTemplate:      tt.fields.dbListCmdTemplate,
				customVars:             tt.fields.customVars,
				databasesKey:           tt.fields.databasesKey,
				dbmapKey:               tt.fields.dbmapKey,
				storageRepo:            tt.fields.storageRepo,
				dbRepo:                 tt.fields.dbRepo,
				evictionPolicy:         tt.fields.evictionPolicy,
				granularEvictionPolicy: tt.fields.granularEvictionPolicy,
				evictionMu:             tt.fields.evictionMu,
				logger:                 tt.fields.logger,
			}
			got, err := e.processCmd(tt.args.cmdTemplate, tt.args.vaultFolder, tt.args.dbs, tt.args.dbmap, tt.args.customVariables)
			if (err != nil) != tt.wantErr {
				t.Errorf("processCmd() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("processCmd() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewExecutor(t *testing.T) {
	type args struct {
		evictCmdTemplate       string
		backupCmdTemplate      string
		restoreCmdTemplate     string
		dbListCmdTemplate      string
		customVars             map[string]string
		databasesKey           string
		dbmapKey               string
		storageRepo            repo.StorageRepository
		dbRepo                 repo.DBRepository
		evictionPolicy         string
		granularEvictionPolicy string
		logger                 *zap.SugaredLogger
	}
	tests := []struct {
		name string
		args args
		want CommandExecutor
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := NewExecutor(tt.args.evictCmdTemplate, tt.args.backupCmdTemplate, tt.args.restoreCmdTemplate, tt.args.dbListCmdTemplate, tt.args.customVars, tt.args.databasesKey, tt.args.dbmapKey, tt.args.storageRepo, tt.args.dbRepo, tt.args.evictionPolicy, tt.args.granularEvictionPolicy, tt.args.logger, "", ""); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewExecutor() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_dirSize(t *testing.T) {
	type args struct {
		root string
	}
	tests := []struct {
		name    string
		args    args
		want    int64
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := dirSize(tt.args.root)
			if (err != nil) != tt.wantErr {
				t.Errorf("dirSize() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("dirSize() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_uniqueVaults(t *testing.T) {
	type args struct {
		arr []entity.Vault
	}
	tests := []struct {
		name string
		args args
		want []entity.Vault
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := uniqueVaults(tt.args.arr); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("uniqueVaults() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_vaultNames(t *testing.T) {
	type args struct {
		vaults []entity.Vault
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := vaultNames(tt.args.vaults); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("vaultNames() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestEviction_hourlyBuckets verifies "0/1h,4h/1d" with 48 hourly vaults:
// rule 1 keeps newest per 1h bucket, rule 2 deletes all >4h into 1d buckets.
// Expected ~6 survivors; allow 5–7 due to wall-clock bucket alignment.
func TestEviction_hourlyBuckets(t *testing.T) {
	e := &Executor{logger: zap.NewNop().Sugar()}
	rules, err := parseRules("0/1h,4h/1d")
	if err != nil {
		t.Fatalf("parseRules: %v", err)
	}

	const n = 48
	oneHourMs := int64(60 * 60 * 1000)
	items := make([]entity.Vault, n)
	for i := 0; i < n; i++ {
		ageMs := int64(n-i) * oneHourMs
		items[i] = vaultAt("v", ageMs)
		items[i].Folder = "vault" + string(rune('A'+i%26))
	}

	obsolete, err := e.evict(items, rules, nil)
	if err != nil {
		t.Fatalf("evict: %v", err)
	}

	survivors := n - len(obsolete)
	if survivors < 5 || survivors > 7 {
		t.Errorf("expected ~6 survivors, got %d (obsolete=%d)", survivors, len(obsolete))
	}
}

// TestEviction_countBased_keepN verifies "N/delete" keeps exactly N newest vaults.
func TestEviction_countBased_keepN(t *testing.T) {
	e := &Executor{logger: zap.NewNop().Sugar()}

	tests := []struct {
		name      string
		policy    string
		total     int
		wantKeep  int
	}{
		{"keep 3 of 5", "3/delete", 5, 3},
		{"keep 5 of 5", "5/delete", 5, 5},
		{"keep 5 of 3 (below limit)", "5/delete", 3, 3},
		{"keep 1 of 4", "1/delete", 4, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rules, err := parseRules(tt.policy)
			if err != nil {
				t.Fatalf("parseRules(%q): %v", tt.policy, err)
			}
			items := make([]entity.Vault, tt.total)
			for i := 0; i < tt.total; i++ {
				items[i] = vaultAt("vault", int64((i+1)*10*1000))
				items[i].Folder = "folder" + string(rune('A'+i))
			}
			obsolete, err := e.evict(items, rules, nil)
			if err != nil {
				t.Fatalf("evict: %v", err)
			}
			got := tt.total - len(obsolete)
			if got != tt.wantKeep {
				t.Errorf("survivors = %d, want %d (obsolete=%d)", got, tt.wantKeep, len(obsolete))
			}
		})
	}
}
