package tasks

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/entity"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/repo"
	"github.com/Netcracker/qubership-backup-daemon-go/backup-daemon/app/utils"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func makeVaults(n int, ageBase time.Duration, step time.Duration) []entity.Vault {
	vaults := make([]entity.Vault, n)
	now := time.Now()
	for i := range n {
		age := ageBase + time.Duration(i)*step
		ts := now.Add(-age).UnixMilli()
		vaults[i] = entity.Vault{
			Folder:    fmt.Sprintf("/backup-storage/2026%04dT120000", i+1),
			TimeStamp: ts,
		}
	}
	return vaults
}

func nopExecutor(logger *zap.SugaredLogger) *Executor {
	return &Executor{
		evictionPolicy:         "1h/1d,7d/delete",
		granularEvictionPolicy: "7d/delete",
		logger:                 logger,
		storageRepo:            nil, // not needed for evict()
		dbRepo:                 nil,
		rules:                  make(map[string][]Rule),
	}
}

// ---------------------------------------------------------------------------
// BenchmarkEvict — core eviction calculation loop
// ---------------------------------------------------------------------------

// recent: all vaults < 1h old → rule 1 & 2 skip everything — O(n) filter only.
func BenchmarkEvict_AllRecent_100(b *testing.B)  { benchEvict(b, 100, 30*time.Minute, time.Minute) }
func BenchmarkEvict_AllRecent_1000(b *testing.B) { benchEvict(b, 1000, 30*time.Minute, time.Second) }

// mixed: vaults from 2h → 6d ago — rule 1 groups, some evicted.
func BenchmarkEvict_Mixed_10(b *testing.B)   { benchEvict(b, 10, 2*time.Hour, 12*time.Hour) }
func BenchmarkEvict_Mixed_100(b *testing.B)  { benchEvict(b, 100, 2*time.Hour, 2*time.Hour) }
func BenchmarkEvict_Mixed_1000(b *testing.B) { benchEvict(b, 1000, 2*time.Hour, 15*time.Minute) }

// old: all vaults > 7d old → rule 2 deletes all — maximum obsolete slice.
func BenchmarkEvict_AllOld_100(b *testing.B)  { benchEvict(b, 100, 8*24*time.Hour, time.Hour) }
func BenchmarkEvict_AllOld_1000(b *testing.B) { benchEvict(b, 1000, 8*24*time.Hour, time.Minute) }

func benchEvict(b *testing.B, n int, ageBase, step time.Duration) {
	b.Helper()
	logger := zap.NewNop().Sugar()
	e := nopExecutor(logger)
	items := makeVaults(n, ageBase, step)
	rules := "1h/1d,7d/delete"
	parsedRules, err := parseRules(rules)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = e.evict(items, parsedRules, nil)
	}
}

// BenchmarkEvict_WithExclusions — exclude map lookup per vault.
func BenchmarkEvict_WithExclusions_100(b *testing.B) {
	logger := zap.NewNop().Sugar()
	e := nopExecutor(logger)
	items := makeVaults(100, 2*time.Hour, 2*time.Hour)
	exclude := make(map[int64]bool, 20)
	for i := 0; i < 20; i++ {
		exclude[items[i].TimeStamp] = true
	}
	rules := "1h/1d,7d/delete"
	parsedRules, err := parseRules(rules)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = e.evict(items, parsedRules, exclude)
	}
}

// ---------------------------------------------------------------------------
// BenchmarkParseRules — shows if rule parsing should be cached
// ---------------------------------------------------------------------------

func BenchmarkParseRules_Simple(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = parseRules("1h/1d,7d/delete")
	}
}

func BenchmarkParseRules_Complex(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = parseRules("1h/1d,7d/delete,30d/delete")
	}
}

// ---------------------------------------------------------------------------
// BenchmarkTaskPoolEnqueue — channel send + zap field allocation
// ---------------------------------------------------------------------------

func BenchmarkTaskPoolEnqueue_NopLogger(b *testing.B) {
	benchEnqueue(b, zap.NewNop().Sugar())
}

func BenchmarkTaskPoolEnqueue_ProductionLogger(b *testing.B) {
	// Use os.DevNull for cross-platform discard output.
	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{os.DevNull}
	cfg.ErrorOutputPaths = []string{os.DevNull}
	prod, err := cfg.Build()
	if err != nil {
		b.Skipf("cannot build production logger: %v", err)
	}
	benchEnqueue(b, prod.Sugar())
}

func benchEnqueue(b *testing.B, logger *zap.SugaredLogger) {
	b.Helper()
	// Buffer large enough to never block during the benchmark.
	pool, _ := NewTaskPoolForTest(b.N+1, logger)
	task := Task{
		Type:     "backup",
		ProcType: ProcTypeFull,
		Vault:    entity.Vault{Folder: "/backup-storage/20260419T120000"},
		Job:      entity.Job{Vault: "20260419T120000", TaskID: "bench-task-id"},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		pool.EnqueueTask(task)
	}
}

// ---------------------------------------------------------------------------
// BenchmarkTaskExecutorProcess — full task processing without real I/O
// (uses a stub CommandExecutor that returns immediately)
// ---------------------------------------------------------------------------

type noopCommandExecutor struct{}

func (n *noopCommandExecutor) ExecuteEvictCmd(_ string) error { return nil }
func (n *noopCommandExecutor) PerformBackup(_ entity.Vault, _ []entity.DBEntry, _ map[string]string) error {
	return nil
}
func (n *noopCommandExecutor) PerformRestore(_ string, _ []entity.DBEntry, _ map[string]string, _ map[string]string, _ bool, _ string) error {
	return nil
}
func (n *noopCommandExecutor) GetBackupDBs(_ string) ([]string, error) { return nil, nil }
func (n *noopCommandExecutor) PerformEviction(_ context.Context) error { return nil }
func (n *noopCommandExecutor) SetEvictionPolicy(_, _ string) error     { return nil }

type noopDBRepo struct{}

func (n *noopDBRepo) SaveJob(_ context.Context, _ entity.Job) error   { return nil }
func (n *noopDBRepo) UpdateJob(_ context.Context, _ entity.Job) error { return nil }
func (n *noopDBRepo) GetJob(_ context.Context, _ string) (entity.Job, error) {
	return entity.Job{}, nil
}
func (n *noopDBRepo) RemoveVault(_ context.Context, _ string) error { return nil }
func (n *noopDBRepo) RemoveJob(_ context.Context, _ string) error   { return nil }
func (n *noopDBRepo) SelectEverything(_ context.Context, _ string) (entity.Job, error) {
	return entity.Job{}, nil
}

type noopS3Client struct{}

func (n *noopS3Client) CreatePresignedUrl(_ context.Context, _ string, _ int) (string, error) {
	return "", nil
}
func (n *noopS3Client) ListFiles(_ context.Context, _ string) ([]string, error) { return nil, nil }
func (n *noopS3Client) UploadFolder(_ context.Context, _ string) error          { return nil }
func (n *noopS3Client) UploadFolderWithPrefix(_ context.Context, _, _ string) error {
	return nil
}
func (n *noopS3Client) DownloadFolder(_ context.Context, _, _ string) error { return nil }
func (n *noopS3Client) DeletePrefix(_ context.Context, _ string) error      { return nil }
func (n *noopS3Client) RawClient() utils.ClientInterface                    { return nil }

func BenchmarkTaskExecutorProcess_Backup(b *testing.B) {
	logger := zap.NewNop().Sugar()
	ch := make(chan Task, b.N+1)
	te := NewTaskExecutorForTest(ch, &noopCommandExecutor{}, &noopDBRepo{}, &noopS3Client{}, false, logger)
	task := Task{
		Type:       "backup",
		ProcType:   ProcTypeFull,
		Vault:      entity.Vault{Folder: "/backup-storage/20260419T120000"},
		CustomVars: map[string]string{},
		Job: entity.Job{
			Vault:        "20260419T120000",
			TaskID:       "bench-task-id",
			CreationTime: time.Now().UTC().Format(time.RFC3339Nano),
		},
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		te.Process(ctx, task)
	}
}

// ---------------------------------------------------------------------------
// BenchmarkUniqueVaults — deduplication by timestamp
// ---------------------------------------------------------------------------

func BenchmarkUniqueVaults_100_NoDups(b *testing.B) {
	items := makeVaults(100, time.Hour, time.Hour)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = uniqueVaults(items)
	}
}

func BenchmarkUniqueVaults_100_AllDups(b *testing.B) {
	// All same timestamp → only 1 unique.
	ts := time.Now().Add(-2 * time.Hour).UnixMilli()
	items := make([]entity.Vault, 100)
	for i := range items {
		items[i] = entity.Vault{Folder: fmt.Sprintf("/backup/%d", i), TimeStamp: ts}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = uniqueVaults(items)
	}
}

// ---------------------------------------------------------------------------
// BenchmarkSetEvictionPolicy — RWMutex lock/unlock contention
// ---------------------------------------------------------------------------

func BenchmarkSetEvictionPolicy_NoContention(b *testing.B) {
	e := nopExecutor(zap.NewNop().Sugar())
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		err := e.SetEvictionPolicy("1h/1d,7d/delete", "7d/delete")
		if err != nil {
			return
		}
	}
}

func BenchmarkSetEvictionPolicy_Contention8Readers(b *testing.B) {
	e := nopExecutor(zap.NewNop().Sugar())
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	// 8 concurrent readers.
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					e.evictionMu.RLock()
					_ = e.evictionPolicy
					e.evictionMu.RUnlock()
				}
			}
		}()
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		err := e.SetEvictionPolicy("1h/1d,7d/delete", "7d/delete")
		if err != nil {
			b.Errorf("SetEvictionPolicy failed %v", err)
		}
	}
	b.StopTimer()
	cancel()
	wg.Wait()
}

// Ensure repo import is used (the noopDBRepo satisfies repo.DBRepository).
var _ repo.DBRepository = (*noopDBRepo)(nil)
