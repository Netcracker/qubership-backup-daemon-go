package entity

import (
	"encoding/json"
	"testing"
)

// ---------------------------------------------------------------------------
// JSON payloads
// ---------------------------------------------------------------------------

// Minimal backup request — no DBs, no custom vars
var backupPayloadMinimal = []byte(`{}`)

// Typical backup request with DB list
var backupPayloadWithDBs = []byte(`{"dbs":["db1","db2","db3"],"allow_eviction":"true","sharded":false}`)

// Backup request with custom vars that land in CustomVars map via unknown-key loop
var backupPayloadWithCustomVars = []byte(`{"dbs":["db1"],"topic_regex":"my-topic","mode":"compact","region":"us-east-1"}`)

// Large request: 20 DBs + multiple custom vars
var backupPayloadLarge = func() []byte {
	type payload struct {
		DBs        []string `json:"dbs"`
		TopicRegex string   `json:"topic_regex"`
		Mode       string   `json:"mode"`
		Region     string   `json:"region"`
	}
	dbs := make([]string, 20)
	for i := range dbs {
		dbs[i] = "topic-" + string(rune('A'+i))
	}
	b, _ := json.Marshal(payload{DBs: dbs, TopicRegex: ".*", Mode: "full", Region: "eu-west-1"})
	return b
}()

// Restore request with changeDbNames — exercises the unknown-key map loop
var restorePayloadWithChangeNames = []byte(`{"vault":"20260419T120000","changeDbNames":{"old_db":"new_db"}}`)

// Restore request with unknown fields (go to CustomVars)
var restorePayloadWithCustomVars = []byte(`{"vault":"20260419T120000","region":"us-east-1","mode":"full"}`)

// FindRequest variants
var findPayloadNumber = []byte(`{"ts":1774000000000}`)
var findPayloadString = []byte(`{"ts":"1774000000000"}`)

// ---------------------------------------------------------------------------
// BackupRequest benchmarks
// ---------------------------------------------------------------------------

// BenchmarkBackupRequestUnmarshal_Minimal measures the cheapest path.
func BenchmarkBackupRequestUnmarshal_Minimal(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var r BackupRequest
		_ = json.Unmarshal(backupPayloadMinimal, &r)
	}
}

func BenchmarkBackupRequestUnmarshal_WithDBs(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var r BackupRequest
		_ = json.Unmarshal(backupPayloadWithDBs, &r)
	}
}

// BenchmarkBackupRequestUnmarshal_WithCustomVars exercises the unknown-key path
// (currently handled in handler.go via a raw map decode + struct decode).
func BenchmarkBackupRequestUnmarshal_WithCustomVars(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var r BackupRequest
		_ = json.Unmarshal(backupPayloadWithCustomVars, &r)
	}
}

func BenchmarkBackupRequestUnmarshal_Large(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var r BackupRequest
		_ = json.Unmarshal(backupPayloadLarge, &r)
	}
}

// ---------------------------------------------------------------------------
// RestoreRequest benchmarks — has custom UnmarshalJSON using getFieldsName()
// ---------------------------------------------------------------------------

// BenchmarkRestoreRequestUnmarshal_Basic measures the baseline.
func BenchmarkRestoreRequestUnmarshal_Basic(b *testing.B) {
	basic := []byte(`{"vault":"20260419T120000"}`)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var r RestoreRequest
		_ = json.Unmarshal(basic, &r)
	}
}

// BenchmarkRestoreRequestUnmarshal_WithChangeNames adds map field → more allocations.
func BenchmarkRestoreRequestUnmarshal_WithChangeNames(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var r RestoreRequest
		_ = json.Unmarshal(restorePayloadWithChangeNames, &r)
	}
}

// BenchmarkRestoreRequestUnmarshal_WithCustomVars exercises the getFieldsName() +
// unknown-key loop that populates CustomVars.
func BenchmarkRestoreRequestUnmarshal_WithCustomVars(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var r RestoreRequest
		_ = json.Unmarshal(restorePayloadWithCustomVars, &r)
	}
}

// ---------------------------------------------------------------------------
// FindRequest.UnmarshalJSON — two-branch (string vs int64) decode
// ---------------------------------------------------------------------------

func BenchmarkFindRequestUnmarshal_Number(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var r FindRequest
		_ = json.Unmarshal(findPayloadNumber, &r)
	}
}

func BenchmarkFindRequestUnmarshal_String(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var r FindRequest
		_ = json.Unmarshal(findPayloadString, &r)
	}
}

// ---------------------------------------------------------------------------
// DBEntry.UnmarshalJSON — string vs object branch
// ---------------------------------------------------------------------------

func BenchmarkDBEntryUnmarshal_String(b *testing.B) {
	data := []byte(`"my-topic-name"`)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var e DBEntry
		_ = json.Unmarshal(data, &e)
	}
}

func BenchmarkDBEntryUnmarshal_Object(b *testing.B) {
	data := []byte(`{"mydb":{"tables":["t1","t2"]}}`)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var e DBEntry
		_ = json.Unmarshal(data, &e)
	}
}

// ---------------------------------------------------------------------------
// VaultToMap — allocations per call (called on every ListBackup response)
// ---------------------------------------------------------------------------

func BenchmarkVaultToMap(b *testing.B) {
	v := Vault{
		Folder:      "20260419T120000",
		TimeStamp:   1774000000000,
		IsEvictable: true,
		IsSharded:   false,
		IsGranular:  false,
		IsFailed:    false,
		IsLocked:    false,
		Canceled:    false,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = v.ToMap()
	}
}
