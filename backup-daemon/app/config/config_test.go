package config

import (
	"reflect"
	"testing"
)

// fieldTag returns the value of a struct tag key for the named field of Config.
func fieldTag(t *testing.T, fieldName, tagKey string) string {
	t.Helper()
	f, ok := reflect.TypeOf(Config{}).FieldByName(fieldName)
	if !ok {
		t.Fatalf("Config has no field %q", fieldName)
	}
	return f.Tag.Get(tagKey)
}

// TestConfig_EnvTags acts as living documentation for the env var names exposed
// by the daemon. If a tag is renamed the test fails, forcing a deliberate review
// of docs, docker-compose files, and HOCON configs that reference the old name.
func TestConfig_EnvTags(t *testing.T) {
	tests := []struct {
		field   string
		wantEnv string
	}{
		{"Schedule", "BACKUP_SCHEDULE"},
		{"StorageRoot", "STORAGE"},
		{"ExternalRoot", "STORAGE_EXTERNAL"},
		{"DBPath", "DB_PATH"},
		{"EvictionPolicy", "EVICTION_POLICY"},
		{"GranularEvictionPolicy", "GRANULAR_EVICTION_POLICY"},
		{"GranularSchedule", "GRANULAR_SCHEDULE"},
		{"IncrementalSchedule", "INCREMENTAL_SCHEDULE"},
		{"ScheduledDBs", "SCHEDULED_DBS"},
		{"BackupCmd", "BACKUP_COMMAND"},
		{"RestoreCmd", "RESTORE_COMMAND"},
		{"DbListCmd", "LIST_COMMAND"},
		{"EvictCmd", "EVICT_CMD"},
		{"CustomVars", "CUSTOM_VARS"},
		{"S3URL", "S3_URL"},
		{"AccessKeyID", "S3_KEY_ID"},
		{"AccessKeySecret", "S3_KEY_SECRET"},
		{"BucketName", "S3_BUCKET"},
		{"Region", "S3_REGION"},
		{"TLSEnabled", "TLS_ENABLED"},
		{"TLSPort", "TLS_PORT"},
		{"CertsPath", "CERTS_PATH"},
		{"AliasesPath", "ALIASES_PATH"},
		{"S3AliasesUsed", "S3_ALIASES_USED"},
	}

	for _, tc := range tests {
		t.Run(tc.field, func(t *testing.T) {
			got := fieldTag(t, tc.field, "env")
			if got != tc.wantEnv {
				t.Errorf("Config.%s env tag = %q, want %q", tc.field, got, tc.wantEnv)
			}
		})
	}
}

// TestConfig_Defaults verifies the default values declared in struct tags.
// Changing a default is a potentially breaking change for existing deployments.
func TestConfig_Defaults(t *testing.T) {
	tests := []struct {
		field       string
		wantDefault string
	}{
		{"Port", "8080"},
		{"StorageRoot", "/backup-storage"},
		{"ExternalRoot", "/external"},
		{"DBPath", ""},
		{"Region", "us-east-1"},
		{"DbmapKey", "-m"},
		{"TLSPort", "8443"},
		{"TLSEnabled", "false"},
		{"CertsPath", "/tls/"},
		{"AliasesPath", "/aliases/"},
		{"BackupCmd", "ls -la {{.data_folder}}"},
		{"RestoreCmd", "ls -la {{.data_folder}}"},
		{"DbListCmd", "ls -1 {{.data_folder}}"},
	}

	for _, tc := range tests {
		t.Run(tc.field, func(t *testing.T) {
			got := fieldTag(t, tc.field, "default")
			if got != tc.wantDefault {
				t.Errorf("Config.%s default = %q, want %q", tc.field, got, tc.wantDefault)
			}
		})
	}
}
