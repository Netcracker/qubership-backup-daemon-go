package utils

import (
	"fmt"
	"os"
	"strings"
)

func GetSecretValue(key string) string {
	dir := os.Getenv("BACKUP_DAEMON_SECRETS_DIR")
	if dir != "" {
		path := fmt.Sprintf("%s/%s", dir, key)
		data, err := os.ReadFile(path)
		if err == nil {
			return strings.TrimSuffix(string(data), "\n")
		}
	}
	return os.Getenv(key)
}
