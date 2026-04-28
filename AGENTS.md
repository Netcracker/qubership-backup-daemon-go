# CLAUDE.md

This file provides guidance to AI coding assistants when working with code in this repository.

## Project Overview

This is a Go-based backup daemon that provides REST API and scheduled backup capabilities for databases. It's a port/rewrite of a Python-based backup daemon to Go. The service provides backup, restore, eviction (cleanup), and management operations for database backups, with support for both local filesystem and S3 storage.

## Build and Development Commands

### Building
```bash
cd backup-daemon
go build -o backup-daemon ./cmd/main.go
go build -o bdcli ./cmd/bdcli.go
```

### Testing
```bash
cd backup-daemon
go test -v -count=1 ./...
```

### Linting
```bash
cd backup-daemon
golangci-lint run
```

### Docker Build
```bash
docker build -f build/Dockerfile -t backup-daemon:latest .
```

## Architecture

### Core Components

**Main Entry Point (`backup-daemon/cmd/main.go`)**
- Loads configuration from HOCON files (`backup-daemon.conf`)
- Supports both `/etc/backup-daemon.conf` and local config with fallback
- Initializes separate configurations for full and incremental backups
- Sets up S3 aliases if configured
- Bootstraps the entire application

**Application Layer (`backup-daemon/app/app/`)**
- Wires together all components: database, storage repos, executors, task pool, HTTP server
- Creates two parallel execution paths: full backups and incremental backups
- Handles graceful shutdown on SIGTERM/SIGINT
- Parses custom variables from HOCON format into Go maps

**Controller Layer (`backup-daemon/app/controller/`)**
- `BackupDaemon`: Core business logic for backup/restore/eviction operations
- `Scheduler`: Handles cron-based scheduling of backups
- Implements the `BackupDaemonUseCase` interface

**Task Execution (`backup-daemon/app/tasks/`)**
- `Executor`: Executes shell commands (backup_command, restore_command, etc.) via Go templates
- `TaskPool`: Manages concurrent task execution with a worker pool
- Eviction logic uses parsed rules to determine which backups to delete

**REST API (`backup-daemon/app/rest/`)**
- `EndpointHandler`: Legacy REST endpoints (e.g., `/backup`, `/restore`)
- `EndpointHandlerV2`: V2 API endpoints under `/api/v1` with structured JSON responses
- Uses Gin framework for routing

**Repository Layer (`backup-daemon/app/repo/`)**
- `StorageRepository`: Abstracts filesystem operations (local or S3)
- `DBRepository`: SQLite-based persistence for job tracking
- Separate implementations for local filesystem and S3

**Configuration (`backup-daemon/app/config/`)**
- Defines `Config` struct with all daemon settings
- Supports environment variable overrides via `${?VAR_NAME}` syntax in HOCON

### Key Architectural Patterns

1. **Dual Storage Model**: The daemon supports both "full" and "incremental" backup types, each with its own:
   - Configuration
   - Executor
   - Storage repository
   - REST endpoints (e.g., `/backup` vs `/incremental/backup`)

2. **Command Templates**: Backend commands (backup, restore, list, evict) are Go text templates that get interpolated with:
   - `{{.data_folder}}`: vault folder path
   - `{{.dbs}}`: databases to process
   - `{{.dbmap}}`: database rename mappings
   - Custom variables defined in config

3. **Task Lifecycle**:
   - REST handler receives request → creates task
   - Task enqueued to `TaskPool`
   - Worker executes command via `Executor`
   - Result persisted to SQLite via `DBRepository`
   - For S3-enabled mode: backup written to local storage first, then moved to S3

4. **Eviction Policies**: Time-based retention specified as `start_time/interval` (e.g., `7d/7d,1y/delete`) or count-based as `N/delete`

5. **S3 Aliases (Partial Support)**: The system supports loading S3 configurations from `s3_aliases.json` for multi-storage setups. Currently, only the "default" alias is fully integrated.

## Important Files and Locations

- `backup-daemon.conf`: Main configuration file (HOCON format)
- `backup-daemon/cmd/main.go`: Application entry point
- `backup-daemon/app/app/app.go`: Application bootstrap and wiring
- `backup-daemon/app/controller/backup-daemon.go`: Core backup/restore logic
- `backup-daemon/app/tasks/executor.go`: Command execution
- `backup-daemon/app/rest/handler.go`: Legacy REST API
- `backup-daemon/app/rest/handlerV2.go`: V2 REST API
- `backup-daemon/go.mod`: Go module dependencies

## Configuration Notes

- The config uses HOCON format with environment variable substitution
- Two separate schedules: `schedule` (full backups) and `incremental_schedule`
- S3 can be enabled via `s3_enabled: "true"` + related S3 settings
- TLS can be enabled via `tls_enabled: "true"`
- Custom variables allow passing arbitrary parameters to backend scripts

## Testing Strategy

- Unit tests use `go.uber.org/mock` for mocking interfaces
- Benchmark tests are present (e.g., `*_bench_test.go`)
- Integration tests exist for database layer (`db_integration_test.go`)
- Tests are run with `-count=1` to disable caching

## Common Pitfalls

1. **Template Syntax**: Backend commands use Go template syntax `{{.var}}`, not Python-style `%(var)s`
2. **HOCON Parsing**: Custom variables can be specified as plain identifiers, KEY=VALUE pairs, or HOCON objects `{key: "value"}`
3. **S3 Aliases**: Not fully implemented across all code paths—some areas still use the original S3 client configuration
4. **Dual Backup Types**: Many operations have separate code paths for full vs incremental backups; changes may need to be duplicated

## CLI Tool

The `bdcli` command-line tool is built alongside the main daemon and provides a shell client for the REST API. It's installed to `/usr/bin/bdcli` in the Docker image.
