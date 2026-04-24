FROM --platform=$BUILDPLATFORM golang:1.25-alpine3.23 AS builder

ENV GOSUMDB=off

WORKDIR /app
# COPY alpine/alpine-repositories /etc/apk/repositories

COPY backup-daemon/go.mod go.mod
COPY backup-daemon/go.sum go.sum

RUN go mod download

COPY backup-daemon .

RUN go build -a -o backup-daemon ./cmd/main.go \
    && go build -a -o bdcli ./cmd/bdcli.go

FROM debian:12-slim

ENV BACKUP_DAEMON_HOME=/opt/backup
ENV S3_CERT_PATH_INTERNAL=/s3CertInternal

WORKDIR ${BACKUP_DAEMON_HOME}
# COPY alpine/alpine-repositories /etc/apk/repositories

COPY --from=builder /app/backup-daemon ${BACKUP_DAEMON_HOME}/
COPY --from=builder /app/bdcli /usr/bin/bdcli
COPY backup-daemon.conf ${BACKUP_DAEMON_HOME}/backup-daemon.conf
RUN chmod +x /usr/bin/bdcli

RUN set -x && apt-get update && apt-get install -y \
    grep \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /var/log ${S3_CERT_PATH_INTERNAL} /backup-storage \
    && chmod 777 /var/log ${S3_CERT_PATH_INTERNAL} /backup-storage

VOLUME /backup-storage

CMD ["./backup-daemon"]