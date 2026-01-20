FROM --platform=$BUILDPLATFORM golang:1.24.5-alpine3.22 AS builder

ENV GOSUMDB=off

WORKDIR /app
# COPY alpine/alpine-repositories /etc/apk/repositories

COPY backup-daemon/go.mod go.mod
COPY backup-daemon/go.sum go.sum

RUN go mod download

COPY backup-daemon .

RUN go build -a -o backup-daemon ./cmd/main.go

FROM alpine:3.22.2

ENV BACKUP_DAEMON_HOME=/opt/backup
ENV S3_CERT_PATH_INTERNAL=/s3CertInternal

WORKDIR ${BACKUP_DAEMON_HOME}
# COPY alpine/alpine-repositories /etc/apk/repositories

COPY --from=builder /app/backup-daemon ${BACKUP_DAEMON_HOME}/

RUN set -x && apk add --no-cache apk-tools grep curl

RUN mkdir -p /var/log ${S3_CERT_PATH_INTERNAL} /backup-storage \
    && chmod 777 /var/log ${S3_CERT_PATH_INTERNAL} /backup-storage

VOLUME /backup-storage

CMD ["./backup-daemon"]