# Patched Postgres 16 (GetPage smgr) + helpers for database-go-2.
# Build from serverless/database-go-2:
#   docker build -f compute/Dockerfile -t quan-compute .

FROM golang:1.24-bookworm AS gobuild
WORKDIR /src

# walstreamer (TCP QWAL client)
COPY walstreamer/go.mod ./walstreamer/
WORKDIR /src/walstreamer
RUN go mod download
COPY walstreamer/ ./
RUN CGO_ENABLED=0 go build -o /out/walstreamer ./src/

# compute helpers
WORKDIR /src/compute
COPY compute/go.mod ./
COPY compute/cmd ./cmd
RUN CGO_ENABLED=0 go build -o /out/quansync ./cmd/quansync \
 && CGO_ENABLED=0 go build -o /out/quanrestore ./cmd/quanrestore \
 && CGO_ENABLED=0 go build -o /out/qwal ./cmd/qwal

FROM debian:bookworm-slim AS pgbuild
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates python3 \
    libreadline-dev zlib1g-dev flex bison libxml2-dev libssl-dev \
    libcurl4-openssl-dev pkg-config \
 && rm -rf /var/lib/apt/lists/*

ARG PG_VERSION=16.6
WORKDIR /build
RUN curl -fsSL "https://ftp.postgresql.org/pub/source/v${PG_VERSION}/postgresql-${PG_VERSION}.tar.bz2" \
    -o pg.tar.bz2 \
 && tar -xjf pg.tar.bz2

COPY compute/postgres-patch /patch
RUN python3 /patch/apply-patch.py /build/postgresql-${PG_VERSION}

WORKDIR /build/postgresql-${PG_VERSION}
RUN ./configure --prefix=/usr/local/pgsql --with-openssl --with-libxml \
    LIBS="-lcurl" \
 && make -j"$(nproc)" \
 && make install

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    libreadline8 zlib1g libxml2 libssl3 libcurl4 \
    curl ca-certificates bash gosu \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd -r postgres --gid=999 \
 && useradd -r -g postgres --uid=999 --home-dir=/var/lib/postgresql --shell=/bin/bash postgres \
 && mkdir -p /var/lib/postgresql/data /var/run/postgresql \
 && chown -R postgres:postgres /var/lib/postgresql /var/run/postgresql

COPY --from=pgbuild /usr/local/pgsql /usr/local/pgsql
ENV PATH="/usr/local/pgsql/bin:${PATH}" \
    PGDATA=/var/lib/postgresql/data \
    PAGESERVER_HTTP=http://pageserver:8081 \
    SAFEKEEPER_ADDR=safekeeper:5454 \
    SAFEKEEPER_HTTP=http://safekeeper:8080 \
    TENANT_ID=default \
    TIMELINE_ID=main \
    POSTGRES_USER=postgres \
    POSTGRES_PASSWORD=postgres \
    POSTGRES_DB=app \
    PGURL=postgres://postgres@127.0.0.1:5432/postgres?sslmode=disable

COPY --from=gobuild /out/walstreamer /usr/local/bin/walstreamer
COPY --from=gobuild /out/quansync /usr/local/bin/quansync
COPY --from=gobuild /out/quanrestore /usr/local/bin/quanrestore
COPY --from=gobuild /out/qwal /usr/local/bin/qwal
COPY compute/scripts/archive_wal.sh /usr/local/bin/archive_wal.sh
COPY compute/scripts/restore_command.sh /usr/local/bin/restore_command.sh
COPY compute/scripts/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY compute/postgresql.conf /etc/postgresql/postgresql.conf
RUN chmod +x /usr/local/bin/archive_wal.sh /usr/local/bin/restore_command.sh /usr/local/bin/entrypoint.sh \
 && chmod +x /usr/local/bin/walstreamer /usr/local/bin/quansync /usr/local/bin/quanrestore /usr/local/bin/qwal

EXPOSE 5432
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
