FROM postgres:16-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/*

COPY postgresql.conf /etc/postgresql/postgresql.conf
COPY scripts/archive_wal.sh /usr/local/bin/archive_wal.sh
COPY scripts/restore_command.sh /usr/local/bin/restore_command.sh
RUN chmod +x /usr/local/bin/archive_wal.sh /usr/local/bin/restore_command.sh

ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_USER=postgres
ENV POSTGRES_DB=app

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
