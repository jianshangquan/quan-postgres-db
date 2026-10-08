FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/walstreamer ./src/

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates postgresql-client \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/walstreamer /usr/local/bin/walstreamer
ENV SAFEKEEPER_ADDR=safekeeper:5454 \
    TENANT_ID=default \
    TIMELINE_ID=main \
    PGDATA=/var/lib/postgresql/data \
    PGURL=postgres://postgres:postgres@compute:5432/postgres?sslmode=disable \
    PSQL=psql
ENTRYPOINT ["walstreamer"]
