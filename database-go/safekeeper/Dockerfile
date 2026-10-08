FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/safekeeper ./src/

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/safekeeper /usr/local/bin/safekeeper
ENV LISTEN_ADDR=:5454 HTTP_ADDR=:8080 DATA_DIR=/data PAGESERVER_URL=http://pageserver:8081
EXPOSE 5454 8080
ENTRYPOINT ["safekeeper"]
