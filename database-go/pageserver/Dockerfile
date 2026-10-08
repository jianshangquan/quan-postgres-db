FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/pageserver ./src/

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/pageserver /usr/local/bin/pageserver
ENV LISTEN_ADDR=:8081 DATA_DIR=/data
EXPOSE 8081
ENTRYPOINT ["pageserver"]
