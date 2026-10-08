FROM rust:1.87-bookworm AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release --bins

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/safekeeper /usr/local/bin/safekeeper
COPY --from=build /src/target/release/pageserver /usr/local/bin/pageserver
COPY --from=build /src/target/release/objstore /usr/local/bin/objstore
EXPOSE 50051 8080 8081 9000
ENTRYPOINT ["safekeeper"]
