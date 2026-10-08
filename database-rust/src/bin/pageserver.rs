use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::routing::get;
use axum::Router;
use quan_erp_database::config::Config;
use quan_erp_database::pb::storage_server::StorageServer;
use quan_erp_database::rpc::PageserverSvc;
use quan_erp_database::store::{CachedStore, ObjectStore, S3Store};
use tonic::transport::Server;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = Config::from_env()?;
    let remote = S3Store::new(
        &cfg.s3_endpoint,
        &cfg.s3_region,
        &cfg.s3_bucket,
        &cfg.s3_access_key,
        &cfg.s3_secret_key,
    )
    .await?;
    for _ in 0..30 {
        if remote.ensure_bucket().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let cache_dir = format!("{}/cache", cfg.data_dir);
    tokio::fs::create_dir_all(&cache_dir).await.ok();
    let store = Arc::new(CachedStore::new(remote, cache_dir, 512));
    let svc = PageserverSvc::new(store, cfg.node_id.clone());

    let http_addr: SocketAddr = cfg.http_addr.parse()?;
    tokio::spawn(async move {
        let app = Router::new().route("/healthz", get(|| async { "ok" }));
        info!("pageserver http {http_addr}");
        let listener = tokio::net::TcpListener::bind(http_addr)
            .await
            .expect("http bind");
        axum::serve(listener, app).await.expect("http serve");
    });

    let grpc_addr: SocketAddr = cfg.listen_addr.parse()?;
    info!("pageserver gRPC {grpc_addr}");
    Server::builder()
        .add_service(StorageServer::new(svc))
        .serve(grpc_addr)
        .await?;
    Ok(())
}
