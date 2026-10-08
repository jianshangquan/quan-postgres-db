use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use quan_erp_database::config::Config;
use quan_erp_database::pb::raft_server::RaftServer;
use quan_erp_database::pb::storage_server::StorageServer;
use quan_erp_database::raft::RaftNode;
use quan_erp_database::rpc::{archive_router, SafekeeperSvc};
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

    let grpc_advertise = cfg
        .peers
        .iter()
        .find(|p| p.id == cfg.node_id)
        .map(|p| p.grpc_addr.clone())
        .unwrap_or_else(|| cfg.listen_addr.clone());

    let node = RaftNode::new(
        cfg.node_id.clone(),
        grpc_advertise,
        cfg.peers.clone(),
        Arc::clone(&store),
    );
    node.spawn_background();

    let svc = SafekeeperSvc::new(Arc::clone(&node), cfg.pageserver_addr.clone()).await;
    let seeds: Vec<String> = cfg.peers.iter().map(|p| p.grpc_addr.clone()).collect();

    let http_addr: SocketAddr = cfg.http_addr.parse()?;
    let http_app = archive_router(seeds);
    tokio::spawn(async move {
        info!("safekeeper http {http_addr}");
        let listener = tokio::net::TcpListener::bind(http_addr)
            .await
            .expect("http bind");
        axum::serve(listener, http_app).await.expect("http serve");
    });

    let grpc_addr: SocketAddr = cfg.listen_addr.parse()?;
    info!(
        "safekeeper gRPC {grpc_addr} node={} pageserver={}",
        cfg.node_id, cfg.pageserver_addr
    );
    Server::builder()
        .add_service(StorageServer::new(svc.clone()))
        .add_service(RaftServer::new(svc))
        .serve(grpc_addr)
        .await?;
    Ok(())
}
