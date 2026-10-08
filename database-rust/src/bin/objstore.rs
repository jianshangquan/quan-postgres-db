use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::Router;
use serde::Deserialize;
use tokio::fs;
use tokio::sync::Mutex;
use tracing::info;

#[derive(Clone)]
struct AppState {
    root: PathBuf,
    lock: Arc<Mutex<()>>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let raw = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| ":9000".into());
    let addr: SocketAddr = if raw.starts_with(':') {
        format!("0.0.0.0{raw}").parse()?
    } else {
        raw.parse()?
    };

    let root = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    fs::create_dir_all(&root).await?;
    let state = AppState {
        root,
        lock: Arc::new(Mutex::new(())),
    };

    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/", get(|| async { "ok" }))
        .route("/{*key}", put(put_object).get(get_or_list).head(head_object))
        .with_state(state);

    info!("objstore listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn put_object(
    State(st): State<AppState>,
    AxumPath(key): AxumPath<String>,
    body: Bytes,
) -> impl IntoResponse {
    let _g = st.lock.lock().await;
    let path = st.root.join(&key);
    if let Some(parent) = path.parent() {
        if let Err(e) = fs::create_dir_all(parent).await {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    }
    if !key.contains('/') {
        if let Err(e) = fs::create_dir_all(&path).await {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
        return StatusCode::OK.into_response();
    }
    if let Err(e) = fs::write(&path, &body).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }
    StatusCode::OK.into_response()
}

#[derive(Deserialize)]
struct ListQuery {
    prefix: Option<String>,
    #[serde(rename = "list-type")]
    list_type: Option<String>,
}

async fn get_or_list(
    State(st): State<AppState>,
    AxumPath(key): AxumPath<String>,
    Query(q): Query<ListQuery>,
) -> Response {
    if !key.contains('/') || q.list_type.is_some() || q.prefix.is_some() {
        let bucket = key.split('/').next().unwrap_or(&key);
        let bucket_dir = st.root.join(bucket);
        if !bucket_dir.exists() {
            return (StatusCode::NOT_FOUND, "NoSuchBucket").into_response();
        }
        let prefix = q.prefix.unwrap_or_default();
        let mut contents = String::new();
        let mut count = 0;
        if let Ok(entries) = walk_files(&bucket_dir).await {
            for (rel, size) in entries {
                if !prefix.is_empty() && !rel.starts_with(&prefix) {
                    continue;
                }
                contents.push_str(&format!(
                    "<Contents><Key>{rel}</Key><Size>{size}</Size><StorageClass>STANDARD</StorageClass></Contents>"
                ));
                count += 1;
            }
        }
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>{bucket}</Name><Prefix>{prefix}</Prefix><IsTruncated>false</IsTruncated>
<KeyCount>{count}</KeyCount><MaxKeys>1000</MaxKeys>{contents}
</ListBucketResult>"#
        );
        return (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/xml")],
            xml,
        )
            .into_response();
    }
    let path = st.root.join(&key);
    match fs::read(&path).await {
        Ok(data) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            data,
        )
            .into_response(),
        Err(_) => (StatusCode::NOT_FOUND, "NoSuchKey").into_response(),
    }
}

async fn head_object(
    State(st): State<AppState>,
    AxumPath(key): AxumPath<String>,
) -> StatusCode {
    let path = st.root.join(&key);
    if path.exists() {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn walk_files(root: &Path) -> anyhow::Result<Vec<(String, u64)>> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut rd = fs::read_dir(&dir).await?;
        while let Some(ent) = rd.next_entry().await? {
            let p = ent.path();
            let meta = ent.metadata().await?;
            if meta.is_dir() {
                stack.push(p);
            } else {
                let rel = p.strip_prefix(root)?.to_string_lossy().replace('\\', "/");
                out.push((rel, meta.len()));
            }
        }
    }
    Ok(out)
}
