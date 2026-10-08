use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use serde::Deserialize;

use crate::pb::storage_client::StorageClient;
use crate::pb::AppendWalRequest;

#[derive(Clone)]
pub struct ArchiveState {
    pub seeds: Vec<String>,
}

#[derive(Deserialize)]
pub struct WalQuery {
    #[serde(default = "default_tenant")]
    tenant: String,
    #[serde(default = "default_timeline")]
    timeline: String,
    #[serde(default)]
    start_lsn: u64,
    #[serde(default)]
    end_lsn: u64,
}

fn default_tenant() -> String {
    "default".into()
}
fn default_timeline() -> String {
    "main".into()
}

pub fn archive_router(seeds: Vec<String>) -> Router {
    Router::new()
        .route("/wal", post(post_wal))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(ArchiveState { seeds })
}

async fn post_wal(
    State(st): State<ArchiveState>,
    Query(q): Query<WalQuery>,
    body: Bytes,
) -> impl IntoResponse {
    let end_lsn = if q.end_lsn == 0 {
        q.start_lsn + body.len() as u64
    } else {
        q.end_lsn
    };
    let req = AppendWalRequest {
        tenant_id: q.tenant,
        timeline_id: q.timeline,
        start_lsn: q.start_lsn,
        end_lsn,
        payload: body.to_vec(),
    };
    match append_with_redirect(&st.seeds, req).await {
        Ok(ack) => (StatusCode::OK, [("x-ack-lsn", ack.to_string())], "ok").into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    }
}

async fn append_with_redirect(
    seeds: &[String],
    req: AppendWalRequest,
) -> anyhow::Result<u64> {
    let mut queue: Vec<String> = seeds.to_vec();
    let mut tried = std::collections::HashSet::new();
    let mut last_err = anyhow::anyhow!("no safekeeper");
    while let Some(addr) = queue.pop() {
        if !tried.insert(addr.clone()) {
            continue;
        }
        let endpoint = if addr.starts_with("http") {
            addr.clone()
        } else {
            format!("http://{addr}")
        };
        match StorageClient::connect(endpoint).await {
            Ok(mut cli) => match cli.append_wal(req.clone()).await {
                Ok(resp) => return Ok(resp.into_inner().ack_lsn),
                Err(status) => {
                    let msg = status.message().to_string();
                    if let Some(rest) = msg.strip_prefix("not leader; try ") {
                        let leader = rest.trim();
                        if !leader.is_empty() {
                            queue.push(leader.to_string());
                        }
                    }
                    last_err = anyhow::anyhow!("{msg}");
                }
            },
            Err(e) => last_err = e.into(),
        }
    }
    Err(last_err)
}
