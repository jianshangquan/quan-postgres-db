use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing::warn;

use crate::pb::raft_server::Raft;
use crate::pb::storage_client::StorageClient;
use crate::pb::storage_server::Storage;
use crate::pb::*;
use crate::raft::{Command, RaftNode};
use crate::store::ObjectStore;

pub struct SafekeeperSvc<S: ObjectStore + 'static> {
    pub node: Arc<RaftNode<S>>,
    pub pageserver_addr: String,
}

impl<S: ObjectStore + 'static> Clone for SafekeeperSvc<S> {
    fn clone(&self) -> Self {
        Self {
            node: Arc::clone(&self.node),
            pageserver_addr: self.pageserver_addr.clone(),
        }
    }
}

impl<S: ObjectStore + 'static> SafekeeperSvc<S> {
    pub async fn new(node: Arc<RaftNode<S>>, pageserver_addr: String) -> Self {
        if !pageserver_addr.is_empty() {
            let addr = pageserver_addr.clone();
            let n = Arc::clone(&node);
            n.set_on_apply(move |aw| {
                let addr = addr.clone();
                tokio::spawn(async move {
                    if let Err(e) = fanout_ingest(&addr, aw).await {
                        warn!("pageserver fanout: {e:#}");
                    }
                });
            })
            .await;
        }
        Self {
            node,
            pageserver_addr,
        }
    }
}

async fn fanout_ingest(addr: &str, aw: crate::raft::AppliedWal) -> anyhow::Result<()> {
    let endpoint = if addr.starts_with("http") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    };
    let channel = tonic::transport::Channel::from_shared(endpoint)?
        .connect()
        .await?;
    let mut cli = StorageClient::new(channel)
        .max_decoding_message_size(64 * 1024 * 1024)
        .max_encoding_message_size(64 * 1024 * 1024);
    cli.ingest_wal(IngestWalRequest {
        tenant_id: aw.tenant_id,
        timeline_id: aw.timeline_id,
        start_lsn: aw.start_lsn,
        end_lsn: aw.end_lsn,
        payload: aw.payload,
    })
    .await?;
    Ok(())
}

#[tonic::async_trait]
impl<S: ObjectStore + 'static> Storage for SafekeeperSvc<S> {
    async fn create_timeline(
        &self,
        req: Request<CreateTimelineRequest>,
    ) -> Result<Response<CreateTimelineResponse>, Status> {
        let r = req.into_inner();
        if r.tenant_id.is_empty() || r.timeline_id.is_empty() {
            return Err(Status::invalid_argument(
                "tenant_id and timeline_id required",
            ));
        }
        if !self.node.is_leader().await {
            let leader = self.node.leader_grpc_addr().await;
            return Err(Status::failed_precondition(format!(
                "not leader; try {leader}"
            )));
        }
        self.node
            .propose(Command::CreateTimeline {
                tenant_id: r.tenant_id.clone(),
                timeline_id: r.timeline_id.clone(),
            })
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        Ok(Response::new(CreateTimelineResponse {
            tenant_id: r.tenant_id,
            timeline_id: r.timeline_id,
        }))
    }

    async fn append_wal(
        &self,
        req: Request<AppendWalRequest>,
    ) -> Result<Response<AppendWalResponse>, Status> {
        let r = req.into_inner();
        if r.tenant_id.is_empty() || r.timeline_id.is_empty() || r.payload.is_empty() {
            return Err(Status::invalid_argument(
                "tenant_id, timeline_id, payload required",
            ));
        }
        if !self.node.is_leader().await {
            let leader = self.node.leader_grpc_addr().await;
            return Err(Status::failed_precondition(format!(
                "not leader; try {leader}"
            )));
        }
        let ack = self
            .node
            .propose(Command::AppendWal {
                tenant_id: r.tenant_id,
                timeline_id: r.timeline_id,
                start_lsn: r.start_lsn,
                end_lsn: r.end_lsn,
                payload: r.payload,
            })
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;
        Ok(Response::new(AppendWalResponse {
            ack_lsn: ack,
            leader_addr: self.node.leader_grpc_addr().await,
        }))
    }

    async fn get_page(
        &self,
        _req: Request<GetPageRequest>,
    ) -> Result<Response<GetPageResponse>, Status> {
        Err(Status::unimplemented("GetPage is served by pageserver"))
    }

    async fn get_status(
        &self,
        _req: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, Status> {
        let (is_leader, leader_addr, last_lsn) = self.node.status_role().await;
        Ok(Response::new(GetStatusResponse {
            role: "safekeeper".into(),
            node_id: self.node.node_id.clone(),
            is_leader,
            leader_addr,
            last_lsn,
        }))
    }

    async fn ingest_wal(
        &self,
        _req: Request<IngestWalRequest>,
    ) -> Result<Response<IngestWalResponse>, Status> {
        Err(Status::unimplemented("IngestWAL is served by pageserver"))
    }
}

#[tonic::async_trait]
impl<S: ObjectStore + 'static> Raft for SafekeeperSvc<S> {
    async fn request_vote(
        &self,
        req: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        Ok(Response::new(
            self.node.handle_request_vote(req.into_inner()).await,
        ))
    }

    async fn append_entries(
        &self,
        req: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        Ok(Response::new(
            self.node.handle_append_entries(req.into_inner()).await,
        ))
    }
}
