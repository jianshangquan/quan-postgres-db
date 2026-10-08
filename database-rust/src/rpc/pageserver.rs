use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::apply::Applier;
use crate::page::PageCache;
use crate::pb::storage_server::Storage;
use crate::pb::*;
use crate::store::{ObjectStore, TimelineMeta};

pub struct PageserverSvc<S: ObjectStore> {
    store: Arc<S>,
    applier: Applier<Arc<S>>,
    cache: PageCache,
    node_id: String,
}

impl<S: ObjectStore + 'static> PageserverSvc<S> {
    pub fn new(store: Arc<S>, node_id: String) -> Self {
        Self {
            applier: Applier::new(Arc::clone(&store)),
            store,
            cache: PageCache::new(256),
            node_id,
        }
    }
}

#[tonic::async_trait]
impl<S: ObjectStore + 'static> Storage for PageserverSvc<S> {
    async fn create_timeline(
        &self,
        req: Request<CreateTimelineRequest>,
    ) -> Result<Response<CreateTimelineResponse>, Status> {
        let r = req.into_inner();
        self.store
            .put_meta(&TimelineMeta {
                tenant_id: r.tenant_id.clone(),
                timeline_id: r.timeline_id.clone(),
                last_lsn: 0,
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(CreateTimelineResponse {
            tenant_id: r.tenant_id,
            timeline_id: r.timeline_id,
        }))
    }

    async fn append_wal(
        &self,
        _req: Request<AppendWalRequest>,
    ) -> Result<Response<AppendWalResponse>, Status> {
        Err(Status::unimplemented("AppendWAL is served by safekeeper"))
    }

    async fn ingest_wal(
        &self,
        req: Request<IngestWalRequest>,
    ) -> Result<Response<IngestWalResponse>, Status> {
        let r = req.into_inner();
        let applied = self
            .applier
            .apply(
                &r.tenant_id,
                &r.timeline_id,
                r.start_lsn,
                r.end_lsn,
                &r.payload,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(IngestWalResponse {
            applied_lsn: applied,
        }))
    }

    async fn get_page(
        &self,
        req: Request<GetPageRequest>,
    ) -> Result<Response<GetPageResponse>, Status> {
        let r = req.into_inner();
        let request_lsn = if r.request_lsn == 0 {
            u64::MAX
        } else {
            r.request_lsn
        };
        if let Some((page, lsn)) = self.cache.get(
            &r.tenant_id,
            &r.timeline_id,
            r.rel_filenode,
            r.fork,
            r.block,
            request_lsn,
        ) {
            return Ok(Response::new(GetPageResponse { page, lsn }));
        }
        let (page, lsn) = self
            .applier
            .get_page(
                &r.tenant_id,
                &r.timeline_id,
                r.rel_filenode,
                r.fork,
                r.block,
                request_lsn,
            )
            .await
            .map_err(|e| Status::not_found(e.to_string()))?;
        self.cache.put(
            &r.tenant_id,
            &r.timeline_id,
            r.rel_filenode,
            r.fork,
            r.block,
            lsn,
            &page,
        );
        Ok(Response::new(GetPageResponse { page, lsn }))
    }

    async fn get_status(
        &self,
        _req: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, Status> {
        Ok(Response::new(GetStatusResponse {
            role: "pageserver".into(),
            node_id: self.node_id.clone(),
            is_leader: false,
            leader_addr: String::new(),
            last_lsn: 0,
        }))
    }
}
