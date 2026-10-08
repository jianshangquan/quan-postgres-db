mod cached;
mod memory;
mod s3;

pub use cached::CachedStore;
pub use memory::MemoryStore;
pub use s3::S3Store;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub const PAGE_SIZE: usize = 8192;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineMeta {
    pub tenant_id: String,
    pub timeline_id: String,
    pub last_lsn: u64,
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn ensure_bucket(&self) -> anyhow::Result<()>;
    async fn put_meta(&self, meta: &TimelineMeta) -> anyhow::Result<()>;
    async fn get_meta(&self, tenant_id: &str, timeline_id: &str) -> anyhow::Result<TimelineMeta>;
    async fn put_wal(
        &self,
        tenant_id: &str,
        timeline_id: &str,
        segno: u64,
        data: &[u8],
    ) -> anyhow::Result<()>;
    async fn get_wal(
        &self,
        tenant_id: &str,
        timeline_id: &str,
        segno: u64,
    ) -> anyhow::Result<Vec<u8>>;
    async fn put_page(
        &self,
        tenant_id: &str,
        timeline_id: &str,
        rel: u32,
        fork: u32,
        block: u32,
        lsn: u64,
        page: &[u8],
    ) -> anyhow::Result<()>;
    async fn get_page(
        &self,
        tenant_id: &str,
        timeline_id: &str,
        rel: u32,
        fork: u32,
        block: u32,
        lsn: u64,
    ) -> anyhow::Result<Vec<u8>>;
    async fn get_page_at_or_before(
        &self,
        tenant_id: &str,
        timeline_id: &str,
        rel: u32,
        fork: u32,
        block: u32,
        request_lsn: u64,
    ) -> anyhow::Result<(Vec<u8>, u64)>;
}

pub fn wal_key(tenant_id: &str, timeline_id: &str, segno: u64) -> String {
    format!("tenants/{tenant_id}/timelines/{timeline_id}/wal/{segno:016x}")
}

pub fn page_key(
    tenant_id: &str,
    timeline_id: &str,
    rel: u32,
    fork: u32,
    block: u32,
    lsn: u64,
) -> String {
    format!("tenants/{tenant_id}/timelines/{timeline_id}/layers/{rel}/{fork}/{block}/{lsn:016x}")
}

pub fn page_prefix(tenant_id: &str, timeline_id: &str, rel: u32, fork: u32, block: u32) -> String {
    format!("tenants/{tenant_id}/timelines/{timeline_id}/layers/{rel}/{fork}/{block}/")
}

pub fn meta_key(tenant_id: &str, timeline_id: &str) -> String {
    format!("tenants/{tenant_id}/timelines/{timeline_id}/meta.json")
}

pub fn segno_from_lsn(lsn: u64) -> u64 {
    const SEG: u64 = 16 * 1024 * 1024;
    if lsn == 0 {
        0
    } else {
        lsn / SEG
    }
}

#[async_trait]
impl<T: ObjectStore + ?Sized> ObjectStore for std::sync::Arc<T> {
    async fn ensure_bucket(&self) -> anyhow::Result<()> {
        (**self).ensure_bucket().await
    }
    async fn put_meta(&self, meta: &TimelineMeta) -> anyhow::Result<()> {
        (**self).put_meta(meta).await
    }
    async fn get_meta(&self, tenant_id: &str, timeline_id: &str) -> anyhow::Result<TimelineMeta> {
        (**self).get_meta(tenant_id, timeline_id).await
    }
    async fn put_wal(
        &self,
        tenant_id: &str,
        timeline_id: &str,
        segno: u64,
        data: &[u8],
    ) -> anyhow::Result<()> {
        (**self).put_wal(tenant_id, timeline_id, segno, data).await
    }
    async fn get_wal(
        &self,
        tenant_id: &str,
        timeline_id: &str,
        segno: u64,
    ) -> anyhow::Result<Vec<u8>> {
        (**self).get_wal(tenant_id, timeline_id, segno).await
    }
    async fn put_page(
        &self,
        tenant_id: &str,
        timeline_id: &str,
        rel: u32,
        fork: u32,
        block: u32,
        lsn: u64,
        page: &[u8],
    ) -> anyhow::Result<()> {
        (**self)
            .put_page(tenant_id, timeline_id, rel, fork, block, lsn, page)
            .await
    }
    async fn get_page(
        &self,
        tenant_id: &str,
        timeline_id: &str,
        rel: u32,
        fork: u32,
        block: u32,
        lsn: u64,
    ) -> anyhow::Result<Vec<u8>> {
        (**self)
            .get_page(tenant_id, timeline_id, rel, fork, block, lsn)
            .await
    }
    async fn get_page_at_or_before(
        &self,
        tenant_id: &str,
        timeline_id: &str,
        rel: u32,
        fork: u32,
        block: u32,
        request_lsn: u64,
    ) -> anyhow::Result<(Vec<u8>, u64)> {
        (**self)
            .get_page_at_or_before(tenant_id, timeline_id, rel, fork, block, request_lsn)
            .await
    }
}
