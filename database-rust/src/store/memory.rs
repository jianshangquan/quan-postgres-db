use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use super::{
    meta_key, page_key, page_prefix, wal_key, ObjectStore, TimelineMeta, PAGE_SIZE,
};

#[derive(Default)]
pub struct MemoryStore {
    data: Mutex<HashMap<String, Vec<u8>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ObjectStore for MemoryStore {
    async fn ensure_bucket(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn put_meta(&self, meta: &TimelineMeta) -> anyhow::Result<()> {
        let b = serde_json::to_vec(meta)?;
        self.data
            .lock()
            .unwrap()
            .insert(meta_key(&meta.tenant_id, &meta.timeline_id), b);
        Ok(())
    }

    async fn get_meta(&self, tenant_id: &str, timeline_id: &str) -> anyhow::Result<TimelineMeta> {
        let guard = self.data.lock().unwrap();
        let b = guard
            .get(&meta_key(tenant_id, timeline_id))
            .ok_or_else(|| anyhow::anyhow!("meta not found"))?;
        Ok(serde_json::from_slice(b)?)
    }

    async fn put_wal(
        &self,
        tenant_id: &str,
        timeline_id: &str,
        segno: u64,
        data: &[u8],
    ) -> anyhow::Result<()> {
        self.data
            .lock()
            .unwrap()
            .insert(wal_key(tenant_id, timeline_id, segno), data.to_vec());
        Ok(())
    }

    async fn get_wal(
        &self,
        tenant_id: &str,
        timeline_id: &str,
        segno: u64,
    ) -> anyhow::Result<Vec<u8>> {
        self.data
            .lock()
            .unwrap()
            .get(&wal_key(tenant_id, timeline_id, segno))
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("wal not found"))
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
        if page.len() != PAGE_SIZE {
            anyhow::bail!("page must be {PAGE_SIZE} bytes");
        }
        self.data.lock().unwrap().insert(
            page_key(tenant_id, timeline_id, rel, fork, block, lsn),
            page.to_vec(),
        );
        Ok(())
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
        self.data
            .lock()
            .unwrap()
            .get(&page_key(tenant_id, timeline_id, rel, fork, block, lsn))
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("page not found"))
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
        let prefix = page_prefix(tenant_id, timeline_id, rel, fork, block);
        let guard = self.data.lock().unwrap();
        let mut best_lsn = 0u64;
        let mut best: Option<Vec<u8>> = None;
        for (k, v) in guard.iter() {
            if !k.starts_with(&prefix) {
                continue;
            }
            let hex = k.rsplit('/').next().unwrap_or("");
            let Ok(lsn) = u64::from_str_radix(hex, 16) else {
                continue;
            };
            if lsn <= request_lsn && lsn >= best_lsn {
                best_lsn = lsn;
                best = Some(v.clone());
            }
        }
        best.map(|p| (p, best_lsn))
            .ok_or_else(|| anyhow::anyhow!("page not found at or before LSN {request_lsn}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn page_at_or_before() {
        let m = MemoryStore::new();
        let p1 = vec![1u8; PAGE_SIZE];
        let p2 = vec![2u8; PAGE_SIZE];
        m.put_page("t", "tl", 1, 0, 0, 10, &p1).await.unwrap();
        m.put_page("t", "tl", 1, 0, 0, 20, &p2).await.unwrap();
        let (got, lsn) = m.get_page_at_or_before("t", "tl", 1, 0, 0, 15).await.unwrap();
        assert_eq!(lsn, 10);
        assert_eq!(got, p1);
    }
}
