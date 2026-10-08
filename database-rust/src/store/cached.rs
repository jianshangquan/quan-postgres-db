use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_trait::async_trait;
use tokio::fs;

use super::{
    meta_key, page_key, wal_key, ObjectStore, TimelineMeta,
};

pub struct CachedStore<S: ObjectStore> {
    remote: S,
    dir: PathBuf,
    lru: Mutex<(HashMap<String, Vec<u8>>, VecDeque<String>)>,
    max: usize,
}

impl<S: ObjectStore> CachedStore<S> {
    pub fn new(remote: S, dir: impl AsRef<Path>, max: usize) -> Self {
        Self {
            remote,
            dir: dir.as_ref().to_path_buf(),
            lru: Mutex::new((HashMap::new(), VecDeque::new())),
            max: if max == 0 { 256 } else { max },
        }
    }

    fn remember(&self, key: &str, data: &[u8]) {
        let mut g = self.lru.lock().unwrap();
        if !g.0.contains_key(key) {
            g.1.push_back(key.to_string());
        }
        g.0.insert(key.to_string(), data.to_vec());
        while g.1.len() > self.max {
            if let Some(old) = g.1.pop_front() {
                g.0.remove(&old);
            }
        }
    }

    fn from_mem(&self, key: &str) -> Option<Vec<u8>> {
        self.lru.lock().unwrap().0.get(key).cloned()
    }

    fn disk_path(&self, key: &str) -> PathBuf {
        self.dir.join(key)
    }

    async fn write_disk(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        let p = self.disk_path(key);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(p, data).await?;
        Ok(())
    }

    async fn read_disk(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        Ok(fs::read(self.disk_path(key)).await?)
    }

    pub fn clear_memory(&self) {
        let mut g = self.lru.lock().unwrap();
        g.0.clear();
        g.1.clear();
    }
}

#[async_trait]
impl<S: ObjectStore> ObjectStore for CachedStore<S> {
    async fn ensure_bucket(&self) -> anyhow::Result<()> {
        self.remote.ensure_bucket().await
    }

    async fn put_meta(&self, meta: &TimelineMeta) -> anyhow::Result<()> {
        self.remote.put_meta(meta).await?;
        let b = serde_json::to_vec(meta)?;
        let key = meta_key(&meta.tenant_id, &meta.timeline_id);
        let _ = self.write_disk(&key, &b).await;
        self.remember(&key, &b);
        Ok(())
    }

    async fn get_meta(&self, tenant_id: &str, timeline_id: &str) -> anyhow::Result<TimelineMeta> {
        let key = meta_key(tenant_id, timeline_id);
        if let Some(b) = self.from_mem(&key) {
            return Ok(serde_json::from_slice(&b)?);
        }
        if let Ok(b) = self.read_disk(&key).await {
            self.remember(&key, &b);
            return Ok(serde_json::from_slice(&b)?);
        }
        self.remote.get_meta(tenant_id, timeline_id).await
    }

    async fn put_wal(
        &self,
        tenant_id: &str,
        timeline_id: &str,
        segno: u64,
        data: &[u8],
    ) -> anyhow::Result<()> {
        self.remote
            .put_wal(tenant_id, timeline_id, segno, data)
            .await?;
        let key = wal_key(tenant_id, timeline_id, segno);
        let _ = self.write_disk(&key, data).await;
        self.remember(&key, data);
        Ok(())
    }

    async fn get_wal(
        &self,
        tenant_id: &str,
        timeline_id: &str,
        segno: u64,
    ) -> anyhow::Result<Vec<u8>> {
        let key = wal_key(tenant_id, timeline_id, segno);
        if let Some(b) = self.from_mem(&key) {
            return Ok(b);
        }
        if let Ok(b) = self.read_disk(&key).await {
            self.remember(&key, &b);
            return Ok(b);
        }
        let b = self.remote.get_wal(tenant_id, timeline_id, segno).await?;
        let _ = self.write_disk(&key, &b).await;
        self.remember(&key, &b);
        Ok(b)
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
        self.remote
            .put_page(tenant_id, timeline_id, rel, fork, block, lsn, page)
            .await?;
        let key = page_key(tenant_id, timeline_id, rel, fork, block, lsn);
        let _ = self.write_disk(&key, page).await;
        self.remember(&key, page);
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
        let key = page_key(tenant_id, timeline_id, rel, fork, block, lsn);
        if let Some(b) = self.from_mem(&key) {
            return Ok(b);
        }
        if let Ok(b) = self.read_disk(&key).await {
            self.remember(&key, &b);
            return Ok(b);
        }
        let b = self
            .remote
            .get_page(tenant_id, timeline_id, rel, fork, block, lsn)
            .await?;
        let _ = self.write_disk(&key, &b).await;
        self.remember(&key, &b);
        Ok(b)
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
        let (page, lsn) = self
            .remote
            .get_page_at_or_before(tenant_id, timeline_id, rel, fork, block, request_lsn)
            .await?;
        let key = page_key(tenant_id, timeline_id, rel, fork, block, lsn);
        let _ = self.write_disk(&key, &page).await;
        self.remember(&key, &page);
        Ok((page, lsn))
    }
}
