use std::collections::HashMap;
use std::sync::Mutex;

use crate::store::{ObjectStore, TimelineMeta};
use crate::wal;

pub struct Applier<S: ObjectStore> {
    store: S,
    unknown: Mutex<HashMap<String, Vec<u8>>>,
    last_lsn: Mutex<HashMap<String, u64>>,
}

impl<S: ObjectStore> Applier<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            unknown: Mutex::new(HashMap::new()),
            last_lsn: Mutex::new(HashMap::new()),
        }
    }

    fn key(tenant: &str, timeline: &str) -> String {
        format!("{tenant}/{timeline}")
    }

    pub async fn apply(
        &self,
        tenant_id: &str,
        timeline_id: &str,
        _start_lsn: u64,
        end_lsn: u64,
        payload: &[u8],
    ) -> anyhow::Result<u64> {
        let (fps, unknown) = wal::decode_records(payload)?;
        if !unknown.is_empty() {
            let k = Self::key(tenant_id, timeline_id);
            self.unknown
                .lock()
                .unwrap()
                .entry(k)
                .or_default()
                .extend_from_slice(&unknown);
        }
        let mut applied = 0u64;
        for rec in fps {
            self.store
                .put_page(
                    tenant_id,
                    timeline_id,
                    rec.rel_filenode,
                    rec.fork,
                    rec.block,
                    rec.lsn,
                    &rec.page,
                )
                .await?;
            applied = applied.max(rec.lsn);
        }
        applied = applied.max(end_lsn);

        let mut meta = self
            .store
            .get_meta(tenant_id, timeline_id)
            .await
            .unwrap_or(TimelineMeta {
                tenant_id: tenant_id.to_string(),
                timeline_id: timeline_id.to_string(),
                last_lsn: 0,
            });
        if applied > meta.last_lsn {
            meta.last_lsn = applied;
            self.store.put_meta(&meta).await?;
        }
        self.last_lsn
            .lock()
            .unwrap()
            .insert(Self::key(tenant_id, timeline_id), applied);
        Ok(applied)
    }

    pub async fn get_page(
        &self,
        tenant_id: &str,
        timeline_id: &str,
        rel: u32,
        fork: u32,
        block: u32,
        request_lsn: u64,
    ) -> anyhow::Result<(Vec<u8>, u64)> {
        self.store
            .get_page_at_or_before(tenant_id, timeline_id, rel, fork, block, request_lsn)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;
    use crate::wal::{encode_fpi, FpiRecord, PAGE_SIZE};

    #[tokio::test]
    async fn apply_fpi() {
        let store = MemoryStore::new();
        let a = Applier::new(store);
        let page = vec![0x42u8; PAGE_SIZE];
        let payload = encode_fpi(&FpiRecord {
            rel_filenode: 100,
            fork: 0,
            block: 3,
            lsn: 500,
            page: page.clone(),
        })
        .unwrap();
        a.apply("ten", "tl", 400, 500, &payload).await.unwrap();
        let (got, lsn) = a.get_page("ten", "tl", 100, 0, 3, 500).await.unwrap();
        assert_eq!(lsn, 500);
        assert_eq!(got, page);
    }
}
