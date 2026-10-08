use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

#[derive(Clone, Hash, Eq, PartialEq)]
struct Key {
    tenant: String,
    timeline: String,
    rel: u32,
    fork: u32,
    block: u32,
}

struct Entry {
    lsn: u64,
    page: Vec<u8>,
}

pub struct PageCache {
    items: Mutex<(HashMap<Key, Entry>, VecDeque<Key>)>,
    max: usize,
}

impl PageCache {
    pub fn new(max: usize) -> Self {
        Self {
            items: Mutex::new((HashMap::new(), VecDeque::new())),
            max: if max == 0 { 128 } else { max },
        }
    }

    pub fn put(
        &self,
        tenant: &str,
        timeline: &str,
        rel: u32,
        fork: u32,
        block: u32,
        lsn: u64,
        page: &[u8],
    ) {
        let k = Key {
            tenant: tenant.into(),
            timeline: timeline.into(),
            rel,
            fork,
            block,
        };
        let mut g = self.items.lock().unwrap();
        if !g.0.contains_key(&k) {
            g.1.push_back(k.clone());
        }
        g.0.insert(
            k,
            Entry {
                lsn,
                page: page.to_vec(),
            },
        );
        while g.1.len() > self.max {
            if let Some(old) = g.1.pop_front() {
                g.0.remove(&old);
            }
        }
    }

    pub fn get(
        &self,
        tenant: &str,
        timeline: &str,
        rel: u32,
        fork: u32,
        block: u32,
        request_lsn: u64,
    ) -> Option<(Vec<u8>, u64)> {
        let k = Key {
            tenant: tenant.into(),
            timeline: timeline.into(),
            rel,
            fork,
            block,
        };
        let g = self.items.lock().unwrap();
        let e = g.0.get(&k)?;
        if e.lsn > request_lsn {
            return None;
        }
        Some((e.page.clone(), e.lsn))
    }
}
