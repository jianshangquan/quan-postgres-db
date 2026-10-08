use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::{
    meta_key, page_key, page_prefix, wal_key, ObjectStore, TimelineMeta, PAGE_SIZE,
};

/// Minimal path-style S3 HTTP/1.1 client (objstore / MinIO / path-style AWS).
pub struct S3Store {
    host: String,
    port: u16,
    bucket: String,
}

impl S3Store {
    pub async fn new(
        endpoint: &str,
        _region: &str,
        bucket: &str,
        _access_key: &str,
        _secret_key: &str,
    ) -> anyhow::Result<Self> {
        let ep = endpoint
            .trim()
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .trim_end_matches('/');
        let (host, port) = if let Some((h, p)) = ep.rsplit_once(':') {
            (h.to_string(), p.parse().unwrap_or(80))
        } else {
            (ep.to_string(), 80)
        };
        Ok(Self {
            host,
            port,
            bucket: bucket.to_string(),
        })
    }

    async fn request(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
        extra_query: &str,
    ) -> anyhow::Result<(u16, Vec<u8>)> {
        let mut stream = TcpStream::connect((self.host.as_str(), self.port)).await?;
        let path_q = if extra_query.is_empty() {
            path.to_string()
        } else {
            format!("{path}?{extra_query}")
        };
        let req = format!(
            "{method} {path_q} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n",
            host = self.host,
            port = self.port,
            len = body.len(),
        );
        stream.write_all(req.as_bytes()).await?;
        if !body.is_empty() {
            stream.write_all(body).await?;
        }
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await?;
        let header_end = buf
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .ok_or_else(|| anyhow::anyhow!("bad http response"))?;
        let header = std::str::from_utf8(&buf[..header_end])?;
        let status: u16 = header
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let body = buf[header_end + 4..].to_vec();
        Ok((status, body))
    }

    async fn put(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        let path = format!("/{}/{}", self.bucket, key);
        let (status, _) = self.request("PUT", &path, data, "").await?;
        if !(200..300).contains(&status) {
            anyhow::bail!("put {key}: HTTP {status}");
        }
        Ok(())
    }

    async fn get(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        let path = format!("/{}/{}", self.bucket, key);
        let (status, body) = self.request("GET", &path, &[], "").await?;
        if !(200..300).contains(&status) {
            anyhow::bail!("get {key}: HTTP {status}");
        }
        Ok(body)
    }
}

#[async_trait]
impl ObjectStore for S3Store {
    async fn ensure_bucket(&self) -> anyhow::Result<()> {
        let path = format!("/{}", self.bucket);
        let (status, _) = self.request("HEAD", &path, &[], "").await?;
        if (200..300).contains(&status) {
            return Ok(());
        }
        let (status, _) = self.request("PUT", &path, &[], "").await?;
        if !(200..300).contains(&status) && status != 409 {
            anyhow::bail!("create bucket: HTTP {status}");
        }
        Ok(())
    }

    async fn put_meta(&self, meta: &TimelineMeta) -> anyhow::Result<()> {
        let b = serde_json::to_vec(meta)?;
        self.put(&meta_key(&meta.tenant_id, &meta.timeline_id), &b)
            .await
    }

    async fn get_meta(&self, tenant_id: &str, timeline_id: &str) -> anyhow::Result<TimelineMeta> {
        let b = self.get(&meta_key(tenant_id, timeline_id)).await?;
        Ok(serde_json::from_slice(&b)?)
    }

    async fn put_wal(
        &self,
        tenant_id: &str,
        timeline_id: &str,
        segno: u64,
        data: &[u8],
    ) -> anyhow::Result<()> {
        self.put(&wal_key(tenant_id, timeline_id, segno), data).await
    }

    async fn get_wal(
        &self,
        tenant_id: &str,
        timeline_id: &str,
        segno: u64,
    ) -> anyhow::Result<Vec<u8>> {
        self.get(&wal_key(tenant_id, timeline_id, segno)).await
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
        self.put(
            &page_key(tenant_id, timeline_id, rel, fork, block, lsn),
            page,
        )
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
        self.get(&page_key(tenant_id, timeline_id, rel, fork, block, lsn))
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
        let prefix = page_prefix(tenant_id, timeline_id, rel, fork, block);
        let path = format!("/{}", self.bucket);
        let q = format!("list-type=2&prefix={prefix}");
        let (status, body) = self.request("GET", &path, &[], &q).await?;
        if !(200..300).contains(&status) {
            anyhow::bail!("list: HTTP {status}");
        }
        let text = String::from_utf8_lossy(&body);
        let mut best_lsn = 0u64;
        let mut best_key: Option<String> = None;
        for part in text.split("<Key>").skip(1) {
            let Some(end) = part.find("</Key>") else {
                continue;
            };
            let key = &part[..end];
            let hex = key.rsplit('/').next().unwrap_or("");
            let Ok(lsn) = u64::from_str_radix(hex, 16) else {
                continue;
            };
            if lsn <= request_lsn && lsn >= best_lsn {
                best_lsn = lsn;
                best_key = Some(key.to_string());
            }
        }
        let key = best_key
            .ok_or_else(|| anyhow::anyhow!("page not found at or before LSN {request_lsn}"))?;
        Ok((self.get(&key).await?, best_lsn))
    }
}
