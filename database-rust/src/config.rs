use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub role: String,
    pub node_id: String,
    pub listen_addr: String,
    pub raft_addr: String,
    pub http_addr: String,
    pub data_dir: String,
    pub peers: Vec<Peer>,
    pub page_size: usize,
    pub pageserver_addr: String,
    pub s3_endpoint: String,
    pub s3_bucket: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub s3_region: String,
    pub bootstrap: bool,
}

#[derive(Debug, Clone)]
pub struct Peer {
    pub id: String,
    pub raft_addr: String,
    pub grpc_addr: String,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let peers_raw = env::var("SAFEKEEPER_PEERS").unwrap_or_default();
        let peers = parse_peers(&peers_raw)?;
        Ok(Self {
            role: env_or("ROLE", "safekeeper"),
            node_id: env_or("NODE_ID", "sk-1"),
            listen_addr: normalize_addr(env_or("LISTEN_ADDR", ":50051")),
            raft_addr: normalize_addr(env_or("RAFT_ADDR", ":7000")),
            http_addr: normalize_addr(env_or("HTTP_ADDR", ":8080")),
            data_dir: env_or("DATA_DIR", "./data"),
            peers,
            page_size: env_or("PAGE_SIZE", "8192").parse().unwrap_or(8192),
            pageserver_addr: env::var("PAGESERVER_ADDR").unwrap_or_default(),
            s3_endpoint: env_or("S3_ENDPOINT", "http://minio:9000"),
            s3_bucket: env_or("S3_BUCKET", "quan-erp-pages"),
            s3_access_key: env_or("S3_ACCESS_KEY", "minioadmin"),
            s3_secret_key: env_or("S3_SECRET_KEY", "minioadmin"),
            s3_region: env_or("S3_REGION", "us-east-1"),
            bootstrap: env_or("BOOTSTRAP", "false")
                .parse()
                .unwrap_or(false),
        })
    }
}

fn env_or(k: &str, def: &str) -> String {
    env::var(k).unwrap_or_else(|_| def.to_string())
}

fn normalize_addr(a: String) -> String {
    if a.starts_with(':') {
        format!("0.0.0.0{a}")
    } else {
        a
    }
}

/// Parse `id@host:raftPort:grpcPort,...`
pub fn parse_peers(s: &str) -> anyhow::Result<Vec<Peer>> {
    let mut out = Vec::new();
    for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (id, rest) = part
            .split_once('@')
            .ok_or_else(|| anyhow::anyhow!("invalid peer {part}: want id@host:raft:grpc"))?;
        let bits: Vec<&str> = rest.rsplitn(3, ':').collect();
        if bits.len() != 3 {
            anyhow::bail!("invalid peer {part}: want host:raftPort:grpcPort");
        }
        // rsplitn gives [grpc, raft, host...]
        let grpc_port = bits[0];
        let raft_port = bits[1];
        let host = bits[2];
        out.push(Peer {
            id: id.to_string(),
            raft_addr: format!("{host}:{raft_port}"),
            grpc_addr: format!("{host}:{grpc_port}"),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_peer() {
        let p = parse_peers("sk-1@safekeeper-1:7000:50051").unwrap();
        assert_eq!(p[0].id, "sk-1");
        assert_eq!(p[0].raft_addr, "safekeeper-1:7000");
        assert_eq!(p[0].grpc_addr, "safekeeper-1:50051");
    }
}
