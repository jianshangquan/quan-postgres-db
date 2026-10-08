use std::time::{Duration, Instant};

use quan_erp_database::pb::storage_client::StorageClient;
use quan_erp_database::pb::*;
use quan_erp_database::wal::{encode_fpi, FpiRecord, PAGE_SIZE};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let seeds = std::env::var("SAFEKEEPER")
        .unwrap_or_else(|_| "127.0.0.1:25051,127.0.0.1:25052,127.0.0.1:25053".into());
    let pageserver =
        std::env::var("PAGESERVER").unwrap_or_else(|_| "127.0.0.1:25061".into());

    let seeds: Vec<String> = seeds.split(',').map(|s| s.trim().to_string()).collect();
    let (mut sk, leader) = find_leader(&seeds).await?;
    println!("leader={leader}");

    sk.create_timeline(CreateTimelineRequest {
        tenant_id: "smoke".into(),
        timeline_id: "t1".into(),
    })
    .await?;

    let page = vec![0x7Eu8; PAGE_SIZE];
    let payload = encode_fpi(&FpiRecord {
        rel_filenode: 42,
        fork: 0,
        block: 7,
        lsn: 1000,
        page: page.clone(),
    })?;
    let resp = sk
        .append_wal(AppendWalRequest {
            tenant_id: "smoke".into(),
            timeline_id: "t1".into(),
            start_lsn: 900,
            end_lsn: 1000,
            payload,
        })
        .await?
        .into_inner();
    if resp.ack_lsn != 1000 {
        anyhow::bail!("ack_lsn={}", resp.ack_lsn);
    }

    let endpoint = format!("http://{pageserver}");
    let channel = tonic::transport::Channel::from_shared(endpoint)?.connect().await?;
    let mut ps = StorageClient::new(channel);
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        match ps
            .get_page(GetPageRequest {
                tenant_id: "smoke".into(),
                timeline_id: "t1".into(),
                rel_filenode: 42,
                fork: 0,
                block: 7,
                request_lsn: 1000,
            })
            .await
        {
            Ok(r) => {
                let r = r.into_inner();
                if r.page == page {
                    println!("GetPage OK lsn={}", r.lsn);
                    return Ok(());
                }
            }
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    anyhow::bail!("page mismatch or timeout")
}

async fn find_leader(
    seeds: &[String],
) -> anyhow::Result<(StorageClient<tonic::transport::Channel>, String)> {
    let deadline = Instant::now() + Duration::from_secs(45);
    while Instant::now() < deadline {
        for addr in seeds {
            let endpoint = format!("http://{addr}");
            if let Ok(channel) = tonic::transport::Channel::from_shared(endpoint.clone())?
                .connect()
                .await
            {
                let mut cli = StorageClient::new(channel);
                if let Ok(st) = cli.get_status(GetStatusRequest {}).await {
                    if st.into_inner().is_leader {
                        let channel = tonic::transport::Channel::from_shared(endpoint)?
                            .connect()
                            .await?;
                        return Ok((StorageClient::new(channel), addr.clone()));
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    anyhow::bail!("no leader among {seeds:?}")
}
