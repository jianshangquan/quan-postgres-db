use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Notify, RwLock};
use tonic::transport::Channel;
use tracing::{info, warn};

use crate::config::Peer;
use crate::pb::raft_client::RaftClient;
use crate::pb::{
    AppendEntriesRequest, AppendEntriesResponse, LogEntry as PbLogEntry, RequestVoteRequest,
    RequestVoteResponse,
};
use crate::store::{segno_from_lsn, ObjectStore, TimelineMeta};
use crate::wal::{encode_frame, Frame};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    CreateTimeline {
        tenant_id: String,
        timeline_id: String,
    },
    AppendWal {
        tenant_id: String,
        timeline_id: String,
        start_lsn: u64,
        end_lsn: u64,
        payload: Vec<u8>,
    },
}

#[derive(Debug, Clone)]
struct LogEntry {
    index: u64,
    term: u64,
    data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone)]
pub struct AppliedWal {
    pub tenant_id: String,
    pub timeline_id: String,
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub payload: Vec<u8>,
}

pub struct RaftNode<S: ObjectStore> {
    pub node_id: String,
    pub grpc_addr: String,
    peers: Vec<Peer>,
    store: Arc<S>,
    inner: RwLock<State>,
    apply_notify: Notify,
    on_apply: Mutex<Option<Arc<dyn Fn(AppliedWal) + Send + Sync>>>,
}

struct State {
    role: Role,
    current_term: u64,
    voted_for: Option<String>,
    log: Vec<LogEntry>, // index 0 unused; entries start at 1
    commit_index: u64,
    last_applied: u64,
    leader_id: Option<String>,
    leader_grpc: Option<String>,
    last_lsn: u64,
    next_index: HashMap<String, u64>,
    match_index: HashMap<String, u64>,
    election_deadline: tokio::time::Instant,
}

impl<S: ObjectStore + 'static> RaftNode<S> {
    pub fn new(node_id: String, grpc_addr: String, peers: Vec<Peer>, store: Arc<S>) -> Arc<Self> {
        let mut log = Vec::new();
        log.push(LogEntry {
            index: 0,
            term: 0,
            data: Vec::new(),
        });
        Arc::new(Self {
            node_id,
            grpc_addr,
            peers,
            store,
            inner: RwLock::new(State {
                role: Role::Follower,
                current_term: 0,
                voted_for: None,
                log,
                commit_index: 0,
                last_applied: 0,
                leader_id: None,
                leader_grpc: None,
                last_lsn: 0,
                next_index: HashMap::new(),
                match_index: HashMap::new(),
                election_deadline: tokio::time::Instant::now() + random_election_timeout(),
            }),
            apply_notify: Notify::new(),
            on_apply: Mutex::new(None),
        })
    }

    pub async fn set_on_apply<F>(&self, f: F)
    where
        F: Fn(AppliedWal) + Send + Sync + 'static,
    {
        *self.on_apply.lock().await = Some(Arc::new(f));
    }

    pub async fn is_leader(&self) -> bool {
        self.inner.read().await.role == Role::Leader
    }

    pub async fn leader_grpc_addr(&self) -> String {
        self.inner
            .read()
            .await
            .leader_grpc
            .clone()
            .unwrap_or_default()
    }

    pub async fn last_lsn(&self) -> u64 {
        self.inner.read().await.last_lsn
    }

    pub async fn status_role(&self) -> (bool, String, u64) {
        let s = self.inner.read().await;
        (
            s.role == Role::Leader,
            s.leader_grpc.clone().unwrap_or_default(),
            s.last_lsn,
        )
    }

    pub fn spawn_background(self: &Arc<Self>) {
        let n = Arc::clone(self);
        tokio::spawn(async move { n.election_loop().await });
        let n = Arc::clone(self);
        tokio::spawn(async move { n.apply_loop().await });
    }

    async fn election_loop(self: Arc<Self>) {
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        loop {
            tick.tick().await;
            let now = tokio::time::Instant::now();
            let (role, due) = {
                let s = self.inner.read().await;
                (s.role, s.election_deadline)
            };
            match role {
                Role::Leader => {
                    self.broadcast_heartbeat().await;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Role::Follower | Role::Candidate => {
                    if now >= due {
                        self.start_election().await;
                    }
                }
            }
        }
    }

    async fn apply_loop(self: Arc<Self>) {
        loop {
            self.apply_notify.notified().await;
            loop {
                let (idx, data) = {
                    let mut s = self.inner.write().await;
                    if s.last_applied >= s.commit_index {
                        break;
                    }
                    s.last_applied += 1;
                    let e = s.log[s.last_applied as usize].clone();
                    (e.index, e.data)
                };
                if let Err(e) = self.apply_entry(idx, &data).await {
                    warn!("apply entry {idx}: {e:#}");
                }
            }
        }
    }

    async fn apply_entry(&self, _index: u64, data: &[u8]) -> anyhow::Result<()> {
        let cmd: Command = serde_json::from_slice(data)?;
        match cmd {
            Command::CreateTimeline {
                tenant_id,
                timeline_id,
            } => {
                self.store
                    .put_meta(&TimelineMeta {
                        tenant_id,
                        timeline_id,
                        last_lsn: 0,
                    })
                    .await?;
            }
            Command::AppendWal {
                tenant_id,
                timeline_id,
                start_lsn,
                end_lsn,
                payload,
            } => {
                let frame = encode_frame(&Frame {
                    tenant_id: tenant_id.clone(),
                    timeline_id: timeline_id.clone(),
                    start_lsn,
                    end_lsn,
                    payload: payload.clone(),
                });
                let segno = segno_from_lsn(end_lsn);
                self.store
                    .put_wal(&tenant_id, &timeline_id, segno, &frame)
                    .await?;
                let mut meta = self
                    .store
                    .get_meta(&tenant_id, &timeline_id)
                    .await
                    .unwrap_or(TimelineMeta {
                        tenant_id: tenant_id.clone(),
                        timeline_id: timeline_id.clone(),
                        last_lsn: 0,
                    });
                if end_lsn > meta.last_lsn {
                    meta.last_lsn = end_lsn;
                    self.store.put_meta(&meta).await?;
                }
                {
                    let mut s = self.inner.write().await;
                    if end_lsn > s.last_lsn {
                        s.last_lsn = end_lsn;
                    }
                }
                let is_leader = self.inner.read().await.role == Role::Leader;
                if is_leader {
                    if let Some(cb) = self.on_apply.lock().await.as_ref() {
                        cb(AppliedWal {
                            tenant_id,
                            timeline_id,
                            start_lsn,
                            end_lsn,
                            payload,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    async fn start_election(&self) {
        let (term, last_idx, last_term) = {
            let mut s = self.inner.write().await;
            s.role = Role::Candidate;
            s.current_term += 1;
            s.voted_for = Some(self.node_id.clone());
            s.leader_id = None;
            s.election_deadline = tokio::time::Instant::now() + random_election_timeout();
            let last = s.log.last().unwrap();
            (s.current_term, last.index, last.term)
        };
        info!(node = %self.node_id, term, "starting election");
        let mut votes = 1u32;
        let majority = (self.peers.len() as u32 / 2) + 1;
        let mut handles = Vec::new();
        for p in &self.peers {
            if p.id == self.node_id {
                continue;
            }
            // Raft RPCs share the gRPC Storage listen port (grpc_addr).
            let addr = p.grpc_addr.clone();
            let candidate = self.node_id.clone();
            handles.push(tokio::spawn(async move {
                request_vote_remote(&addr, term, &candidate, last_idx, last_term).await
            }));
        }
        for h in handles {
            if let Ok(Ok(resp)) = h.await {
                let mut s = self.inner.write().await;
                if resp.term > s.current_term {
                    s.current_term = resp.term;
                    s.role = Role::Follower;
                    s.voted_for = None;
                    return;
                }
                if resp.vote_granted {
                    votes += 1;
                }
            }
        }
        if votes >= majority {
            self.become_leader().await;
        }
    }

    async fn become_leader(&self) {
        {
            let mut s = self.inner.write().await;
            s.role = Role::Leader;
            s.leader_id = Some(self.node_id.clone());
            s.leader_grpc = Some(self.grpc_addr.clone());
            let last = s.log.last().unwrap().index;
            s.next_index.clear();
            s.match_index.clear();
            for p in &self.peers {
                if p.id != self.node_id {
                    s.next_index.insert(p.id.clone(), last + 1);
                    s.match_index.insert(p.id.clone(), 0);
                }
            }
            info!(node = %self.node_id, term = s.current_term, "became leader");
        }
        self.broadcast_heartbeat().await;
    }

    async fn broadcast_heartbeat(&self) {
        let peers: Vec<_> = self
            .peers
            .iter()
            .filter(|p| p.id != self.node_id)
            .cloned()
            .collect();
        for p in peers {
            let _ = self.replicate_to(&p).await;
        }
    }

    async fn replicate_to(&self, peer: &Peer) -> anyhow::Result<()> {
        let (req, term) = {
            let s = self.inner.read().await;
            if s.role != Role::Leader {
                return Ok(());
            }
            let next = *s.next_index.get(&peer.id).unwrap_or(&1);
            let prev_idx = next.saturating_sub(1);
            let prev_term = s.log.get(prev_idx as usize).map(|e| e.term).unwrap_or(0);
            let entries: Vec<PbLogEntry> = s.log[next as usize..]
                .iter()
                .map(|e| PbLogEntry {
                    index: e.index,
                    term: e.term,
                    data: e.data.clone(),
                })
                .collect();
            (
                AppendEntriesRequest {
                    term: s.current_term,
                    leader_id: self.node_id.clone(),
                    prev_log_index: prev_idx,
                    prev_log_term: prev_term,
                    entries,
                    leader_commit: s.commit_index,
                    leader_grpc_addr: self.grpc_addr.clone(),
                },
                s.current_term,
            )
        };
        let resp = append_entries_remote(&peer.grpc_addr, req).await?;
        let mut s = self.inner.write().await;
        if resp.term > s.current_term {
            s.current_term = resp.term;
            s.role = Role::Follower;
            s.voted_for = None;
            s.leader_id = None;
            return Ok(());
        }
        if s.role != Role::Leader || term != s.current_term {
            return Ok(());
        }
        if resp.success {
            s.next_index.insert(peer.id.clone(), resp.match_index + 1);
            s.match_index.insert(peer.id.clone(), resp.match_index);
            self.update_commit(&mut s);
        } else {
            let next = s.next_index.entry(peer.id.clone()).or_insert(1);
            *next = (*next).saturating_sub(1).max(1);
        }
        Ok(())
    }

    fn update_commit(&self, s: &mut State) {
        let last = s.log.last().unwrap().index;
        for n in (s.commit_index + 1)..=last {
            if s.log[n as usize].term != s.current_term {
                continue;
            }
            let mut count = 1u32; // self
            for (_id, mi) in &s.match_index {
                if *mi >= n {
                    count += 1;
                }
            }
            let majority = (self.peers.len() as u32 / 2) + 1;
            if count >= majority {
                s.commit_index = n;
                self.apply_notify.notify_one();
            }
        }
    }

    pub async fn propose(&self, cmd: Command) -> anyhow::Result<u64> {
        let data = serde_json::to_vec(&cmd)?;
        let end_lsn = match &cmd {
            Command::AppendWal { end_lsn, .. } => *end_lsn,
            _ => 0,
        };
        {
            let mut s = self.inner.write().await;
            if s.role != Role::Leader {
                anyhow::bail!("not leader");
            }
            let index = s.log.last().unwrap().index + 1;
            let term = s.current_term;
            s.log.push(LogEntry {
                index,
                term,
                data,
            });
            // count self as matched
            // commit may need peer acks
        }
        // replicate to peers and wait for commit of this entry
        let target_index = {
            let s = self.inner.read().await;
            s.log.last().unwrap().index
        };
        let peers: Vec<_> = self
            .peers
            .iter()
            .filter(|p| p.id != self.node_id)
            .cloned()
            .collect();
        for p in &peers {
            let _ = self.replicate_to(p).await;
        }
        // If single-node cluster, commit immediately
        {
            let mut s = self.inner.write().await;
            if self.peers.len() <= 1 {
                s.commit_index = target_index;
                self.apply_notify.notify_one();
            } else {
                // self match
                self.update_commit(&mut s);
            }
        }
        // wait until applied
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            {
                let s = self.inner.read().await;
                if s.last_applied >= target_index {
                    return Ok(if end_lsn == 0 { target_index } else { end_lsn });
                }
                if s.role != Role::Leader {
                    anyhow::bail!("lost leadership");
                }
            }
            // retry replicate
            for p in &peers {
                let _ = self.replicate_to(p).await;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        anyhow::bail!("propose timeout")
    }

    // --- Raft RPC handlers ---

    pub async fn handle_request_vote(&self, req: RequestVoteRequest) -> RequestVoteResponse {
        let mut s = self.inner.write().await;
        if req.term > s.current_term {
            s.current_term = req.term;
            s.role = Role::Follower;
            s.voted_for = None;
            s.leader_id = None;
        }
        let last = s.log.last().unwrap();
        let log_ok = req.last_log_term > last.term
            || (req.last_log_term == last.term && req.last_log_index >= last.index);
        let can_vote = s.voted_for.as_ref().map(|v| v == &req.candidate_id).unwrap_or(true);
        let grant = req.term >= s.current_term && can_vote && log_ok;
        if grant {
            s.voted_for = Some(req.candidate_id);
            s.election_deadline = tokio::time::Instant::now() + random_election_timeout();
        }
        RequestVoteResponse {
            term: s.current_term,
            vote_granted: grant,
        }
    }

    pub async fn handle_append_entries(&self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        let mut s = self.inner.write().await;
        if req.term < s.current_term {
            return AppendEntriesResponse {
                term: s.current_term,
                success: false,
                match_index: 0,
            };
        }
        if req.term > s.current_term {
            s.current_term = req.term;
            s.voted_for = None;
        }
        s.role = Role::Follower;
        s.leader_id = Some(req.leader_id.clone());
        s.leader_grpc = Some(req.leader_grpc_addr.clone());
        s.election_deadline = tokio::time::Instant::now() + random_election_timeout();

        if req.prev_log_index > 0 {
            match s.log.get(req.prev_log_index as usize) {
                Some(e) if e.term == req.prev_log_term => {}
                _ => {
                    return AppendEntriesResponse {
                        term: s.current_term,
                        success: false,
                        match_index: 0,
                    };
                }
            }
        }
        // truncate conflicting and append
        let mut idx = req.prev_log_index + 1;
        for e in &req.entries {
            if let Some(existing) = s.log.get(idx as usize) {
                if existing.term != e.term {
                    s.log.truncate(idx as usize);
                    s.log.push(LogEntry {
                        index: e.index,
                        term: e.term,
                        data: e.data.clone(),
                    });
                }
            } else {
                s.log.push(LogEntry {
                    index: e.index,
                    term: e.term,
                    data: e.data.clone(),
                });
            }
            idx = e.index + 1;
        }
        if req.leader_commit > s.commit_index {
            let last = s.log.last().unwrap().index;
            s.commit_index = req.leader_commit.min(last);
            self.apply_notify.notify_one();
        }
        let match_index = s.log.last().unwrap().index;
        AppendEntriesResponse {
            term: s.current_term,
            success: true,
            match_index,
        }
    }
}

fn random_election_timeout() -> Duration {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    Duration::from_millis(150 + (nanos % 150) as u64)
}

async fn raft_channel(addr: &str) -> anyhow::Result<Channel> {
    let endpoint = if addr.starts_with("http") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    };
    Ok(Channel::from_shared(endpoint)?.connect().await?)
}

async fn request_vote_remote(
    addr: &str,
    term: u64,
    candidate_id: &str,
    last_log_index: u64,
    last_log_term: u64,
) -> anyhow::Result<RequestVoteResponse> {
    let ch = raft_channel(addr).await?;
    let mut cli = RaftClient::new(ch);
    let resp = cli
        .request_vote(RequestVoteRequest {
            term,
            candidate_id: candidate_id.to_string(),
            last_log_index,
            last_log_term,
        })
        .await?
        .into_inner();
    Ok(resp)
}

async fn append_entries_remote(
    addr: &str,
    req: AppendEntriesRequest,
) -> anyhow::Result<AppendEntriesResponse> {
    let ch = raft_channel(addr).await?;
    let mut cli = RaftClient::new(ch);
    Ok(cli.append_entries(req).await?.into_inner())
}
