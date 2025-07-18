use std::collections::HashMap;
use std::time::{Duration, Instant};

use bytestring::ByteString;
use serde::de::{self, Deserializer};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};

use futures::channel::oneshot::Sender;
use tikv_raft::eraftpb::{ConfChange, Message as RaftMessage};
use tikv_raft::prelude::Snapshot;
use tikv_raft::StateRole;

/// Enumeration representing various types of responses that can be sent back to clients.
#[derive(Serialize, Deserialize, Debug)]
pub enum RaftResponse {
    /// Indicates that the request was sent to the wrong leader.
    WrongLeader {
        leader_id: u64,
        leader_addr: Option<String>,
    },
    /// Indicates that a join request was successful.
    JoinSuccess {
        assigned_id: u64,
        peer_addrs: HashMap<u64, String>,
    },
    /// Contains the leader ID in response to a request for ID.
    RequestId { leader_id: u64 },
    /// Represents an error with a message.
    Error(String),
    /// Too busy
    Busy,
    /// Contains arbitrary response data.
    Response { data: Vec<u8> },
    /// Represents the status of the system.
    Status(Status),
    /// Represents a successful operation.
    Ok,
}

/// Enumeration representing different types of messages that can be sent within the system.
#[allow(dead_code)]
pub enum Message {
    /// A proposal message to be processed.
    Propose {
        proposal: Vec<u8>,
        chan: Sender<RaftResponse>,
    },
    /// A query message to be processed.
    Query {
        query: Vec<u8>,
        chan: Sender<RaftResponse>,
    },
    /// A configuration change message to be processed.
    ConfigChange {
        change: ConfChange,
        chan: Sender<RaftResponse>,
    },
    /// A request for the leader's ID.
    RequestId { chan: Sender<RaftResponse> },
    /// Report that a node is unreachable.
    ReportUnreachable { node_id: u64 },
    /// A Raft message to be processed.
    Raft(Box<RaftMessage>),
    /// A request for the status of the system.
    Status { chan: Sender<RaftResponse> },
    /// Snapshot
    Snapshot { snapshot: Snapshot },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PeerState {
    pub addr: ByteString,
    pub available: bool,
}

/// Struct representing the status of the system.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Status {
    pub id: u64,
    pub leader_id: u64,
    pub uncommitteds: usize,
    pub merger_proposals: usize,
    pub sending_raft_messages: isize,
    pub timeout_max: isize,
    pub timeout_recent_count: isize,
    pub propose_count: isize,
    pub propose_rate: f64,
    pub peers: HashMap<u64, Option<PeerState>>,
    #[serde(
        serialize_with = "Status::serialize_role",
        deserialize_with = "Status::deserialize_role"
    )]
    pub role: StateRole,
}

impl Status {
    #[inline]
    pub fn available(&self) -> bool {
        if matches!(self.role, StateRole::Leader) {
            //Check if the number of available nodes is greater than or equal to half of the total nodes.
            let (all_count, available_count) = self.get_count();
            let available = available_count >= ((all_count / 2) + (all_count % 2));
            log::debug!(
                "is Leader, all_count: {}, available_count: {} {}",
                all_count,
                available_count,
                available
            );
            available
        } else if self.leader_id > 0 {
            //As long as a leader exists and is available, the system considers itself in a normal state.
            let available = self
                .peers
                .get(&self.leader_id)
                .and_then(|p| p.as_ref().map(|p| p.available))
                .unwrap_or_default();
            log::debug!("has Leader, available: {}", available);
            available
        } else {
            //If there is no Leader, it's still necessary to check whether the number of all other
            // available nodes is greater than or equal to half.
            let (all_count, available_count) = self.get_count();
            let available = available_count >= ((all_count / 2) + (all_count % 2));
            log::debug!(
                "no Leader, all_count: {}, available_count: {} {}",
                all_count,
                available_count,
                available
            );
            available
        }
    }

    #[inline]
    fn get_count(&self) -> (usize, usize) {
        let available_count = self
            .peers
            .iter()
            .filter(|(_, p)| if let Some(p) = p { p.available } else { false })
            .count();
        if self.peers.contains_key(&self.id) {
            (self.peers.len() - 1, available_count - 1)
        } else {
            (self.peers.len(), available_count)
        }
    }

    /// Checks if the node has started.
    #[inline]
    pub fn is_started(&self) -> bool {
        self.leader_id > 0
    }

    /// Checks if this node is the leader.
    #[inline]
    pub fn is_leader(&self) -> bool {
        self.leader_id == self.id && matches!(self.role, StateRole::Leader)
    }

    #[inline]
    pub fn deserialize_role<'de, D>(deserializer: D) -> Result<StateRole, D::Error>
    where
        D: Deserializer<'de>,
    {
        let role = match u8::deserialize(deserializer)? {
            1 => StateRole::Follower,
            2 => StateRole::Candidate,
            3 => StateRole::Leader,
            4 => StateRole::PreCandidate,
            _ => return Err(de::Error::missing_field("role")),
        };
        Ok(role)
    }

    #[inline]
    pub fn serialize_role<S>(role: &StateRole, s: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match role {
            StateRole::Follower => 1u8,
            StateRole::Candidate => 2u8,
            StateRole::Leader => 3u8,
            StateRole::PreCandidate => 4u8,
        }
        .serialize(s)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) enum RemoveNodeType {
    Normal,
    Stale,
}

/// Enumeration for reply channels which could be single or multiple.
pub(crate) enum ReplyChan {
    /// Single reply channel with its timestamp.
    One((Sender<RaftResponse>, Instant)),
    /// Multiple reply channels with their timestamps.
    More(Vec<(Sender<RaftResponse>, Instant)>),
}

/// Enumeration for proposals which could be a single proposal or multiple proposals.
#[derive(Serialize, Deserialize)]
pub(crate) enum Proposals {
    /// A single proposal.
    One(Vec<u8>),
    /// Multiple proposals.
    More(Vec<Vec<u8>>),
}

/// A struct to manage proposal batching and sending.
pub(crate) struct Merger {
    proposals: Vec<Vec<u8>>,
    chans: Vec<(Sender<RaftResponse>, Instant)>,
    start_collection_time: i64,
    proposal_batch_size: usize,
    proposal_batch_timeout: i64,
}

impl Merger {
    /// Creates a new `Merger` instance with the specified batch size and timeout.
    ///
    /// # Parameters
    /// - `proposal_batch_size`: The maximum number of proposals to include in a batch.
    /// - `proposal_batch_timeout`: The timeout duration for collecting proposals.
    ///
    /// # Returns
    /// A new `Merger` instance.
    pub fn new(proposal_batch_size: usize, proposal_batch_timeout: Duration) -> Self {
        Self {
            proposals: Vec::new(),
            chans: Vec::new(),
            start_collection_time: 0,
            proposal_batch_size,
            proposal_batch_timeout: proposal_batch_timeout.as_millis() as i64,
        }
    }

    /// Adds a new proposal and its corresponding reply channel to the merger.
    ///
    /// # Parameters
    /// - `proposal`: The proposal data to be added.
    /// - `chan`: The reply channel for the proposal.
    #[inline]
    pub fn add(&mut self, proposal: Vec<u8>, chan: Sender<RaftResponse>) {
        self.proposals.push(proposal);
        self.chans.push((chan, Instant::now()));
    }

    /// Returns the number of proposals currently held by the merger.
    ///
    /// # Returns
    /// The number of proposals.
    #[inline]
    pub fn len(&self) -> usize {
        self.proposals.len()
    }

    /// Retrieves a batch of proposals and their corresponding reply channels if the batch size or timeout criteria are met.
    ///
    /// # Returns
    /// An `Option` containing the proposals and reply channels, or `None` if no batch is ready.
    #[inline]
    pub fn take(&mut self) -> Option<(Proposals, ReplyChan)> {
        let max = self.proposal_batch_size;
        let len = self.len();
        let len = if len > max { max } else { len };
        if len > 0 && (len == max || self.timeout()) {
            let data = if len == 1 {
                match (self.proposals.pop(), self.chans.pop()) {
                    (Some(proposal), Some(chan)) => {
                        Some((Proposals::One(proposal), ReplyChan::One(chan)))
                    }
                    _ => unreachable!(),
                }
            } else {
                let mut proposals = self.proposals.drain(0..len).collect::<Vec<_>>();
                let mut chans = self.chans.drain(0..len).collect::<Vec<_>>();
                proposals.reverse();
                chans.reverse();
                Some((Proposals::More(proposals), ReplyChan::More(chans)))
            };
            self.start_collection_time = chrono::Local::now().timestamp_millis();
            data
        } else {
            None
        }
    }

    #[inline]
    fn timeout(&self) -> bool {
        chrono::Local::now().timestamp_millis()
            > (self.start_collection_time + self.proposal_batch_timeout)
    }
}

#[tokio::test]
async fn test_merger() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut merger = Merger::new(50, Duration::from_millis(200));
    use futures::channel::oneshot::channel;
    use std::time::Duration;

    let add = |merger: &mut Merger| {
        let (tx, rx) = channel();
        merger.add(vec![1, 2, 3], tx);
        rx
    };

    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Arc;
    const MAX: i64 = 111;
    let count = Arc::new(AtomicI64::new(0));
    let mut futs = Vec::new();
    for _ in 0..MAX {
        let rx = add(&mut merger);
        let count1 = count.clone();
        let fut = async move {
            let r = tokio::time::timeout(Duration::from_secs(3), rx).await;
            match r {
                Ok(_) => {}
                Err(_) => {
                    println!("timeout ...");
                }
            }
            count1.fetch_add(1, Ordering::SeqCst);
        };

        futs.push(fut);
    }

    let sends = async {
        loop {
            if let Some((_data, chan)) = merger.take() {
                match chan {
                    ReplyChan::One((tx, _)) => {
                        let _ = tx.send(RaftResponse::Ok);
                    }
                    ReplyChan::More(txs) => {
                        for (tx, _) in txs {
                            let _ = tx.send(RaftResponse::Ok);
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            if merger.len() == 0 {
                break;
            }
        }
    };

    let count_p = count.clone();
    let count_print = async move {
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            println!("count_p: {}", count_p.load(Ordering::SeqCst));
            if count_p.load(Ordering::SeqCst) >= MAX {
                break;
            }
        }
    };
    println!("futs: {}", futs.len());
    futures::future::join3(futures::future::join_all(futs), sends, count_print).await;

    Ok(())
}
