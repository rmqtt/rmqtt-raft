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
    /// Report the transport result of a snapshot to the Raft progress tracker.
    ReportSnapshot { node_id: u64, success: bool },
    /// A Raft message to be processed.
    Raft(Box<RaftMessage>),
    /// A request for the status of the system.
    Status { chan: Sender<RaftResponse> },
    /// Requests the current leader to transfer leadership to another voter.
    TransferLeader {
        node_id: u64,
        chan: Sender<RaftResponse>,
    },
    /// Snapshot
    Snapshot { snapshot: Snapshot },
    /// Gracefully stops the local Raft event loop.
    Shutdown { chan: Sender<RaftResponse> },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PeerState {
    pub addr: ByteString,
    pub available: bool,
}

/// Replication progress as observed by the leader for one Raft peer.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct PeerReplicationState {
    pub matched: u64,
    pub next_index: u64,
    pub committed_index: u64,
    pub pending_snapshot: u64,
    pub recent_active: bool,
    pub paused: bool,
    pub state: String,
}

/// Struct representing the status of the system.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Status {
    pub id: u64,
    pub leader_id: u64,
    pub applied_index: u64,
    pub committed_index: u64,
    pub last_index: u64,
    pub snapshot_restored: bool,
    pub snapshot_index: u64,
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub peer_replication: HashMap<u64, PeerReplicationState>,
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
    /// Returns true once the local state machine has restored its initial
    /// snapshot and applied every log entry committed on this node.
    #[inline]
    pub fn local_caught_up(&self) -> bool {
        self.snapshot_restored && self.applied_index >= self.committed_index
    }

    #[inline]
    pub fn is_voter(&self, id: u64) -> bool {
        self.voters.contains(&id)
    }

    #[inline]
    pub fn is_learner(&self, id: u64) -> bool {
        self.learners.contains(&id)
    }

    /// Returns true when the leader has observed a recently active peer whose
    /// replicated log has reached the leader's current committed index.
    #[inline]
    pub fn peer_caught_up(&self, id: u64) -> bool {
        self.is_leader()
            && self.peer_replication.get(&id).is_some_and(|progress| {
                progress.recent_active
                    && progress.pending_snapshot == 0
                    && progress.matched >= self.committed_index
            })
    }

    #[inline]
    pub fn available(&self) -> bool {
        if matches!(self.role, StateRole::Leader) {
            //Check if the number of available nodes is greater than or equal to half of the total nodes.
            let (all_count, available_count) = self.get_count();
            let available = all_count > 0 && available_count >= ((all_count / 2) + 1);
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
            let available = all_count > 0 && available_count >= ((all_count / 2) + 1);
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
            .voters
            .iter()
            .filter(|&&id| {
                id == self.id
                    || self
                        .peers
                        .get(&id)
                        .and_then(|peer| peer.as_ref())
                        .is_some_and(|peer| peer.available)
            })
            .count();
        (self.voters.len(), available_count)
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

#[cfg(test)]
mod status_tests {
    use super::*;

    fn leader_status() -> Status {
        Status {
            id: 1,
            leader_id: 1,
            applied_index: 10,
            committed_index: 10,
            last_index: 10,
            snapshot_restored: true,
            snapshot_index: 8,
            voters: vec![1, 2, 3],
            learners: vec![4],
            peer_replication: HashMap::new(),
            uncommitteds: 0,
            merger_proposals: 0,
            sending_raft_messages: 0,
            timeout_max: 0,
            timeout_recent_count: 0,
            propose_count: 0,
            propose_rate: 0.0,
            peers: HashMap::from([
                (
                    2,
                    Some(PeerState {
                        addr: ByteString::from("node-2"),
                        available: true,
                    }),
                ),
                (
                    3,
                    Some(PeerState {
                        addr: ByteString::from("node-3"),
                        available: false,
                    }),
                ),
                (
                    4,
                    Some(PeerState {
                        addr: ByteString::from("node-4"),
                        available: true,
                    }),
                ),
            ]),
            role: StateRole::Leader,
        }
    }

    #[test]
    fn availability_uses_voter_quorum_and_ignores_learners() {
        let mut status = leader_status();
        assert!(status.available());

        status
            .peers
            .get_mut(&2)
            .unwrap()
            .as_mut()
            .unwrap()
            .available = false;
        assert!(!status.available());
    }

    #[test]
    fn caught_up_requires_restored_snapshot_and_finished_snapshot_transfer() {
        let mut status = leader_status();
        assert!(status.local_caught_up());

        status.peer_replication.insert(
            4,
            PeerReplicationState {
                matched: 10,
                pending_snapshot: 8,
                recent_active: true,
                ..Default::default()
            },
        );
        assert!(!status.peer_caught_up(4));

        status
            .peer_replication
            .get_mut(&4)
            .unwrap()
            .pending_snapshot = 0;
        assert!(status.peer_caught_up(4));
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
