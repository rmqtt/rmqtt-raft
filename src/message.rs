use std::collections::HashMap;
use std::time::{Duration, Instant};

use futures::channel::oneshot::Sender;
use serde::{Deserialize, Serialize};
use tikv_raft::eraftpb::{ConfChange, Message as RaftMessage};

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
}

/// Struct representing the status of the system.
#[derive(Serialize, Deserialize, Debug)]
pub struct Status {
    pub id: u64,
    pub leader_id: u64,
    pub uncommitteds: usize,
    pub merger_proposals: usize,
    pub sending_raft_messages: isize,
    pub peers: HashMap<u64, String>,
}

impl Status {
    /// Checks if the node has started.
    #[inline]
    pub fn is_started(&self) -> bool {
        self.leader_id > 0
    }

    /// Checks if this node is the leader.
    #[inline]
    pub fn is_leader(&self) -> bool {
        self.leader_id == self.id
    }
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
