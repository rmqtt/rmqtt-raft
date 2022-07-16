use std::collections::HashMap;
use std::time::Instant;
use futures::channel::oneshot::Sender;
use raft::eraftpb::{ConfChange, Message as RaftMessage};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum RaftResponse {
    WrongLeader {
        leader_id: u64,
        leader_addr: Option<String>,
    },
    JoinSuccess {
        assigned_id: u64,
        peer_addrs: HashMap<u64, String>,
    },
    RequestId {
        leader_id: u64,
    },
    Error(String),
    Response {
        data: Vec<u8>,
    },
    Status(Status),
    Ok,
}

#[allow(dead_code)]
pub enum Message {
    Propose {
        proposal: Vec<u8>,
        chan: Sender<RaftResponse>,
    },
    Query {
        query: Vec<u8>,
        chan: Sender<RaftResponse>,
    },
    ConfigChange {
        change: ConfChange,
        chan: Sender<RaftResponse>,
    },
    RequestId {
        chan: Sender<RaftResponse>,
    },
    ReportUnreachable {
        node_id: u64,
    },
    Raft(Box<RaftMessage>),
    Status {
        chan: Sender<RaftResponse>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Status {
    pub id: u64,
    pub leader_id: u64,
    pub uncommitteds: usize,
    pub active_mailbox_sends: isize,
    pub active_mailbox_querys: isize,
    pub active_send_proposal_grpc_requests: isize,
    pub active_send_message_grpc_requests: isize,
    pub peers: HashMap<u64, String>,
}

impl Status {
    #[inline]
    pub fn is_started(&self) -> bool {
        self.leader_id > 0
    }

    #[inline]
    pub fn is_leader(&self) -> bool {
        self.leader_id == self.id
    }
}

pub(crate) enum ReplyChan {
    One((Sender<RaftResponse>, Instant)),
    More(Vec<(Sender<RaftResponse>, Instant)>),
}

#[derive(Serialize, Deserialize)]
pub(crate) enum Proposals {
    One(Vec<u8>),
    More(Vec<Vec<u8>>),
}


pub(crate) struct Merger {
    proposals: Vec<Vec<u8>>,
    chans: Vec<(Sender<RaftResponse>, Instant)>,
    start_collection_time: i64,
}


impl Merger {
    pub fn new() -> Self {
        Self {
            proposals: Vec::new(),
            chans: Vec::new(),
            start_collection_time: 0,
        }
    }

    #[inline]
    pub fn add(&mut self, proposal: Vec<u8>, chan: Sender<RaftResponse>) {
        self.proposals.push(proposal);
        self.chans.push((chan, Instant::now()));
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.proposals.len()
    }

    #[inline]
    pub fn take(&mut self) -> Option<(Proposals, ReplyChan)> {
        let max = 50; //@TODO configurable, 50
        let len = self.len();
        let len = if len > max { max } else { len };
        if len > 0 && (len == max || self.timeout()) {
            let data = if len == 1 {
                match (self.proposals.pop(), self.chans.pop()) {
                    (Some(proposal), Some(chan)) => {
                        Some((Proposals::One(proposal), ReplyChan::One(chan)))
                    }
                    _ => unreachable!()
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
        chrono::Local::now().timestamp_millis() > (self.start_collection_time + 200)  //@TODO configurable, 200 millisecond
    }
}

#[tokio::test]
async fn test_merger() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut merger = Merger::new();
    use std::time::Duration;
    use futures::channel::oneshot::channel;

    let add = |merger: &mut Merger| {
        let (tx, rx) = channel();
        merger.add(vec![1, 2, 3], tx);
        rx
    };

    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};
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