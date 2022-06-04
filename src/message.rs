use std::collections::HashMap;

use raft::eraftpb::{ConfChange, Message as RaftMessage};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot::Sender;

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
    Error,
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
    One(Sender<RaftResponse>),
    More(Vec<Sender<RaftResponse>>),
}

#[derive(Serialize, Deserialize)]
pub(crate) enum Proposals {
    One(Vec<u8>),
    More(Vec<Vec<u8>>),
}


pub(crate) struct Merger {
    proposals: Vec<Vec<u8>>,
    chans: Vec<Sender<RaftResponse>>,
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
        self.chans.push(chan);
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
