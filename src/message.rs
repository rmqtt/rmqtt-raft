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
