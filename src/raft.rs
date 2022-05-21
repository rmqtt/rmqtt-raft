use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bincode::{deserialize, serialize};
use futures::future::FutureExt;
use log::{debug, error, info, warn};
use raft::eraftpb::{ConfChange, ConfChangeType};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tonic::Request;

use crate::error::{Error, Result};
use crate::message::{Message, RaftResponse, Status};
use crate::raft_node::{Peer, RaftNode};
use crate::raft_server::RaftServer;
use crate::raft_service::{ConfChange as RiteraftConfChange, Empty, ResultCode};
use crate::raft_service::Proposal;
use crate::raft_service::raft_service_client::RaftServiceClient;

type DashMap<K, V> = dashmap::DashMap<K, V, ahash::RandomState>;

pub type RaftGrpcClient = RaftServiceClient<tonic::transport::channel::Channel>;

#[async_trait]
pub trait Store {
    async fn apply(&mut self, message: &[u8]) -> Result<Vec<u8>>;
    async fn query(&self, query: &[u8]) -> Result<Vec<u8>>;
    async fn snapshot(&self) -> Result<Vec<u8>>;
    async fn restore(&mut self, snapshot: &[u8]) -> Result<()>;
}

struct ProposalSender {
    proposal: Vec<u8>,
    client: RaftGrpcClient,
    //chan: oneshot::Sender<RaftResponse>,
    max_retries: usize,
    timeout: Duration,
}

impl ProposalSender {
    async fn send(mut self) -> Result<RaftResponse> {
        let mut current_retry = 0usize;
        loop {
            let message_request = Request::new(Proposal {
                inner: self.proposal.clone(),
            });
            match self.client.forward(message_request).await {
                Ok(grpc_response) => {
                    let raft_response: RaftResponse =
                        deserialize(&grpc_response.into_inner().inner).expect("deserialize error");
                    //let _ = self.chan.send(raft_response);
                    return Ok(raft_response);
                }
                Err(e) => {
                    if current_retry < self.max_retries {
                        current_retry += 1;
                        tokio::time::sleep(self.timeout).await;
                    } else {
                        error!(
                            "error sending proposal after {} retries: {}",
                            self.max_retries, e
                        );
                        //let _ = self.chan.send(RaftResponse::Error);
                        return Err(Error::RemoteCall(e));
                    }
                }
            }
        }
    }
}

/// A mailbox to send messages to a ruung raft node.
#[derive(Clone)]
pub struct Mailbox {
    peers: Arc<DashMap<(u64, String), Peer>>,
    sender: mpsc::Sender<Message>,
}

impl Mailbox {
    async fn peer(&self, leader_id: u64, leader_addr: String) -> Result<RaftGrpcClient> {
        self.peers
            .entry((leader_id, leader_addr.clone()))
            .or_insert(Peer::new(leader_addr)?)
            .client()
            .await
    }

    async fn send_to_leader(
        &self,
        proposal: Vec<u8>,
        leader_id: u64,
        leader_addr: String,
    ) -> Result<RaftResponse> {
        let leader_client = self.peer(leader_id, leader_addr).await?;
        let proposal_sender = ProposalSender {
            proposal,
            client: leader_client,
            timeout: Duration::from_millis(1000),
            max_retries: 3,
        };
        proposal_sender.send().await
    }

    /// sends a proposal message to commit to the node. This fails if the current node is not the
    /// leader
    pub async fn send(&self, message: Vec<u8>) -> Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        let proposal = Message::Propose {
            proposal: message.clone(),
            chan: tx,
        };
        let sender = self.sender.clone();
        // TODO make timeout duration a variable
        match sender.send(proposal).await {
            Ok(_) => match timeout(Duration::from_secs(15), rx).await {
                Ok(Ok(RaftResponse::Response { data })) => Ok(data),
                Ok(Ok(RaftResponse::WrongLeader {
                          leader_id,
                          leader_addr,
                      })) => {
                    debug!(
                        "this node not is Leader, leader_id: {:?}, leader_addr: {:?}",
                        leader_id, leader_addr
                    );
                    if let Some(leader_addr) = leader_addr {
                        if leader_id != 0 {
                            let resp = self.send_to_leader(message, leader_id, leader_addr).await?;
                            if let RaftResponse::Response { data } = resp {
                                return Ok(data);
                            } else {
                                warn!("recv other raft response: {:?}", resp);
                                return Err(Error::Unknown);
                            }
                        }
                    }
                    Err(Error::LeaderNotExist)
                }
                Ok(Ok(resp)) => {
                    warn!("recv other raft response: {:?}", resp);
                    Err(Error::Unknown)
                }
                Ok(Err(e)) => {
                    error!("recv error, {:?}", e);
                    Err(Error::Unknown)
                }
                Err(e) => {
                    error!("timeout, {:?}", e);
                    Err(Error::Unknown)
                }
            },
            Err(e) => {
                error!("send error, {:?}", e.to_string());
                Err(Error::Unknown)
            }
        }
    }
    //
    // pub async fn send_to_this_node(&self, message: Vec<u8>) -> Result<Vec<u8>> {
    //     info!("send message len: {:?}", message.len());
    //     let (tx, rx) = oneshot::channel();
    //     let proposal = Message::Propose {
    //         proposal: message,
    //         chan: tx,
    //     };
    //     let sender = self.sender.clone();
    //     // TODO make timeout duration a variable
    //     match sender.send(proposal).await {
    //         Ok(_) => match timeout(Duration::from_secs(5), rx).await {
    //             Ok(Ok(RaftResponse::Response { data })) => {
    //                 info!("recv raft response: {:?}", data);
    //                 Ok(data)
    //             }
    //             Ok(Ok(RaftResponse::WrongLeader {
    //                 leader_id,
    //                 leader_addr,
    //             })) => {
    //                 warn!(
    //                     "recv WrongLeader leader_id: {:?}, leader_addr: {:?}",
    //                     leader_id, leader_addr
    //                 );
    //                 Err(Error::Unknown)
    //             }
    //             Ok(Ok(resp)) => {
    //                 warn!("recv other raft response: {:?}", resp);
    //                 Err(Error::Unknown)
    //             }
    //             Ok(Err(e)) => {
    //                 error!("recv error, {:?}", e);
    //                 Err(Error::Unknown)
    //             }
    //             Err(e) => {
    //                 error!("timeout, {:?}", e);
    //                 Err(Error::Unknown)
    //             }
    //         },
    //         Err(e) => {
    //             error!("send error, {:?}", e.to_string());
    //             Err(Error::Unknown)
    //         }
    //     }
    // }

    pub async fn query(&self, query: Vec<u8>) -> Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        let sender = self.sender.clone();
        match sender.send(Message::Query { query, chan: tx }).await {
            Ok(_) => match timeout(Duration::from_secs(5), rx).await {
                Ok(Ok(RaftResponse::Response { data })) => Ok(data),
                _ => Err(Error::Unknown),
            },
            _ => Err(Error::Unknown),
        }
    }

    pub async fn leave(&self) -> Result<()> {
        let mut change = ConfChange::default();
        // set node id to 0, the node will set it to self when it receives it.
        change.set_node_id(0);
        change.set_change_type(ConfChangeType::RemoveNode);
        let sender = self.sender.clone();
        let (chan, rx) = oneshot::channel();
        match sender.send(Message::ConfigChange { change, chan }).await {
            Ok(_) => match rx.await {
                Ok(RaftResponse::Ok) => Ok(()),
                _ => Err(Error::Unknown),
            },
            _ => Err(Error::Unknown),
        }
    }

    pub async fn status(&self) -> Result<Status> {
        let (tx, rx) = oneshot::channel();
        let sender = self.sender.clone();
        match sender.send(Message::Status { chan: tx }).await {
            Ok(_) => match timeout(Duration::from_secs(5), rx).await {
                Ok(Ok(RaftResponse::Status(status))) => Ok(status),
                _ => Err(Error::Unknown),
            },
            _ => Err(Error::Unknown),
        }
    }
}

pub struct Raft<S: Store + 'static> {
    store: S,
    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
    addr: String,
    logger: slog::Logger,
}

impl<S: Store + Send + Sync + 'static> Raft<S> {
    /// creates a new node with the given address and store.
    pub fn new(addr: String, store: S, logger: slog::Logger) -> Self {
        let (tx, rx) = mpsc::channel(100);
        Self {
            store,
            tx,
            rx,
            addr,
            logger,
        }
    }

    /// gets the node's `Mailbox`.
    pub fn mailbox(&self) -> Mailbox {
        Mailbox {
            peers: Arc::new(DashMap::default()),
            sender: self.tx.clone(),
        }
    }

    /// find leader id and leader address
    pub async fn find_leader_info(&self, peer_addrs: Vec<String>) -> Result<Option<(u64, String)>> {
        let mut futs = Vec::new();
        for addr in peer_addrs {
            let fut = async {
                let _addr = addr.clone();
                match self.request_leader(addr).await {
                    Ok(reply) => Ok(reply),
                    Err(e) => {
                        info!("find_leader, addr: {}, {:?}", _addr, e);
                        Err(e)
                    }
                }
            };
            futs.push(fut.boxed());
        }

        let (leader_id, leader_addr) = match futures::future::select_ok(futs).await {
            Ok((Some((leader_id, leader_addr)), _)) => (leader_id, leader_addr),
            Ok((None, _)) => return Err(Error::LeaderNotExist),
            Err(_e) => return Ok(None),
        };

        info!("leader_id: {}, leader_addr: {}", leader_id, leader_addr);
        if leader_id == 0 {
            Ok(None)
        } else {
            Ok(Some((leader_id, leader_addr)))
        }
    }

    async fn request_leader(&self, peer_addr: String) -> Result<Option<(u64, String)>> {
        let (leader_id, leader_addr): (u64, String) = {
            let mut client = RaftServiceClient::connect(format!("http://{}", peer_addr)).await?;
            let response = client
                .request_id(Request::new(Empty::default()))
                .await?
                .into_inner();
            match response.code() {
                ResultCode::WrongLeader => {
                    let (leader_id, addr): (u64, Option<String>) = deserialize(&response.data)?;
                    if let Some(addr) = addr {
                        (leader_id, addr)
                    } else {
                        return Ok(None);
                    }
                }
                ResultCode::Ok => (deserialize(&response.data)?, peer_addr),
                ResultCode::Error => return Ok(None),
            }
        };
        Ok(Some((leader_id, leader_addr)))
    }

    /// Create a new leader for the cluster, with id 1. There has to be exactly one node in the
    /// cluster that is initialised that way
    pub async fn lead(self, node_id: u64) -> Result<()> {
        let addr = self.addr.clone();
        let node =
            RaftNode::new_leader(self.rx, self.tx.clone(), node_id, self.store, &self.logger);

        let server = RaftServer::new(self.tx, addr);
        let _server_handle = tokio::spawn(server.run());
        let node_handle = tokio::spawn(async {
            if let Err(e) = node.run().await {
                warn!("node run error: {:?}", e);
                Err(e)
            } else {
                Ok(())
            }
        });
        let e = tokio::try_join!(node_handle);
        warn!("leaving leader node, {:?}", e);

        Ok(())
    }

    /// Tries to join a new cluster at `addr`, getting an id from the leader, or finding it if
    /// `addr` is not the current leader of the cluster
    pub async fn join(
        self,
        node_id: u64,
        leader_id: Option<u64>,
        leader_addr: String,
    ) -> Result<()> {
        // 1. try to discover the leader and obtain an id from it, if leader_id is None.
        info!("attempting to join peer cluster at {}", leader_addr);
        let (leader_id, leader_addr): (u64, String) = if let Some(leader_id) = leader_id {
            (leader_id, leader_addr)
        } else {
            self.request_leader(leader_addr)
                .await?
                .ok_or(Error::JoinError)?
        };

        // 2. run server and node to prepare for joining
        let addr = self.addr.clone();
        let mut node =
            RaftNode::new_follower(self.rx, self.tx.clone(), node_id, self.store, &self.logger)?;
        node.add_peer(&leader_addr, leader_id).await?;
        let mut client = node.peer_mut(leader_id).unwrap().client().await?;
        let server = RaftServer::new(self.tx, addr);
        let _server_handle = tokio::spawn(server.run());
        // let node_handle = tokio::spawn(node.run());

        //try remove from the cluster
        let mut change_remove = ConfChange::default();
        change_remove.set_node_id(node_id);
        change_remove.set_change_type(ConfChangeType::RemoveNode);
        let change_remove = RiteraftConfChange {
            inner: protobuf::Message::write_to_bytes(&change_remove)?,
        };

        let raft_response = client
            .change_config(Request::new(change_remove))
            .await?
            .into_inner();

        info!(
            "change_remove raft_response: {:?}",
            deserialize(&raft_response.inner)?
        );

        // 3. Join the cluster
        // TODO: handle wrong leader
        let mut change = ConfChange::default();
        change.set_node_id(node_id);
        change.set_change_type(ConfChangeType::AddNode);
        // change.set_context(prost::bytes::Bytes::from(serialize(&self.addr)?));
        change.set_context(serialize(&self.addr)?);

        let change = RiteraftConfChange {
            inner: protobuf::Message::write_to_bytes(&change)?,
        };
        let raft_response = client
            .change_config(Request::new(change))
            .await?
            .into_inner();
        if let RaftResponse::JoinSuccess {
            assigned_id,
            peer_addrs,
        } = deserialize(&raft_response.inner)?
        {
            info!("change_config response.assigned_id: {:?}", assigned_id);
            info!("change_config response.peer_addrs: {:?}", peer_addrs);
            for (id, addr) in peer_addrs {
                if id != assigned_id {
                    node.add_peer(&addr, id).await?;
                }
            }
        } else {
            return Err(Error::JoinError);
        }

        let node_handle = tokio::spawn(node.run());
        let _ = tokio::try_join!(node_handle);

        Ok(())
    }
}
