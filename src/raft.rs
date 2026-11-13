use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytestring::ByteString;
use futures::channel::{mpsc, oneshot};
use futures::future::FutureExt;
use futures::SinkExt;
use log::{debug, info, warn};
use postcard::{from_bytes, to_stdvec};
use prost::Message as _;
use tikv_raft::eraftpb::{ConfChange, ConfChangeType};
use tokio::sync::{watch, RwLock};
use tokio::time::timeout;
use tonic::Request;

use crate::error::{Error, Result};
use crate::message::{Message, RaftResponse, RemoveNodeType, Status};
use crate::raft_node::{Peer, RaftNode};
use crate::raft_server::RaftServer;
use crate::raft_service::connect;
use crate::raft_service::{
    ConfChange as RiteraftConfChange, Empty, RaftServiceClientType, ResultCode,
};
use crate::Config;

type DashMap<K, V> = dashmap::DashMap<K, V, ahash::RandomState>;

const JOIN_CONNECT_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(500);
const JOIN_CONNECT_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);
const JOIN_CONNECT_RETRY_MAX_JITTER: u64 = 250;

fn join_connect_retry_delay(
    node_id: u64,
    attempt: usize,
    base_delay: Duration,
    remaining: Duration,
) -> Duration {
    let jitter_millis = node_id
        .wrapping_mul(97)
        .wrapping_add((attempt as u64).wrapping_mul(53))
        % (JOIN_CONNECT_RETRY_MAX_JITTER + 1);
    let delay = base_delay
        .saturating_add(Duration::from_millis(jitter_millis))
        .min(JOIN_CONNECT_RETRY_MAX_DELAY);
    delay.min(remaining)
}

async fn connect_to_join_leader(
    peer: &Peer,
    node_id: u64,
    leader_addr: &str,
    retry_timeout: Duration,
) -> Result<RaftServiceClientType> {
    if retry_timeout.is_zero() {
        return peer.client().await;
    }

    let started_at = Instant::now();
    let mut attempt = 1usize;
    let mut base_delay = JOIN_CONNECT_RETRY_INITIAL_DELAY;

    loop {
        let remaining = retry_timeout.saturating_sub(started_at.elapsed());
        if remaining.is_zero() {
            warn!(
                "raft join connection to {} timed out after {:?}",
                leader_addr, retry_timeout
            );
            return Err(Error::Elapsed);
        }

        match timeout(remaining, peer.client()).await {
            Ok(Ok(client)) => {
                if attempt > 1 {
                    info!(
                        "raft join connection to {} recovered after {} attempts in {:?}",
                        leader_addr,
                        attempt,
                        started_at.elapsed()
                    );
                }
                return Ok(client);
            }
            Ok(Err(error @ Error::Grpc(_))) => {
                let remaining = retry_timeout.saturating_sub(started_at.elapsed());
                if remaining.is_zero() {
                    warn!(
                        "raft join connection to {} failed after {} attempts in {:?}: {:?}",
                        leader_addr,
                        attempt,
                        started_at.elapsed(),
                        error
                    );
                    return Err(error);
                }

                let retry_delay = join_connect_retry_delay(node_id, attempt, base_delay, remaining);
                warn!(
                    "raft join connection attempt {} to {} failed: {:?}; retrying in {:?}",
                    attempt, leader_addr, error, retry_delay
                );
                tokio::time::sleep(retry_delay).await;
                base_delay = base_delay
                    .saturating_mul(2)
                    .min(JOIN_CONNECT_RETRY_MAX_DELAY);
                attempt += 1;
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                warn!(
                    "raft join connection to {} timed out after {} attempts in {:?}",
                    leader_addr,
                    attempt,
                    started_at.elapsed()
                );
                return Err(Error::Elapsed);
            }
        }
    }
}

#[async_trait]
pub trait Store: Clone + Send + Sync {
    async fn apply(&mut self, message: &[u8]) -> Result<Vec<u8>>;
    async fn query(&self, query: &[u8]) -> Result<Vec<u8>>;
    async fn snapshot(&self) -> Result<Vec<u8>>;
    async fn restore(&mut self, snapshot: &[u8]) -> Result<()>;
}

struct ProposalSender {
    proposal: Vec<u8>,
    client: Peer,
}

impl ProposalSender {
    async fn send(self) -> Result<RaftResponse> {
        match self.client.send_proposal(self.proposal).await {
            Ok(reply) => {
                let raft_response: RaftResponse = from_bytes(&reply)?;
                Ok(raft_response)
            }
            Err(e) => {
                warn!("error sending proposal {:?}", e);
                Err(e)
            }
        }
    }
}

#[derive(Clone)]
struct LeaderInfo {
    leader: bool,
    target_leader_id: u64,
    target_leader_addr: Option<String>,
}

type LeaderInfoError = ByteString;

/// A mailbox to send messages to a running raft node.
#[derive(Clone)]
pub struct Mailbox {
    peers: Arc<DashMap<(u64, String), Peer>>,
    sender: mpsc::Sender<Message>,
    shutdown: watch::Sender<bool>,
    grpc_timeout: Duration,
    grpc_concurrency_limit: usize,
    grpc_message_size: usize,
    grpc_breaker_threshold: u64,
    grpc_breaker_retry_interval: i64,
    #[allow(clippy::type_complexity)]
    leader_info: Arc<
        RwLock<
            Option<(
                Option<LeaderInfo>,
                Option<LeaderInfoError>,
                std::time::Instant,
            )>,
        >,
    >,
}

impl Mailbox {
    #[inline]
    pub(crate) fn new(
        peers: Arc<DashMap<(u64, String), Peer>>,
        sender: mpsc::Sender<Message>,
        shutdown: watch::Sender<bool>,
        grpc_timeout: Duration,
        grpc_concurrency_limit: usize,
        grpc_message_size: usize,
        grpc_breaker_threshold: u64,
        grpc_breaker_retry_interval: i64,
    ) -> Self {
        Self {
            peers,
            sender,
            shutdown,
            grpc_timeout,
            grpc_concurrency_limit,
            grpc_message_size,
            grpc_breaker_threshold,
            grpc_breaker_retry_interval,
            leader_info: Arc::new(RwLock::new(None)),
        }
    }

    /// Gracefully stops the local Raft node and its gRPC listener.
    ///
    /// This is intended for explicit lifecycle management. It does not run on
    /// the proposal or query fast paths.
    pub async fn shutdown(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let result = match self
            .sender
            .clone()
            .send(Message::Shutdown { chan: tx })
            .await
        {
            Ok(()) => match timeout(self.grpc_timeout, rx).await {
                Ok(Ok(RaftResponse::Ok)) => Ok(()),
                Ok(Ok(RaftResponse::Error(error))) => Err(Error::Msg(error)),
                Ok(Ok(response)) => Err(Error::Msg(format!(
                    "unexpected raft shutdown response: {response:?}"
                ))),
                Ok(Err(error)) => Err(Error::RecvError(error.to_string())),
                Err(error) => Err(error.into()),
            },
            Err(error) => Err(Error::SendError(error.to_string())),
        };

        // Always release the listener, even when the event loop has already
        // stopped and can no longer acknowledge the shutdown message.
        let _ = self.shutdown.send(true);
        result
    }

    /// Retrieves a list of peers with their IDs.
    /// This method returns a vector containing tuples of peer IDs and their respective `Peer` objects.
    /// It iterates over the internal `peers` map and collects the IDs and cloned `Peer` instances.
    #[inline]
    pub fn pears(&self) -> Vec<(u64, Peer)> {
        self.peers
            .iter()
            .map(|p| {
                let (id, _) = p.key();
                (*id, p.value().clone())
            })
            .collect::<Vec<_>>()
    }

    #[inline]
    async fn peer(&self, leader_id: u64, leader_addr: String) -> Peer {
        self.peers
            .entry((leader_id, leader_addr.clone()))
            .or_insert_with(|| {
                Peer::new(
                    leader_addr,
                    self.grpc_timeout,
                    self.grpc_concurrency_limit,
                    self.grpc_message_size,
                    self.grpc_breaker_threshold,
                    self.grpc_breaker_retry_interval,
                )
            })
            .clone()
    }

    #[inline]
    async fn send_to_leader(
        &self,
        proposal: Vec<u8>,
        leader_id: u64,
        leader_addr: String,
    ) -> Result<RaftResponse> {
        let peer = self.peer(leader_id, leader_addr).await;
        let proposal_sender = ProposalSender {
            proposal,
            client: peer,
        };
        proposal_sender.send().await
    }

    /// Sends a proposal to the leader node.
    /// This method first attempts to send the proposal to the local node if it is the leader.
    /// If the node is not the leader, it retrieves the leader's address and sends the proposal to the leader node.
    /// If the proposal is successfully handled, the method returns a `RaftResponse::Response` with the resulting data.
    #[inline]
    pub async fn send_proposal(&self, message: Vec<u8>) -> Result<Vec<u8>> {
        match self.get_leader_info().await? {
            LeaderInfo { leader: true, .. } => {
                debug!("this node is leader");
                let (tx, rx) = oneshot::channel();
                let proposal = Message::Propose {
                    proposal: message.clone(),
                    chan: tx,
                };
                let mut sender = self.sender.clone();
                sender
                    .send(proposal)
                    .await //.try_send(proposal)
                    .map_err(|e| Error::SendError(e.to_string()))?;
                let reply = timeout(self.grpc_timeout, rx).await;
                let reply = reply
                    .map_err(|e| Error::RecvError(e.to_string()))?
                    .map_err(|e| Error::RecvError(e.to_string()))?;
                match reply {
                    RaftResponse::Response { data } => return Ok(data),
                    RaftResponse::Busy => return Err(Error::Busy),
                    RaftResponse::Error(e) => return Err(Error::from(e)),
                    _ => {
                        warn!("Recv other raft response: {:?}", reply);
                        return Err(Error::Unknown);
                    }
                }
            }
            LeaderInfo {
                leader: false,
                target_leader_id,
                target_leader_addr,
                ..
            } => {
                debug!(
                    "This node not is Leader, leader_id: {:?}, leader_addr: {:?}",
                    target_leader_id, target_leader_addr
                );
                if let Some(target_leader_addr) = target_leader_addr {
                    if target_leader_id != 0 {
                        return match self
                            .send_to_leader(message, target_leader_id, target_leader_addr.clone())
                            .await?
                        {
                            RaftResponse::Response { data } => return Ok(data),
                            RaftResponse::WrongLeader {
                                leader_id,
                                leader_addr,
                            } => {
                                warn!("The target node is not the Leader, target_leader_id: {}, target_leader_addr: {:?}, actual_leader_id: {}, actual_leader_addr: {:?}",
                            target_leader_id, target_leader_addr, leader_id, leader_addr);
                                return Err(Error::NotLeader);
                            }
                            RaftResponse::Busy => Err(Error::Busy),
                            RaftResponse::Error(e) => Err(Error::from(e)),
                            _ => {
                                warn!("Recv other raft response, target_leader_id: {}, target_leader_addr: {:?}", target_leader_id, target_leader_addr);
                                return Err(Error::Unknown);
                            }
                        };
                    }
                }
            }
        }
        Err(Error::LeaderNotExist)
    }

    /// Deprecated method to send a message, internally calls `send_proposal`.
    #[inline]
    #[deprecated]
    pub async fn send(&self, message: Vec<u8>) -> Result<Vec<u8>> {
        self.send_proposal(message).await
    }

    /// Sends a query to the Raft node and returns the response data.
    /// It sends a `Message::Query` containing the query bytes and waits for a response.
    /// On success, it returns the data wrapped in `RaftResponse::Response`.
    #[inline]
    pub async fn query(&self, query: Vec<u8>) -> Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        let mut sender = self.sender.clone();
        match sender.try_send(Message::Query { query, chan: tx }) {
            Ok(()) => match timeout(self.grpc_timeout, rx).await {
                Ok(Ok(RaftResponse::Response { data })) => Ok(data),
                Ok(Ok(RaftResponse::Error(e))) => Err(Error::from(e)),
                _ => Err(Error::Unknown),
            },
            Err(e) => Err(Error::SendError(e.to_string())),
        }
    }

    /// Sends a request to leave the Raft cluster.
    /// It initiates a `ConfigChange` to remove the node from the cluster and waits for a response.
    #[inline]
    pub async fn leave(&self) -> Result<()> {
        let mut change = ConfChange::default();
        // set node id to 0, the node will set it to self when it receives it.
        change.set_node_id(0);
        change.set_change_type(ConfChangeType::RemoveNode);
        change.set_context(to_stdvec(&RemoveNodeType::Normal)?);
        let mut sender = self.sender.clone();
        let (chan, rx) = oneshot::channel();
        match sender.send(Message::ConfigChange { change, chan }).await {
            Ok(()) => match rx.await {
                Ok(RaftResponse::Ok) => Ok(()),
                Ok(RaftResponse::Error(e)) => Err(Error::from(e)),
                _ => Err(Error::Unknown),
            },
            Err(e) => Err(Error::SendError(e.to_string())),
        }
    }

    async fn change_config(&self, change: ConfChange) -> Result<RaftResponse> {
        let mut sender = self.sender.clone();
        let (chan, rx) = oneshot::channel();
        sender
            .send(Message::ConfigChange { change, chan })
            .await
            .map_err(|e| Error::SendError(e.to_string()))?;
        match timeout(self.grpc_timeout, rx).await {
            Ok(Ok(RaftResponse::Error(e))) => Err(Error::from(e)),
            Ok(Ok(RaftResponse::WrongLeader { .. })) => Err(Error::NotLeader),
            Ok(Ok(response)) => Ok(response),
            Ok(Err(e)) => Err(Error::RecvError(e.to_string())),
            Err(e) => Err(Error::RecvError(e.to_string())),
        }
    }

    /// Promotes an existing learner to a voting member. This must be invoked
    /// on the current leader after the learner has caught up.
    pub async fn promote_learner(&self, node_id: u64, node_addr: String) -> Result<()> {
        let mut change = ConfChange::default();
        change.set_node_id(node_id);
        change.set_change_type(ConfChangeType::AddNode);
        change.set_context(to_stdvec(&node_addr)?);
        match self.change_config(change).await? {
            RaftResponse::Ok | RaftResponse::JoinSuccess { .. } => Ok(()),
            response => Err(Error::from(format!(
                "unexpected promote learner response: {response:?}"
            ))),
        }
    }

    /// Removes a member through the current leader.
    pub async fn remove_node(&self, node_id: u64) -> Result<()> {
        let mut change = ConfChange::default();
        change.set_node_id(node_id);
        change.set_change_type(ConfChangeType::RemoveNode);
        change.set_context(to_stdvec(&RemoveNodeType::Normal)?);
        match self.change_config(change).await? {
            RaftResponse::Ok => Ok(()),
            response => Err(Error::from(format!(
                "unexpected remove node response: {response:?}"
            ))),
        }
    }

    /// Requests a leadership transfer to a caught-up voter. The call confirms
    /// that the request was accepted; callers should poll status to confirm the
    /// new leader.
    pub async fn transfer_leader(&self, node_id: u64) -> Result<()> {
        let mut sender = self.sender.clone();
        let (chan, rx) = oneshot::channel();
        sender
            .send(Message::TransferLeader { node_id, chan })
            .await
            .map_err(|e| Error::SendError(e.to_string()))?;
        match timeout(self.grpc_timeout, rx).await {
            Ok(Ok(RaftResponse::Ok)) => Ok(()),
            Ok(Ok(RaftResponse::Error(e))) => Err(Error::from(e)),
            Ok(Ok(RaftResponse::WrongLeader { .. })) => Err(Error::NotLeader),
            Ok(Ok(response)) => Err(Error::from(format!(
                "unexpected transfer leader response: {response:?}"
            ))),
            Ok(Err(e)) => Err(Error::RecvError(e.to_string())),
            Err(e) => Err(Error::RecvError(e.to_string())),
        }
    }

    /// Retrieves the current status of the Raft node.
    /// Sends a `Message::Status` request and waits for a `RaftResponse::Status` reply, which contains the node's status.
    #[inline]
    pub async fn status(&self) -> Result<Status> {
        let (tx, rx) = oneshot::channel();
        let mut sender = self.sender.clone();
        match sender.send(Message::Status { chan: tx }).await {
            Ok(_) => match timeout(self.grpc_timeout, rx).await {
                Ok(Ok(RaftResponse::Status(status))) => Ok(status),
                Ok(Ok(RaftResponse::Error(e))) => Err(Error::from(e)),
                _ => Err(Error::Unknown),
            },
            Err(e) => Err(Error::SendError(e.to_string())),
        }
    }

    /// Retrieves leader information, including whether the current node is the leader, the leader ID, and its address.
    /// This method sends a `Message::RequestId` and waits for a response with the leader's ID and address.
    #[inline]
    async fn _get_leader_info(&self) -> std::result::Result<LeaderInfo, LeaderInfoError> {
        let (tx, rx) = oneshot::channel();
        let mut sender = self.sender.clone();
        match sender.send(Message::RequestId { chan: tx }).await {
            Ok(_) => match timeout(self.grpc_timeout, rx).await {
                Ok(Ok(RaftResponse::RequestId { leader_id })) => Ok(LeaderInfo {
                    leader: true,
                    target_leader_id: leader_id,
                    target_leader_addr: None,
                }),
                Ok(Ok(RaftResponse::WrongLeader {
                    leader_id,
                    leader_addr,
                })) => Ok(LeaderInfo {
                    leader: false,
                    target_leader_id: leader_id,
                    target_leader_addr: leader_addr,
                }),
                Ok(Ok(RaftResponse::Error(e))) => Err(LeaderInfoError::from(e)),
                _ => Err("Unknown".into()),
            },
            Err(e) => Err(LeaderInfoError::from(e.to_string())),
        }
    }

    #[inline]
    async fn get_leader_info(&self) -> Result<LeaderInfo> {
        {
            let leader_info = self.leader_info.read().await;
            if let Some((leader_info, err, inst)) = leader_info.as_ref() {
                if inst.elapsed().as_secs() < 5 {
                    if let Some(leader_info) = leader_info {
                        return Ok(leader_info.clone());
                    }
                    if let Some(err) = err {
                        return Err(err.to_string().into());
                    }
                }
            }
        }

        let mut write = self.leader_info.write().await;

        return match self._get_leader_info().await {
            Ok(leader_info) => {
                write.replace((Some(leader_info.clone()), None, std::time::Instant::now()));
                Ok(leader_info)
            }
            Err(e) => {
                let err = e.to_string().into();
                write.replace((None, Some(e), std::time::Instant::now()));
                Err(err)
            }
        };
    }
}

pub struct Raft<S: Store + 'static> {
    store: S,
    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
    laddr: SocketAddr,
    logger: slog::Logger,
    cfg: Arc<Config>,
    shutdown: watch::Sender<bool>,
}

impl<S: Store + Send + Sync + 'static> Raft<S> {
    /// Creates a new Raft node with the provided address, store, logger, and configuration.
    /// The node communicates with other peers using a mailbox.
    pub fn new<A: ToSocketAddrs>(
        laddr: A,
        store: S,
        logger: slog::Logger,
        cfg: Config,
    ) -> Result<Self> {
        let laddr = laddr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| Error::from("None"))?;
        let (tx, rx) = mpsc::channel(100_000);
        let (shutdown, _) = watch::channel(false);
        let cfg = Arc::new(cfg);
        Ok(Self {
            store,
            tx,
            rx,
            laddr,
            logger,
            cfg,
            shutdown,
        })
    }

    /// Returns a `Mailbox` for the Raft node, which facilitates communication with peers.
    pub fn mailbox(&self) -> Mailbox {
        Mailbox::new(
            Arc::new(DashMap::default()),
            self.tx.clone(),
            self.shutdown.clone(),
            self.cfg.grpc_timeout,
            self.cfg.grpc_concurrency_limit,
            self.cfg.grpc_message_size,
            self.cfg.grpc_breaker_threshold,
            self.cfg.grpc_breaker_retry_interval.as_millis() as i64,
        )
    }

    /// Finds leader information by querying a list of peer addresses.
    /// Returns the leader ID and its address if found.
    pub async fn find_leader_info(&self, peer_addrs: Vec<String>) -> Result<Option<(u64, String)>> {
        let mut futs = Vec::new();
        for addr in peer_addrs {
            let fut = async {
                let _addr = addr.clone();
                match self.request_leader(addr).await {
                    Ok(reply) => Ok(reply),
                    Err(e) => Err(e),
                }
            };
            futs.push(fut.boxed());
        }

        let (leader_id, leader_addr) = match futures::future::select_ok(futs).await {
            Ok((Some((leader_id, leader_addr)), _)) => (leader_id, leader_addr),
            Ok((None, _)) => return Err(Error::LeaderNotExist),
            Err(_e) => return Ok(None),
        };

        if leader_id == 0 {
            Ok(None)
        } else {
            Ok(Some((leader_id, leader_addr)))
        }
    }

    /// Requests the leader information from a specific peer.
    /// Sends a `Message::RequestId` to the peer and waits for the response.
    async fn request_leader(&self, peer_addr: String) -> Result<Option<(u64, String)>> {
        let (leader_id, leader_addr): (u64, String) = {
            let mut client = connect(
                &peer_addr,
                1,
                self.cfg.grpc_message_size,
                self.cfg.grpc_timeout,
            )
            .await?;
            let response = client
                .request_id(Request::new(Empty::default()))
                .await?
                .into_inner();
            match response.code() {
                ResultCode::WrongLeader => {
                    let (leader_id, addr): (u64, Option<String>) = from_bytes(&response.data)?;
                    if let Some(addr) = addr {
                        (leader_id, addr)
                    } else {
                        return Ok(None);
                    }
                }
                ResultCode::Ok => (from_bytes(&response.data)?, peer_addr),
                ResultCode::Error => return Ok(None),
            }
        };
        Ok(Some((leader_id, leader_addr)))
    }

    /// The `lead` function transitions the current node to the leader role in a Raft cluster.
    /// It initializes the leader node and runs both the Raft server and the node concurrently.
    /// The function will return once the server or node experiences an error, or when the leader
    /// role is relinquished.
    ///
    /// # Arguments
    ///
    /// * `node_id` - The unique identifier for the node.
    ///
    /// # Returns
    ///
    /// A `Result<()>` indicating success or failure during the process.
    pub async fn lead(self, node_id: u64, node_addr: String) -> Result<()> {
        let mut node = RaftNode::new_leader(
            self.rx,
            self.tx.clone(),
            node_id,
            self.store,
            &self.logger,
            self.cfg.clone(),
        )
        .await?;
        node.add_peer(&node_addr, node_id);
        let server = RaftServer::new(self.tx, self.laddr, self.cfg.clone());
        let server_handle = async {
            if let Err(e) = server.run(self.shutdown.subscribe()).await {
                warn!("raft server run error: {:?}", e);
                Err(e)
            } else {
                Ok(())
            }
        };
        let node_handle = async {
            if let Err(e) = node.run().await {
                warn!("node run error: {:?}", e);
                Err(e)
            } else {
                Ok(())
            }
        };

        tokio::try_join!(server_handle, node_handle)?;
        info!("leaving leader node");

        Ok(())
    }

    /// The `join` function is used to make the current node join an existing Raft cluster.
    /// It tries to discover the current leader, communicates with the leader to join the cluster,
    /// and configures the node as a follower.
    ///
    /// # Arguments
    ///
    /// * `node_id` - The unique identifier for the current node.
    /// * `node_addr` - The address of the current node.
    /// * `leader_id` - The optional leader node's identifier (if already known).
    /// * `leader_addr` - The address of the leader node.
    ///
    /// # Returns
    ///
    /// A `Result<()>` indicating success or failure during the joining process.
    pub async fn join(
        self,
        node_id: u64,
        node_addr: String,
        leader_id: Option<u64>,
        leader_addr: String,
    ) -> Result<()> {
        self.join_with_role(
            node_id,
            node_addr,
            leader_id,
            leader_addr,
            ConfChangeType::AddNode,
        )
        .await
    }

    /// Joins an existing cluster as a non-voting learner. The learner must be
    /// explicitly promoted by the leader after its replicated state catches up.
    pub async fn join_as_learner(
        self,
        node_id: u64,
        node_addr: String,
        leader_id: Option<u64>,
        leader_addr: String,
    ) -> Result<()> {
        self.join_with_role(
            node_id,
            node_addr,
            leader_id,
            leader_addr,
            ConfChangeType::AddLearnerNode,
        )
        .await
    }

    async fn join_with_role(
        self,
        node_id: u64,
        node_addr: String,
        leader_id: Option<u64>,
        leader_addr: String,
        join_change_type: ConfChangeType,
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
        let join_connect_retry_timeout = self.cfg.join_connect_retry_timeout;
        let mut node = RaftNode::new_follower(
            self.rx,
            self.tx.clone(),
            node_id,
            self.store,
            &self.logger,
            self.cfg.clone(),
        )?;
        let peer = node.add_peer(&leader_addr, leader_id);
        let mut client =
            connect_to_join_leader(&peer, node_id, &leader_addr, join_connect_retry_timeout)
                .await?;
        let server = RaftServer::new(self.tx, self.laddr, self.cfg.clone());
        let server_handle = async {
            if let Err(e) = server.run(self.shutdown.subscribe()).await {
                warn!("raft server run error: {:?}", e);
                Err(e)
            } else {
                Ok(())
            }
        };

        let node_handle = async {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            //try remove from the cluster
            let mut change_remove = ConfChange::default();
            change_remove.set_node_id(node_id);
            change_remove.set_change_type(ConfChangeType::RemoveNode);
            change_remove.set_context(to_stdvec(&RemoveNodeType::Stale)?);
            let change_remove = RiteraftConfChange {
                inner: ConfChange::encode_to_vec(&change_remove),
            };

            let raft_response = client
                .change_config(Request::new(change_remove))
                .await?
                .into_inner();

            info!(
                "change_remove raft_response: {:?}",
                from_bytes::<RaftResponse>(&raft_response.inner)?
            );

            // 3. Join the cluster
            // TODO: handle wrong leader
            let mut change = ConfChange::default();
            change.set_node_id(node_id);
            change.set_change_type(join_change_type);
            change.set_context(to_stdvec(&node_addr)?);
            // change.set_context(serialize(&node_addr)?);

            let change = RiteraftConfChange {
                inner: ConfChange::encode_to_vec(&change),
            };
            let raft_response = client
                .change_config(Request::new(change))
                .await?
                .into_inner();
            if let RaftResponse::JoinSuccess {
                assigned_id,
                peer_addrs,
            } = from_bytes(&raft_response.inner)?
            {
                info!(
                    "change_config response.assigned_id: {:?}, peer_addrs: {:?}",
                    assigned_id, peer_addrs
                );
                for (id, addr) in peer_addrs {
                    if id != assigned_id {
                        node.add_peer(&addr, id);
                    }
                }
            } else {
                return Err(Error::JoinError);
            }

            if let Err(e) = node.run().await {
                warn!("node run error: {:?}", e);
                Err(e)
            } else {
                Ok(())
            }
        };
        let _ = tokio::try_join!(server_handle, node_handle)?;
        info!("leaving follower node");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Default)]
    struct TestStore;

    #[async_trait]
    impl Store for TestStore {
        async fn apply(&mut self, message: &[u8]) -> Result<Vec<u8>> {
            Ok(message.to_vec())
        }

        async fn query(&self, query: &[u8]) -> Result<Vec<u8>> {
            Ok(query.to_vec())
        }

        async fn snapshot(&self) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }

        async fn restore(&mut self, _snapshot: &[u8]) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn graceful_shutdown_stops_the_raft_runtime() {
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let raft = Raft::new("127.0.0.1:0", TestStore, logger, Config::default()).unwrap();
        let mailbox = raft.mailbox();
        let runtime = tokio::spawn(raft.lead(1, "127.0.0.1:0".to_string()));

        let mut started = false;
        for _ in 0..50 {
            if mailbox.status().await.is_ok() {
                started = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(started, "raft runtime did not start");

        mailbox.shutdown().await.unwrap();
        let result = timeout(Duration::from_secs(5), runtime)
            .await
            .expect("raft runtime did not stop")
            .expect("raft runtime task panicked");
        assert!(result.is_ok());
    }

    #[test]
    fn join_connect_retry_delay_is_bounded() {
        let remaining = Duration::from_millis(120);
        let delay = join_connect_retry_delay(2, 10, Duration::from_secs(5), remaining);
        assert_eq!(delay, remaining);
    }

    #[test]
    fn join_connect_retry_delay_uses_node_jitter() {
        let remaining = Duration::from_secs(10);
        let node_one = join_connect_retry_delay(1, 1, JOIN_CONNECT_RETRY_INITIAL_DELAY, remaining);
        let node_two = join_connect_retry_delay(2, 1, JOIN_CONNECT_RETRY_INITIAL_DELAY, remaining);
        assert_ne!(node_one, node_two);
        assert!(node_one <= JOIN_CONNECT_RETRY_MAX_DELAY);
        assert!(node_two <= JOIN_CONNECT_RETRY_MAX_DELAY);
    }

    #[test]
    fn join_connect_retry_timeout_defaults_to_twenty_seconds() {
        assert_eq!(
            Config::default().join_connect_retry_timeout,
            Duration::from_secs(20)
        );
    }
}
