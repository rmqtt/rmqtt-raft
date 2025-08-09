use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicI64, AtomicIsize, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use bincode::{deserialize, serialize};
use box_counter::Counter;
use bytestring::ByteString;
use futures::channel::{mpsc, oneshot};
use futures::SinkExt;
use futures::StreamExt;
use log::*;
use prost::Message as _;
use scopeguard::guard;
use tikv_raft::eraftpb::{ConfChange, ConfChangeType, Entry, EntryType, Message as RaftMessage};
use tikv_raft::{prelude::*, raw_node::RawNode, Config as RaftConfig};
use tokio::sync::RwLock;
use tokio::time::timeout;
use tonic::Request;

use crate::error::{Error, Result};
use crate::message::{
    Merger, Message, PeerState, Proposals, RaftResponse, RemoveNodeType, ReplyChan, Status,
};
use crate::raft::Store;
use crate::raft_service::raft_service_client::RaftServiceClient;
use crate::raft_service::{connect, Message as RraftMessage, Proposal as RraftProposal, Query};
use crate::storage::{LogStore, MemStorage};
use crate::timeout_recorder::TimeoutRecorder;
use crate::Config;

pub type RaftGrpcClient = RaftServiceClient<tonic::transport::channel::Channel>;

struct MessageSender {
    message: RaftMessage,
    client: Peer,
    client_id: u64,
    chan: mpsc::Sender<Message>,
    max_retries: usize,
    timeout: Duration,
    sending_raft_messages: Arc<AtomicIsize>,
}

impl MessageSender {
    /// attempt to send a message MessageSender::max_retries times at MessageSender::timeout
    /// inteval.
    async fn send(self) {
        let sending_raft_messages = self.sending_raft_messages.clone();
        sending_raft_messages.fetch_add(1, Ordering::SeqCst);
        let _guard = guard((), |_| {
            sending_raft_messages.fetch_sub(1, Ordering::SeqCst);
        });
        self._send().await
    }

    async fn _send(mut self) {
        let mut current_retry = 0usize;
        loop {
            match self.client.send_message(&self.message).await {
                Ok(_) => {
                    return;
                }
                Err(e) => {
                    if current_retry < self.max_retries {
                        current_retry += 1;
                        tokio::time::sleep(self.timeout).await;
                    } else {
                        warn!(
                            "error sending message after {}/{} retries: {:?}, target addr: {:?}",
                            current_retry, self.max_retries, e, self.client.addr
                        );
                        if let Err(e) = self
                            .chan
                            .send(Message::ReportUnreachable {
                                node_id: self.client_id,
                            })
                            .await
                        {
                            warn!(
                                "error ReportUnreachable after {}/{} retries: {:?}, target addr: {:?}",
                                current_retry, self.max_retries, e, self.client.addr
                            );
                        }
                        return;
                    }
                }
            }
        }
    }
}

struct QuerySender {
    query: Vec<u8>,
    client: Peer,
    chan: oneshot::Sender<RaftResponse>,
    max_retries: usize,
    timeout: Duration,
}

impl QuerySender {
    async fn send(self) {
        let mut current_retry = 0usize;

        let mut client = match self.client.client().await {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    "error sending query after, {:?}, target addr: {:?}",
                    e, self.client.addr
                );
                if let Err(e) = self.chan.send(RaftResponse::Error(e.to_string())) {
                    warn!(
                        "send_query, Message::Query, RaftResponse send error: {:?}, target addr: {:?}",
                        e, self.client.addr
                    );
                }
                return;
            }
        };

        loop {
            let message_request = Request::new(Query {
                inner: self.query.clone(),
            });
            match client.send_query(message_request).await {
                Ok(grpc_response) => {
                    let raft_response = match deserialize(&grpc_response.into_inner().inner) {
                        Ok(resp) => resp,
                        Err(e) => {
                            warn!(
                                    "send_query, Message::Query, RaftResponse deserialize error: {:?}, target addr: {:?}",
                                    e, self.client.addr
                                );
                            return;
                        }
                    };
                    if let Err(e) = self.chan.send(raft_response) {
                        warn!(
                            "send_query, Message::Query, RaftResponse send error: {:?}, target addr: {:?}",
                            e, self.client.addr
                        );
                    }
                    return;
                }
                Err(e) => {
                    if current_retry < self.max_retries {
                        current_retry += 1;
                        tokio::time::sleep(self.timeout).await;
                    } else {
                        warn!(
                            "error sending query after {} retries: {}, target addr: {:?}",
                            self.max_retries, e, self.client.addr
                        );
                        if let Err(e) = self.chan.send(RaftResponse::Error(e.to_string())) {
                            warn!(
                                "send_query, Message::Query, RaftResponse send error: {:?}, target addr: {:?}",
                                e, self.client.addr
                            );
                        }
                        return;
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct Peer {
    addr: ByteString,
    client: Arc<RwLock<Option<RaftGrpcClient>>>,
    grpc_fails: Arc<AtomicU64>,
    grpc_fail_time: Arc<AtomicI64>,
    crw_timeout: Duration,
    concurrency_limit: usize,
    grpc_message_size: usize,
    grpc_breaker_threshold: u64,
    grpc_breaker_retry_interval: i64,
    active_tasks: Arc<AtomicI64>,
}

impl Peer {
    /// Creates a new `Peer` instance with the specified parameters.
    ///
    /// # Parameters
    /// - `addr`: The address of the peer to connect to.
    /// - `crw_timeout`: The timeout duration for connection and read/write operations.
    /// - `concurrency_limit`: The maximum number of concurrent gRPC requests allowed.
    /// - `grpc_message_size`: The maximum size of a gRPC message.
    /// - `grpc_breaker_threshold`: The threshold for the number of gRPC failures before breaking the circuit.
    /// - `grpc_breaker_retry_interval`: The time interval for retrying after the circuit breaker is tripped.
    ///
    /// # Returns
    /// - A new `Peer` instance with the provided configuration.
    ///
    /// # Behavior
    /// - Initializes internal state, including counters and timeouts.
    /// - Logs the connection attempt to the specified address.
    pub fn new(
        addr: String,
        crw_timeout: Duration,
        concurrency_limit: usize,
        grpc_message_size: usize,
        grpc_breaker_threshold: u64,
        grpc_breaker_retry_interval: i64,
    ) -> Peer {
        debug!("connecting to node at {}...", addr);
        Peer {
            addr: addr.into(),
            client: Arc::new(RwLock::new(None)),
            grpc_fails: Arc::new(AtomicU64::new(0)),
            grpc_fail_time: Arc::new(AtomicI64::new(0)),
            crw_timeout,
            concurrency_limit,
            grpc_message_size,
            grpc_breaker_threshold,
            grpc_breaker_retry_interval,
            active_tasks: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Returns the number of currently active tasks associated with this peer.
    ///
    /// # Returns
    /// - The number of active tasks as an `i64`.
    ///
    /// # Behavior
    /// - Reads the value of the `active_tasks` counter.
    #[inline]
    pub fn active_tasks(&self) -> i64 {
        self.active_tasks.load(Ordering::SeqCst)
    }

    /// Returns the number of gRPC failures encountered by this peer.
    ///
    /// # Returns
    /// - The number of gRPC failures as a `u64`.
    ///
    /// # Behavior
    /// - Reads the value of the `grpc_fails` counter.
    #[inline]
    pub fn grpc_fails(&self) -> u64 {
        self.grpc_fails.load(Ordering::SeqCst)
    }

    /// Connects to the peer if not already connected, and returns the gRPC client.
    ///
    /// # Returns
    /// - `Ok(RaftGrpcClient)`: On successful connection, returns the gRPC client.
    /// - `Err(Error)`: On failure, returns an error.
    ///
    /// # Behavior
    /// - Checks if the gRPC client is already connected and available.
    /// - If not, attempts to establish a new connection and store the client.
    #[inline]
    async fn connect(&self) -> Result<RaftGrpcClient> {
        if let Some(c) = self.client.read().await.as_ref() {
            return Ok(c.clone());
        }

        let mut client = self.client.write().await;
        if let Some(c) = client.as_ref() {
            return Ok(c.clone());
        }

        let c = connect(
            &self.addr,
            self.concurrency_limit,
            self.grpc_message_size,
            self.crw_timeout,
        )
        .await?;
        client.replace(c.clone());
        Ok(c)
    }

    /// Retrieves the gRPC client by establishing a connection if needed.
    ///
    /// # Returns
    /// - `Ok(RaftGrpcClient)`: On successful connection, returns the gRPC client.
    /// - `Err(Error)`: On failure, returns an error.
    ///
    /// # Behavior
    /// - Calls `connect` to ensure the client is connected and available.
    #[inline]
    pub async fn client(&self) -> Result<RaftGrpcClient> {
        self.connect().await
    }

    /// Sends a Raft message to the peer and waits for a response.
    ///
    /// # Parameters
    /// - `msg`: The Raft message to be sent.
    ///
    /// # Returns
    /// - `Ok(Vec<u8>)`: On successful message send, returns the response data as a byte vector.
    /// - `Err(Error)`: On failure, returns an error.
    ///
    /// # Behavior
    /// - Checks if the peer is available for sending messages.
    /// - Encodes the message and sends it using the `_send_message` method.
    /// - Updates the active task count and records success or failure.
    #[inline]
    pub async fn send_message(&self, msg: &RaftMessage) -> Result<Vec<u8>> {
        if !self.available() {
            return Err(Error::Msg("The gRPC remote service is unavailable".into()));
        }

        let msg = RraftMessage {
            inner: RaftMessage::encode_to_vec(msg),
        };
        self.active_tasks.fetch_add(1, Ordering::SeqCst);
        let reply = self._send_message(msg).await;
        self.active_tasks.fetch_sub(1, Ordering::SeqCst);
        match reply {
            Ok(reply) => {
                self.record_success();
                Ok(reply)
            }
            Err(e) => {
                self.record_failure();
                Err(e)
            }
        }
    }

    #[inline]
    async fn _send_message(&self, msg: RraftMessage) -> Result<Vec<u8>> {
        let c = self.connect().await?;
        async fn task(mut c: RaftGrpcClient, msg: RraftMessage) -> Result<Vec<u8>> {
            let message_request = Request::new(msg);
            let response = c.send_message(message_request).await?;
            let message_reply = response.into_inner();
            Ok(message_reply.inner)
        }

        let result = tokio::time::timeout(self.crw_timeout, task(c, msg)).await;
        let result = result.map_err(|_| Error::Elapsed)??;
        Ok(result)
    }

    /// Sends a Raft proposal to the peer and waits for a response.
    ///
    /// # Parameters
    /// - `msg`: The Raft proposal to be sent as a byte vector.
    ///
    /// # Returns
    /// - `Ok(Vec<u8>)`: On successful proposal send, returns the response data as a byte vector.
    /// - `Err(Error)`: On failure, returns an error.
    ///
    /// # Behavior
    /// - Checks if the peer is available for sending proposals.
    /// - Wraps the proposal in a `RraftProposal` and sends it using the `_send_proposal` method.
    /// - Updates the active task count and records success or failure.
    #[inline]
    pub async fn send_proposal(&self, msg: Vec<u8>) -> Result<Vec<u8>> {
        if !self.available() {
            return Err(Error::Msg("The gRPC remote service is unavailable".into()));
        }

        let msg = RraftProposal { inner: msg };
        let _active_tasks = self.active_tasks.fetch_add(1, Ordering::SeqCst);
        let reply = self._send_proposal(msg).await;
        self.active_tasks.fetch_sub(1, Ordering::SeqCst);
        match reply {
            Ok(reply) => {
                self.record_success();
                Ok(reply)
            }
            Err(e) => {
                self.record_failure();
                Err(e)
            }
        }
    }

    #[inline]
    async fn _send_proposal(&self, msg: RraftProposal) -> Result<Vec<u8>> {
        let c = self.connect().await?;

        async fn task(mut c: RaftGrpcClient, msg: RraftProposal) -> Result<Vec<u8>> {
            let message_request = Request::new(msg);
            let response = c.send_proposal(message_request).await?;
            let message_reply = response.into_inner();
            Ok(message_reply.inner)
        }

        let result = tokio::time::timeout(self.crw_timeout, task(c, msg)).await;
        let result = result.map_err(|_| Error::Elapsed)??;
        Ok(result)
    }

    #[inline]
    fn record_failure(&self) {
        self.grpc_fails.fetch_add(1, Ordering::SeqCst);
        self.grpc_fail_time
            .store(chrono::Local::now().timestamp_millis(), Ordering::SeqCst);
    }

    #[inline]
    fn record_success(&self) {
        self.grpc_fails.store(0, Ordering::SeqCst);
    }

    #[inline]
    pub(crate) fn is_unavailable(&self) -> bool {
        self.grpc_fails.load(Ordering::SeqCst) >= self.grpc_breaker_threshold
    }

    #[inline]
    fn available(&self) -> bool {
        self.grpc_fails.load(Ordering::SeqCst) < self.grpc_breaker_threshold
            || (chrono::Local::now().timestamp_millis()
                - self.grpc_fail_time.load(Ordering::SeqCst))
                > self.grpc_breaker_retry_interval
    }
}

pub struct RaftNode<S: Store> {
    inner: RawNode<MemStorage>,
    pub peers: HashMap<u64, Option<Peer>>,
    pub rcv: mpsc::Receiver<Message>,
    pub snd: mpsc::Sender<Message>,
    store: S,
    uncommitteds: HashMap<u64, ReplyChan>,
    should_quit: bool,
    seq: AtomicU64,
    sending_raft_messages: Arc<AtomicIsize>,
    last_snap_time: Instant,
    cfg: Arc<Config>,
    timeout_recorder: TimeoutRecorder,
    propose_counter: Counter,
}

impl<S: Store + 'static> RaftNode<S> {
    /// Creates a new leader node for the Raft cluster.
    ///
    /// This function initializes a new `RaftNode` instance as a leader. It sets up the Raft configuration,
    /// applies a default snapshot to initialize the state, and sets the node to be a leader.
    ///
    /// # Parameters
    /// - `rcv`: A receiver for Raft messages. This will be used to receive incoming messages.
    /// - `snd`: A sender for Raft messages. This will be used to send outgoing messages.
    /// - `id`: The unique identifier for this Raft node.
    /// - `store`: The store implementation used for persisting Raft state.
    /// - `logger`: A logger instance for logging messages related to the Raft node.
    /// - `cfg`: Configuration for the Raft node, including various timeouts and limits.
    ///
    /// # Returns
    /// Returns a `Result` containing either the newly created `RaftNode` or an error if the creation failed.
    pub async fn new_leader(
        rcv: mpsc::Receiver<Message>,
        snd: mpsc::Sender<Message>,
        id: u64,
        store: S,
        logger: &slog::Logger,
        cfg: Arc<Config>,
    ) -> Result<Self> {
        let config = Self::new_config(id, &cfg.raft_cfg);
        config.validate()?;

        let mut s = Snapshot::default();
        // Because we don't use the same configuration to initialize every node, so we use
        // a non-zero index to force new followers catch up logs by snapshot first, which will
        // bring all nodes to the same initial state.
        s.mut_metadata().index = 1;
        s.mut_metadata().term = 1;
        s.mut_metadata().mut_conf_state().voters = vec![id];
        s.set_data(store.snapshot().await?);

        let mut storage: MemStorage = MemStorage::create();
        storage.apply_snapshot(s)?;
        let mut inner = RawNode::new(&config, storage, logger)?;
        let peers = HashMap::new();
        let seq = AtomicU64::new(0);
        let sending_raft_messages = Arc::new(AtomicIsize::new(0));
        let last_snap_time = Instant::now(); // + cfg.snapshot_interval;

        inner.raft.become_candidate();
        inner.raft.become_leader();

        // let msg_tx = Self::start_message_sender();
        let uncommitteds = HashMap::new();
        let node = RaftNode {
            inner,
            rcv,
            peers,
            store,
            // msg_tx,
            uncommitteds,
            seq,
            snd,
            should_quit: false,
            sending_raft_messages,
            last_snap_time,
            cfg,
            timeout_recorder: TimeoutRecorder::new(Duration::from_secs(15), 5),
            propose_counter: Counter::new(Duration::from_secs(3)),
        };
        Ok(node)
    }

    /// Creates a new follower node for the Raft cluster.
    ///
    /// This function initializes a new `RaftNode` instance as a follower. It sets up the Raft configuration
    /// and creates a new `RawNode` instance in follower mode.
    ///
    /// # Parameters
    /// - `rcv`: A receiver for Raft messages. This will be used to receive incoming messages.
    /// - `snd`: A sender for Raft messages. This will be used to send outgoing messages.
    /// - `id`: The unique identifier for this Raft node.
    /// - `store`: The store implementation used for persisting Raft state.
    /// - `logger`: A logger instance for logging messages related to the Raft node.
    /// - `cfg`: Configuration for the Raft node, including various timeouts and limits.
    ///
    /// # Returns
    /// Returns a `Result` containing either the newly created `RaftNode` or an error if the creation failed.
    pub fn new_follower(
        rcv: mpsc::Receiver<Message>,
        snd: mpsc::Sender<Message>,
        id: u64,
        store: S,
        logger: &slog::Logger,
        cfg: Arc<Config>,
    ) -> Result<Self> {
        let config = Self::new_config(id, &cfg.raft_cfg);
        config.validate()?;

        let storage = MemStorage::create();
        let inner = RawNode::new(&config, storage, logger)?;
        let peers = HashMap::new();
        let seq = AtomicU64::new(0);
        let sending_raft_messages = Arc::new(AtomicIsize::new(0));
        let last_snap_time = Instant::now(); // + cfg.snapshot_interval;
                                             // let msg_tx = Self::start_message_sender();
        let uncommitteds = HashMap::new();
        Ok(RaftNode {
            inner,
            rcv,
            peers,
            store,
            // msg_tx,
            uncommitteds,
            seq,
            snd,
            should_quit: false,
            sending_raft_messages,
            last_snap_time,
            cfg,
            timeout_recorder: TimeoutRecorder::new(Duration::from_secs(10), 5),
            propose_counter: Counter::new(Duration::from_secs(3)),
        })
    }

    /// Creates a new Raft configuration with the specified node ID.
    ///
    /// This function clones the provided configuration and sets the node ID.
    ///
    /// # Parameters
    /// - `id`: The unique identifier for the Raft node.
    /// - `cfg`: The base Raft configuration to clone and modify.
    ///
    /// # Returns
    /// Returns a `RaftConfig` with the updated node ID.
    #[inline]
    fn new_config(id: u64, cfg: &RaftConfig) -> RaftConfig {
        let mut cfg = cfg.clone();
        cfg.id = id;
        cfg
    }

    /// Retrieves a peer by its ID.
    ///
    /// This function looks up a peer in the `peers` map by its ID.
    ///
    /// # Parameters
    /// - `id`: The ID of the peer to retrieve.
    ///
    /// # Returns
    /// Returns an `Option<Peer>`. If the peer is found, it is returned; otherwise, `None` is returned.
    #[inline]
    pub fn peer(&self, id: u64) -> Option<Peer> {
        match self.peers.get(&id) {
            Some(Some(p)) => Some(p.clone()),
            _ => None,
        }
    }

    /// Checks if the current node is the leader.
    ///
    /// This function compares the leader ID of the Raft instance with the current node's ID.
    ///
    /// # Returns
    /// Returns `true` if the current node is the leader, otherwise `false`.
    #[inline]
    pub fn is_leader(&self) -> bool {
        self.inner.raft.leader_id == self.inner.raft.id
    }

    /// Retrieves the ID of the current node.
    ///
    /// This function returns the unique identifier of the current Raft node.
    ///
    /// # Returns
    /// Returns the node's ID as a `u64`.
    #[inline]
    pub fn id(&self) -> u64 {
        self.raft.id
    }

    /// Adds a new peer to the `peers` map.
    ///
    /// This function creates a new `Peer` instance with the specified address and configuration,
    /// and adds it to the `peers` map.
    ///
    /// # Parameters
    /// - `addr`: The address of the new peer.
    /// - `id`: The unique identifier for the new peer.
    ///
    /// # Returns
    /// Returns the newly created `Peer` instance.
    #[inline]
    pub fn add_peer(&mut self, addr: &str, id: u64) -> Peer {
        let peer = Peer::new(
            addr.to_string(),
            self.cfg.grpc_timeout,
            self.cfg.grpc_concurrency_limit,
            self.cfg.grpc_message_size,
            self.cfg.grpc_breaker_threshold,
            self.cfg.grpc_breaker_retry_interval.as_millis() as i64,
        );
        self.peers.insert(id, Some(peer.clone()));
        peer
    }

    #[inline]
    fn leader(&self) -> u64 {
        self.raft.leader_id
    }

    #[inline]
    fn has_leader(&self) -> bool {
        self.raft.leader_id > 0
    }

    #[inline]
    fn peer_addrs(&self) -> HashMap<u64, String> {
        self.peers
            .iter()
            .filter_map(|(&id, peer)| {
                peer.as_ref()
                    .map(|Peer { addr, .. }| (id, addr.to_string()))
            })
            .collect()
    }

    #[inline]
    fn peer_states(&self) -> HashMap<u64, Option<PeerState>> {
        self.peers
            .iter()
            .map(|(id, peer)| {
                if let Some(p) = peer {
                    (
                        *id,
                        Some(PeerState {
                            addr: p.addr.clone(),
                            available: !p.is_unavailable(),
                        }),
                    )
                } else {
                    (*id, None)
                }
            })
            .collect()
    }

    #[inline]
    async fn status(&self, merger_proposals: usize) -> Status {
        let role = self.raft.state;
        let leader_id = self.raft.leader_id;
        let sending_raft_messages = self.sending_raft_messages.load(Ordering::SeqCst);
        let timeout_max = self.timeout_recorder.max() as isize;
        let timeout_recent_count = self.timeout_recorder.recent_get() as isize;
        let propose_count = self.propose_counter.count();
        let propose_rate = self.propose_counter.rate();
        Status {
            id: self.inner.raft.id,
            leader_id,
            uncommitteds: self.uncommitteds.len(),
            merger_proposals,
            sending_raft_messages,
            timeout_max,
            timeout_recent_count,
            propose_count,
            propose_rate,
            peers: self.peer_states(),
            role,
        }
    }

    // forward query request to leader
    #[inline]
    async fn forward_query(&self, query: Vec<u8>, chan: oneshot::Sender<RaftResponse>) {
        let id = self.leader();
        let peer = match self.peer(id) {
            Some(peer) => peer,
            None => {
                if let Err(e) = chan.send(RaftResponse::WrongLeader {
                    leader_id: id,
                    leader_addr: None,
                }) {
                    warn!(
                        "forward_query, Message::Query, RaftResponse send error: {:?}",
                        e
                    );
                }
                return;
            }
        };

        let query_sender = QuerySender {
            query,
            client: peer,
            chan,
            timeout: Duration::from_millis(1000),
            max_retries: 0,
        };
        tokio::spawn(query_sender.send());
    }

    #[inline]
    async fn send_query(&self, query: &[u8], chan: oneshot::Sender<RaftResponse>) {
        let data = self.store.query(query).await.unwrap_or_default();
        if let Err(e) = chan.send(RaftResponse::Response { data }) {
            warn!("Message::Query, RaftResponse send error: {:?}", e);
        }
    }

    #[inline]
    fn send_wrong_leader(&self, from: &str, chan: oneshot::Sender<RaftResponse>) {
        let leader_id = self.leader();
        // leader can't be an empty node
        let leader_addr = self
            .peers
            .get(&leader_id)
            .and_then(|peer| peer.as_ref().map(|p| p.addr.to_string()));
        let raft_response = RaftResponse::WrongLeader {
            leader_id,
            leader_addr,
        };
        if let Err(e) = chan.send(raft_response) {
            warn!(
                "send_wrong_leader, from: {}, RaftResponse send error: {:?}",
                from, e
            );
        }
    }

    #[inline]
    fn send_is_busy(&self, chan: oneshot::Sender<RaftResponse>) {
        if let Err(e) = chan.send(RaftResponse::Busy) {
            warn!("send_is_busy, RaftResponse send error: {:?}", e);
        }
    }

    #[inline]
    fn _send_error(&self, chan: oneshot::Sender<RaftResponse>, e: String) {
        let raft_response = RaftResponse::Error(e);
        if let Err(e) = chan.send(raft_response) {
            warn!("send_error, RaftResponse send error: {:?}", e);
        }
    }

    #[inline]
    fn send_leader_id(&self, chan: oneshot::Sender<RaftResponse>) {
        if let Err(e) = chan.send(RaftResponse::RequestId {
            leader_id: self.leader(),
        }) {
            warn!("Message::RequestId, RaftResponse send error: {:?}", e);
        }
    }

    #[inline]
    async fn send_status(&self, merger: &Merger, chan: oneshot::Sender<RaftResponse>) {
        if let Err(e) = chan.send(RaftResponse::Status(self.status(merger.len()).await)) {
            warn!("Message::Status, RaftResponse send error: {:?}", e);
        }
    }

    #[inline]
    fn _take_and_propose(&mut self, merger: &mut Merger) -> Result<()> {
        if let Some((data, reply_chans)) = merger.take() {
            let seq = self.seq.fetch_add(1, Ordering::Relaxed);
            self.uncommitteds.insert(seq, reply_chans);
            let seq = serialize(&seq).map_err(|e| anyhow!(e))?;
            let data = serialize(&data).map_err(|e| anyhow!(e))?;
            self.propose(seq, data)?;
        }
        Ok(())
    }

    #[inline]
    fn take_and_propose(&mut self, merger: &mut Merger) {
        if let Err(e) = self._take_and_propose(merger) {
            error!("propose to raft error, {:?}", e);
        }
    }

    #[inline]
    fn is_busy(&self) -> bool {
        self.sending_raft_messages.load(Ordering::SeqCst)
            > self.cfg.raft_cfg.max_inflight_msgs as isize
            || self.timeout_recorder.recent_get() > 0
    }

    pub(crate) async fn run(mut self) -> Result<()> {
        let mut heartbeat = self.cfg.heartbeat;
        let mut now = Instant::now();
        let mut snapshot_received = self.is_leader();
        let mut merger = Merger::new(
            self.cfg.proposal_batch_size,
            self.cfg.proposal_batch_timeout,
        );
        info!("snapshot_received: {:?}", snapshot_received);
        info!("has_leader: {:?}", self.has_leader());

        loop {
            if self.should_quit {
                warn!("Quitting raft");
                return Ok(());
            }

            match timeout(heartbeat, self.rcv.next()).await {
                Ok(Some(Message::ConfigChange { chan, mut change })) => {
                    info!("change Received, {:?}", change);
                    // whenever a change id is 0, it's a message to self.
                    if change.get_node_id() == 0 {
                        change.set_node_id(self.id());
                    }

                    if !self.is_leader() {
                        // wrong leader send client cluster data
                        // TODO: retry strategy in case of failure
                        self.send_wrong_leader("ConfigChange", chan);
                    } else {
                        // leader assign new id to peer
                        info!("received request from: {}", change.get_node_id());
                        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
                        self.uncommitteds
                            .insert(seq, ReplyChan::One((chan, Instant::now())));
                        match serialize(&seq) {
                            Ok(req) => {
                                if let Err(e) = self.propose_conf_change(req, change) {
                                    warn!("propose_conf_change, error: {:?}", e);
                                }
                            }
                            Err(e) => {
                                warn!("serialize seq, error: {:?}", e);
                            }
                        };
                    }
                }
                Ok(Some(Message::Raft(m))) => {
                    let msg_type = m.get_msg_type();
                    if msg_type != MessageType::MsgHeartbeat
                        && msg_type != MessageType::MsgHeartbeatResponse
                    {
                        debug!(
                            "raft message: to={} from={} msg_type={:?}, commit={}, {:?}",
                            self.raft.id,
                            m.from,
                            m.msg_type,
                            m.get_commit(),
                            m
                        );
                    }
                    if MessageType::MsgTransferLeader == msg_type {
                        info!(
                            "raft message MsgTransferLeader, snapshot_received: {}, raft.leader_id: {}, {:?}",
                            snapshot_received, self.raft.leader_id, m
                        );
                    }

                    if !snapshot_received && msg_type == MessageType::MsgHeartbeat {
                        debug!(
                            "raft message, snapshot_received: {}, has_leader: {}, {:?}",
                            snapshot_received,
                            self.has_leader(),
                            m
                        );
                    } else {
                        if let Err(e) = self.step(*m) {
                            warn!(
                                "step error, {:?}, msg_type: {:?}, snapshot_received: {}",
                                e, msg_type, snapshot_received
                            );
                        }
                        if msg_type == MessageType::MsgSnapshot {
                            snapshot_received = true;
                        }
                    }
                }
                Ok(Some(Message::Propose { proposal, chan })) => {
                    self.propose_counter.inc();
                    let now = Instant::now();
                    if !self.is_leader() {
                        debug!("Message::Propose, send_wrong_leader {:?}", proposal);
                        self.send_wrong_leader("Propose", chan);
                    } else if self.is_busy() {
                        self.send_is_busy(chan);
                    } else {
                        merger.add(proposal, chan);
                        self.take_and_propose(&mut merger);
                    }
                    if now.elapsed() > self.cfg.heartbeat {
                        info!("Message::Propose elapsed: {:?}", now.elapsed());
                    }
                }

                Ok(Some(Message::Query { query, chan })) => {
                    let now = Instant::now();
                    if !self.is_leader() {
                        debug!("[forward_query] query.len: {:?}", query.len());
                        self.forward_query(query, chan).await;
                    } else {
                        debug!("Message::Query, {:?}", query);
                        self.send_query(&query, chan).await;
                    }
                    if now.elapsed() > self.cfg.heartbeat {
                        info!("Message::Query elapsed: {:?}", now.elapsed());
                    }
                }

                Ok(Some(Message::RequestId { chan })) => {
                    debug!("requested Id, is_leader: {}", self.is_leader());
                    if !self.is_leader() {
                        self.send_wrong_leader("RequestId", chan);
                    } else {
                        self.send_leader_id(chan);
                    }
                }
                Ok(Some(Message::Status { chan })) => {
                    self.send_status(&merger, chan).await;
                }
                Ok(Some(Message::Snapshot { snapshot })) => {
                    self.set_snapshot(snapshot);
                }
                Ok(Some(Message::ReportUnreachable { node_id })) => {
                    debug!(
                        "Message::ReportUnreachable, node_id: {}, sending_raft_messages: {}",
                        node_id,
                        self.sending_raft_messages.load(Ordering::SeqCst)
                    );
                    self.report_unreachable(node_id);
                }
                Ok(None) => {
                    error!("Recv None");
                    return Err(Error::RecvError("Recv None".into()));
                }
                Err(_) => {
                    self.take_and_propose(&mut merger);
                }
            }

            let elapsed = now.elapsed();
            now = Instant::now();

            if elapsed >= heartbeat {
                if elapsed > Duration::from_millis(500) {
                    warn!(
                        "[run] raft tick elapsed: {:?}, heartbeat: {:?}, uncommitteds: {}, sending_raft_messages: {}",
                        elapsed,
                        heartbeat,
                        self.uncommitteds.len(),
                        self.sending_raft_messages.load(Ordering::SeqCst),
                    );
                }
                heartbeat = self.cfg.heartbeat;
                self.tick();
            } else {
                heartbeat -= elapsed;
            }

            let on_ready_now = Instant::now();
            if let Err(e) = self.on_ready().await {
                error!(
                    "raft on_ready(..) error: {:?}, elapsed: {:?}",
                    e,
                    on_ready_now.elapsed()
                );
                return Err(e);
            }
            if on_ready_now.elapsed() > Duration::from_millis(500) {
                warn!(
                    "[run] raft on_ready(..) uncommitteds: {}, sending_raft_messages: {}, elapsed: {:?}",
                    self.uncommitteds.len(),
                    self.sending_raft_messages.load(Ordering::SeqCst),
                    on_ready_now.elapsed()
                );
            }
        }
    }

    async fn on_ready(&mut self) -> Result<()> {
        if !self.has_ready() {
            return Ok(());
        }

        let mut ready = self.ready();

        if !ready.messages().is_empty() {
            // Send out the messages.
            self.send_messages(ready.take_messages());
        }

        if *ready.snapshot() != Snapshot::default() {
            let snapshot = ready.snapshot();
            log::info!(
                "snapshot metadata: {:?}, data len: {}",
                snapshot.get_metadata(),
                snapshot.get_data().len()
            );
            self.store.restore(snapshot.get_data()).await?;
            let store = self.mut_store();
            store.apply_snapshot(snapshot.clone())?;
        }

        self.handle_committed_entries(ready.take_committed_entries())
            .await?;

        if !ready.entries().is_empty() {
            let entries = ready.entries();
            let store = self.mut_store();
            store.append(entries)?;
        }

        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            let store = self.mut_store();
            store.set_hard_state(hs)?;
        }

        if !ready.persisted_messages().is_empty() {
            // Send out the persisted messages come from the node.
            self.send_messages(ready.take_persisted_messages());
        }
        let mut light_rd = self.advance(ready);

        if let Some(commit) = light_rd.commit_index() {
            let store = self.mut_store();
            store.set_hard_state_comit(commit)?;
        }
        // Send out the messages.
        self.send_messages(light_rd.take_messages());
        // Apply all committed entries.
        self.handle_committed_entries(light_rd.take_committed_entries())
            .await?;
        self.advance_apply();

        Ok(())
    }

    fn send_messages(&mut self, msgs: Vec<RaftMessage>) {
        for message in msgs {
            // for message in ready.messages.drain(..) {
            let client_id = message.get_to();
            let client = match self.peer(client_id) {
                Some(peer) => peer,
                None => continue,
            };

            let message_sender = MessageSender {
                message,
                client,
                client_id,
                chan: self.snd.clone(),
                max_retries: 0,
                timeout: Duration::from_millis(500),
                sending_raft_messages: self.sending_raft_messages.clone(),
            };
            tokio::spawn(message_sender.send());
        }
    }

    async fn handle_committed_entries(&mut self, committed_entries: Vec<Entry>) -> Result<()> {
        // Fitler out empty entries produced by new elected leaders.
        let committed_entries_count = committed_entries.len();
        let now = std::time::Instant::now();
        for entry in committed_entries {
            if entry.data.is_empty() {
                // From new elected leaders.
                continue;
            }
            if let EntryType::EntryConfChange = entry.get_entry_type() {
                self.handle_config_change(&entry).await?;
            } else {
                self.handle_normal(&entry).await?;
            }
        }

        if now.elapsed().as_millis() > 500 {
            log::info!(
                "[handle_committed_entries] uncommitteds.len(): {}, sending_raft_messages: {}, \
                committed_entries_count: {}, raft.inflight_buffers_size: {}, \
                raft.msgs: {}, raft.group_commit: {}, raft.pending_read_count: {}, raft.ready_read_count: {}, \
                 raft.soft_state: {:?}, raft.hard_state: {:?}, raft.state: {:?}, raft.heartbeat_elapsed: {}, \
                 self.raft.read_states: {}, raft.heartbeat_timeout: {}, raft.heartbeat_elapsed: {}, \
                 cost time: {:?}",
                self.uncommitteds.len(), self.sending_raft_messages.load(Ordering::SeqCst),
                committed_entries_count, self.raft.inflight_buffers_size(), self.raft.msgs.len(),
                self.raft.group_commit(), self.raft.pending_read_count(), self.raft.ready_read_count(),
                self.raft.soft_state(), self.raft.hard_state(), self.raft.state, self.raft.heartbeat_elapsed(),
                self.raft.read_states.len(), self.raft.heartbeat_timeout(), self.raft.heartbeat_elapsed(),
                now.elapsed()
            );
        }
        Ok(())
    }

    #[inline]
    async fn handle_config_change(&mut self, entry: &Entry) -> Result<()> {
        info!("handle_config_change, entry: {:?}", entry);
        let seq: u64 = deserialize(entry.get_context())?;
        info!("handle_config_change, seq: {:?}", seq);
        let change = ConfChange::decode(entry.get_data())
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;
        info!("handle_config_change, change: {:?}", change);
        let id = change.get_node_id();

        let change_type = change.get_change_type();

        match change_type {
            ConfChangeType::AddNode => {
                let addr: String = deserialize(change.get_context())?;
                info!("adding {} ({}) to peers", addr, id);
                self.add_peer(&addr, id);
            }
            ConfChangeType::RemoveNode => {
                let ctx = change.get_context();
                let typ = if !ctx.is_empty() {
                    deserialize(change.get_context())?
                } else {
                    RemoveNodeType::Normal
                };
                info!("removing ({}) to peers, RemoveNodeType: {:?}", id, typ);
                if id == self.id() {
                    if !matches!(typ, RemoveNodeType::Stale) {
                        self.should_quit = true;
                        warn!("quiting the cluster");
                    }
                } else {
                    self.peers.remove(&id);
                }
            }
            _ => {
                warn!("unimplemented! change_type: {:?}", change_type);
            }
        }

        if let Ok(cs) = self.apply_conf_change(&change) {
            info!("conf state: {cs:?}, id: {id}, this id: {}", self.id());
            if matches!(change_type, ConfChangeType::AddNode) {
                let store = self.mut_store();
                store.set_conf_state(&cs)?;
                if id != self.id() {
                    let snap = self.generate_snapshot_sync().await?;
                    self.set_snapshot(snap);
                }
            } else {
                let store = self.mut_store();
                store.set_conf_state(&cs)?;
            }
        }

        if let Some(sender) = self.uncommitteds.remove(&seq) {
            let response = match change_type {
                ConfChangeType::AddNode => RaftResponse::JoinSuccess {
                    assigned_id: id,
                    peer_addrs: self.peer_addrs(),
                },
                ConfChangeType::RemoveNode => RaftResponse::Ok,
                _ => {
                    warn!("unimplemented! change_type: {:?}", change_type);
                    RaftResponse::Error("unimplemented".into())
                }
            };
            if let ReplyChan::One((sender, _)) = sender {
                if sender.send(response).is_err() {
                    warn!("error sending response")
                }
            }
        }
        Ok(())
    }

    #[inline]
    async fn handle_normal(&mut self, entry: &Entry) -> Result<()> {
        let seq: u64 = deserialize(entry.get_context())?;
        debug!(
            "[handle_normal] seq:{}, senders.len(): {}",
            seq,
            self.uncommitteds.len()
        );

        match (
            deserialize::<Proposals>(entry.get_data())?,
            self.uncommitteds.remove(&seq),
        ) {
            (Proposals::One(data), chan) => {
                let apply_start = std::time::Instant::now();
                let reply =
                    tokio::time::timeout(Duration::from_secs(5), self.store.apply(&data)).await;
                if apply_start.elapsed().as_secs() > 2 {
                    self.timeout_recorder.incr();
                    log::warn!("apply, cost time: {:?}", apply_start.elapsed());
                }
                if let Some(ReplyChan::One((chan, inst))) = chan {
                    if inst.elapsed().as_secs() > 2 {
                        self.timeout_recorder.incr();
                        debug!(
                                "[handle_normal] cost time, {:?}, chan is canceled: {}, uncommitteds: {}, sending_raft_messages: {}",
                                inst.elapsed(),
                                chan.is_canceled(),
                                self.uncommitteds.len(),
                                self.sending_raft_messages.load(Ordering::SeqCst)
                            );
                    }
                    if !chan.is_canceled() {
                        let reply = reply.unwrap_or_else(|e| Err(Error::from(e)));
                        let res = match reply {
                            Ok(data) => RaftResponse::Response { data },
                            Err(e) => RaftResponse::Error(e.to_string()),
                        };
                        if let Err(_resp) = chan.send(res) {
                            warn!(
                                "[handle_normal] send RaftResponse error, seq:{}, uncommitteds: {}, sending_raft_messages: {}",
                                seq,
                                self.uncommitteds.len(),
                                self.sending_raft_messages.load(Ordering::SeqCst)
                            );
                        }
                    }
                }
            }
            (Proposals::More(mut datas), chans) => {
                let mut chans = if let Some(ReplyChan::More(chans)) = chans {
                    Some(chans)
                } else {
                    None
                };
                while let Some(data) = datas.pop() {
                    let apply_start = std::time::Instant::now();
                    let reply =
                        tokio::time::timeout(Duration::from_secs(5), self.store.apply(&data)).await;
                    if apply_start.elapsed().as_secs() > 2 {
                        self.timeout_recorder.incr();
                        log::warn!("apply, cost time: {:?}", apply_start.elapsed());
                    }
                    if let Some((chan, inst)) = chans.as_mut().and_then(|cs| cs.pop()) {
                        if inst.elapsed().as_secs() > 2 {
                            self.timeout_recorder.incr();
                            debug!(
                                "[handle_normal] cost time, {:?}, chan is canceled: {}, uncommitteds: {}, sending_raft_messages: {}",
                                inst.elapsed(),
                                chan.is_canceled(),
                                self.uncommitteds.len(),
                                self.sending_raft_messages.load(Ordering::SeqCst)
                            );
                        }
                        if !chan.is_canceled() {
                            let reply = reply.unwrap_or_else(|e| Err(Error::from(e)));
                            let res = match reply {
                                Ok(data) => RaftResponse::Response { data },
                                Err(e) => RaftResponse::Error(e.to_string()),
                            };
                            if let Err(_resp) = chan.send(res) {
                                warn!("[handle_normal] send RaftResponse error, seq:{}", seq,);
                            }
                        }
                    }
                }
            }
        }

        if Instant::now() > self.last_snap_time + self.cfg.snapshot_interval {
            self.last_snap_time = Instant::now();
            info!("gen snapshot start");
            self.generate_snapshot_async()?;
        }
        Ok(())
    }

    async fn generate_snapshot_sync(&mut self) -> Result<Snapshot> {
        let last_applied = self.raft.raft_log.applied;
        let last_term = self.raft.raft_log.term(last_applied).unwrap_or(0);
        let mut snapshot = self.mut_store().create_snapshot(last_applied, last_term)?;

        let now = Instant::now();
        let snap = match self.store.snapshot().await {
            Err(e) => {
                log::error!("gen snapshot error, {e:?}");
                return Err(e);
            }
            Ok(snap) => snap,
        };
        info!(
            "gen snapshot cost time: {:?}, snapshot len: {}, last_applied: {last_applied}",
            now.elapsed(),
            snap.len()
        );

        snapshot.set_data(snap);

        Ok(snapshot)
    }

    fn generate_snapshot_async(&mut self) -> Result<()> {
        let store = self.store.clone();
        let mut tx = self.snd.clone();
        let last_applied = self.raft.raft_log.applied;
        let last_term = self.raft.raft_log.term(last_applied).unwrap_or(0);
        let mut snapshot = self.mut_store().create_snapshot(last_applied, last_term)?;

        tokio::spawn(async move {
            let now = Instant::now();
            let snap = match store.snapshot().await {
                Err(e) => {
                    log::error!("gen snapshot error, {e:?}");
                    return;
                }
                Ok(snap) => snap,
            };
            info!(
                "gen snapshot cost time: {:?}, snapshot len: {}, last_applied: {last_applied}",
                now.elapsed(),
                snap.len()
            );

            snapshot.set_data(snap);

            if let Err(e) = tx.send(Message::Snapshot { snapshot }).await {
                log::error!("send snapshot error, {e:?}");
            }
        });

        Ok(())
    }

    fn set_snapshot(&mut self, snap: Snapshot) {
        let store = self.mut_store();
        let last_applied = snap.get_metadata().index;
        store.set_snapshot(snap);
        if let Err(e) = store.compact(last_applied) {
            error!("compact error, {e}");
        }
        info!("set snapshot,last_applied: {last_applied}");
    }
}

impl<S: Store> Deref for RaftNode<S> {
    type Target = RawNode<MemStorage>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S: Store> DerefMut for RaftNode<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
