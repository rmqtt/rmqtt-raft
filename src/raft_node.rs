use std::collections::HashMap;
use std::collections::vec_deque::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bincode::{deserialize, serialize};
use futures::channel::{mpsc, oneshot};
use futures::SinkExt;
use futures::StreamExt;
use log::*;
use raft::{Config, prelude::*, raw_node::RawNode};
use raft::eraftpb::{ConfChange, ConfChangeType, Entry, EntryType, Message as RaftMessage};
use tokio::sync::RwLock;
use tokio::time::timeout;
use tonic::Request;
use tonic::transport::{Channel, Endpoint};

use crate::error::{Error, Result};
use crate::message::{Merger, Message, Proposals, RaftResponse, ReplyChan, Status};
use crate::raft::{active_mailbox_querys, active_mailbox_sends};
use crate::raft::Store;
use crate::raft_server::{send_message_active_requests, send_proposal_active_requests};
use crate::raft_service::{Message as RraftMessage, Proposal as RraftProposal, Query};
use crate::raft_service::raft_service_client::RaftServiceClient;
use crate::storage::{LogStore, MemStorage};

pub type RaftGrpcClient = RaftServiceClient<tonic::transport::channel::Channel>;

struct MessageSender {
    message: RaftMessage,
    client: Peer,
    client_id: u64,
    chan: mpsc::Sender<Message>,
    max_retries: usize,
    timeout: Duration,
}

impl MessageSender {
    /// attempt to send a message MessageSender::max_retries times at MessageSender::timeout
    /// inteval.
    async fn send(mut self) {
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
                            "error sending message after {}/{} retries: {:?}",
                            current_retry, self.max_retries, e
                        );
                        if let Err(e) = self
                            .chan
                            .send(Message::ReportUnreachable {
                                node_id: self.client_id,
                            })
                            .await
                        {
                            warn!(
                                "error ReportUnreachable after {}/{} retries: {:?}",
                                current_retry, self.max_retries, e
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
                    "error sending query after, {:?}", e
                );
                if let Err(e) = self.chan.send(RaftResponse::Error(e.to_string())) {
                    warn!(
                        "send_query, Message::Query, RaftResponse send error: {:?}",
                        e
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
                    let raft_response =
                        deserialize(&grpc_response.into_inner().inner).expect("deserialize error");
                    if let Err(e) = self.chan.send(raft_response) {
                        warn!(
                            "send_query, Message::Query, RaftResponse send error: {:?}",
                            e
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
                            "error sending query after {} retries: {}",
                            self.max_retries, e
                        );
                        if let Err(e) = self.chan.send(RaftResponse::Error(e.to_string())) {
                            warn!(
                                "send_query, Message::Query, RaftResponse send error: {:?}",
                                e
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
    addr: String,
    client: Arc<RwLock<Option<RaftGrpcClient>>>,
    grpc_fails: Arc<AtomicU64>,
    grpc_fail_time: Arc<AtomicI64>,
    crw_timeout: Duration,
    max_concurrency: usize,
    active_tasks: Arc<AtomicI64>,
}

impl Peer {
    pub fn new(addr: String) -> Peer {
        debug!("connecting to node at {}...", addr);
        let crw_timeout = Duration::from_secs(5); //@TODO configurable
        let max_concurrency = 200;
        Peer {
            addr,
            client: Arc::new(RwLock::new(None)),
            grpc_fails: Arc::new(AtomicU64::new(0)),
            grpc_fail_time: Arc::new(AtomicI64::new(0)),
            crw_timeout,
            max_concurrency,
            active_tasks: Arc::new(AtomicI64::new(0)),
        }
    }

    #[inline]
    pub fn active_tasks(&self) -> i64 {
        self.active_tasks.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn grpc_fails(&self) -> u64 {
        self.grpc_fails.load(Ordering::SeqCst)
    }

    #[inline]
    fn _endpoint(&self) -> Result<Endpoint> {
        let endpoint = Channel::from_shared(format!("http://{}", self.addr))
            .map(|endpoint| {
                endpoint
                    .concurrency_limit(self.max_concurrency)
                    .timeout(self.crw_timeout)
            })
            .map_err(|e| Error::Other(Box::new(e)))?;
        Ok(endpoint)
    }

    #[inline]
    async fn _connect(endpoint: &Endpoint, crw_timeout: Duration) -> Result<RaftGrpcClient> {
        let channel = tokio::time::timeout(crw_timeout, endpoint.connect())
            .await
            .map_err(|e| Error::Other(Box::new(e)))?
            .map_err(|e| Error::Other(Box::new(e)))?;
        let client = RaftServiceClient::new(channel);
        Ok(client)
    }

    #[inline]
    async fn connect(&self) -> Result<RaftGrpcClient> {
        if let Some(c) = self.client.read().await.as_ref() {
            return Ok(c.clone());
        }

        let mut client = self.client.write().await;
        if let Some(c) = client.as_ref() {
            return Ok(c.clone());
        }

        let endpoint = self._endpoint()?;
        let c = Self::_connect(&endpoint, self.crw_timeout).await?;
        client.replace(c.clone());
        Ok(c)
    }

    #[inline]
    pub async fn client(&self) -> Result<RaftGrpcClient> {
        self.connect().await
    }

    ///Raft Message
    #[inline]
    pub async fn send_message(&self, msg: &RaftMessage) -> Result<Vec<u8>> {
        if !self.available() {
            return Err(Error::Msg("The gRPC remote service is unavailable".into()));
        }

        let msg = RraftMessage { inner: protobuf::Message::write_to_bytes(msg)? };
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

        let result = tokio::time::timeout(
            Duration::from_secs(15),  //@TODO configurable
            task(c, msg),
        ).await;
        let result = result.map_err(|_| Error::Elapsed)??;
        Ok(result)
    }

    #[inline]
    pub async fn send_proposal(&self, msg: Vec<u8>) -> Result<Vec<u8>> {
        if !self.available() {
            return Err(Error::Msg("The gRPC remote service is unavailable".into()));
        }

        let msg = RraftProposal { inner: msg };
        let _active_tasks = self.active_tasks.fetch_add(1, Ordering::SeqCst);
        // if active_tasks > self.max_concurrency as i64 {
        //     self.active_tasks.fetch_sub(1, Ordering::SeqCst);
        //     return Err(Error::Msg(format!("gRPC service busy, active tasks is {}", active_tasks)));
        // }
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

        let result = tokio::time::timeout(
            self.crw_timeout,
            task(c, msg),
        ).await;
        let result = result.map_err(|_| Error::Elapsed)??;
        Ok(result)
    }


    #[inline]
    pub fn _addr(&self) -> &str {
        &self.addr
    }


    #[inline]
    pub fn record_failure(&self) {
        self.grpc_fails.fetch_add(1, Ordering::SeqCst);
        self.grpc_fail_time
            .store(chrono::Local::now().timestamp_millis(), Ordering::SeqCst);
    }


    #[inline]
    pub fn record_success(&self) {
        self.grpc_fails.store(0, Ordering::SeqCst);
    }


    #[inline]
    pub fn available(&self) -> bool {
        self.grpc_fails.load(Ordering::SeqCst) < 4  //@TODO configurable
            || (chrono::Local::now().timestamp_millis()
            - self.grpc_fail_time.load(Ordering::SeqCst))
            > 3000 //@TODO configurable
    }
}

pub struct RaftNode<S: Store> {
    inner: RawNode<MemStorage>,
    pub peers: HashMap<u64, Option<Peer>>,
    pub rcv: mpsc::Receiver<Message>,
    pub snd: mpsc::Sender<Message>,
    store: S,
    #[allow(dead_code)]
    msg_tx: mpsc::Sender<MessageSender>,
    uncommitteds: HashMap<u64, ReplyChan>,
    should_quit: bool,
    seq: AtomicU64,
    last_snap_time: Instant,
}

impl<S: Store + 'static> RaftNode<S> {
    pub fn new_leader(
        rcv: mpsc::Receiver<Message>,
        snd: mpsc::Sender<Message>,
        id: u64,
        store: S,
        logger: &slog::Logger,
    ) -> Self {
        let config = Self::new_config(id, 10, 5);  //@TODO configurable

        config.validate().unwrap();

        let mut s = Snapshot::default();
        // Because we don't use the same configuration to initialize every node, so we use
        // a non-zero index to force new followers catch up logs by snapshot first, which will
        // bring all nodes to the same initial state.
        s.mut_metadata().index = 1;
        s.mut_metadata().term = 1;
        s.mut_metadata().mut_conf_state().voters = vec![id];

        let mut storage: MemStorage = MemStorage::create();
        storage.apply_snapshot(s).unwrap();
        let mut inner = RawNode::new(&config, storage, logger).unwrap();
        let peers = HashMap::new();
        let seq = AtomicU64::new(0);
        let last_snap_time = Instant::now() + Duration::from_secs(600); //@TODO configurable

        inner.raft.become_candidate();
        inner.raft.become_leader();

        let msg_tx = Self::start_message_sender();
        let uncommitteds = HashMap::new();
        RaftNode {
            inner,
            rcv,
            peers,
            store,
            msg_tx,
            uncommitteds,
            seq,
            snd,
            should_quit: false,
            last_snap_time,
        }
    }

    pub fn new_follower(
        rcv: mpsc::Receiver<Message>,
        snd: mpsc::Sender<Message>,
        id: u64,
        store: S,
        logger: &slog::Logger,
    ) -> Result<Self> {
        let config = Self::new_config(id, 10, 5);

        config.validate().unwrap();

        let storage = MemStorage::create();
        let inner = RawNode::new(&config, storage, logger)?;
        let peers = HashMap::new();
        let seq = AtomicU64::new(0);
        let last_snap_time = Instant::now() + Duration::from_secs(600);  //@TODO configurable
        let msg_tx = Self::start_message_sender();
        let uncommitteds = HashMap::new();
        Ok(RaftNode {
            inner,
            rcv,
            peers,
            store,
            msg_tx,
            uncommitteds,
            seq,
            snd,
            should_quit: false,
            last_snap_time,
        })
    }

    fn start_message_sender() -> mpsc::Sender<MessageSender> {
        let (tx, mut rx): (mpsc::Sender<MessageSender>, mpsc::Receiver<MessageSender>) =
            mpsc::channel(1000);

        tokio::spawn(async move {
            use std::sync::atomic::AtomicBool;
            type Queues = HashMap<u64, (Arc<AtomicBool>, VecDeque<MessageSender>), ahash::RandomState>;
            let mut queues: Queues = Queues::default();

            let sends = |queues: &mut Queues| {
                for (to, (sending, q)) in queues.iter_mut() {
                    if sending.load(Ordering::SeqCst) {
                        continue;
                    }
                    if !q.is_empty() {
                        log::debug!(
                            "to: {}, sending: {}, q.len: {}",
                            to,
                            sending.load(Ordering::SeqCst),
                            q.len()
                        );
                    }
                    if let Some(msg) = q.pop_front() {
                        let sending = sending.clone();
                        sending.store(true, Ordering::SeqCst);
                        tokio::spawn(async move {
                            msg.send().await;
                            sending.store(false, Ordering::SeqCst);
                        });
                    }
                }
            };

            loop {
                match timeout(Duration::from_millis(10), rx.next()).await {  //@TODO configurable
                    Ok(Some(msg)) => {
                        let (_, q) = queues
                            .entry(msg.client_id)
                            .or_insert((Arc::new(AtomicBool::new(false)), VecDeque::new()));
                        q.push_back(msg);
                        if q.len() > 300 {  //@TODO configurable
                            warn!("There is too much backlog of unsent messages, {}", q.len())
                        }
                        sends(&mut queues);
                    }
                    Ok(None) => {
                        log::error!("start_message_sender, recv None");
                        break;
                    }
                    Err(_) => {
                        sends(&mut queues);
                    }
                }
            }
        });

        tx
    }

    #[inline]
    fn new_config(id: u64, election_tick: usize, heartbeat_tick: usize) -> Config {
        Config {
            id,
            election_tick,
            heartbeat_tick,
            check_quorum: true,
            pre_vote: true,
            ..Default::default()
        }
    }

    #[inline]
    pub fn peer(&self, id: u64) -> Option<Peer> {
        match self.peers.get(&id) {
            Some(Some(p)) => Some(p.clone()),
            _ => None,
        }
    }

    #[inline]
    pub fn is_leader(&self) -> bool {
        self.inner.raft.leader_id == self.inner.raft.id
    }

    #[inline]
    pub fn id(&self) -> u64 {
        self.raft.id
    }

    #[inline]
    pub fn add_peer(&mut self, addr: &str, id: u64) -> Peer {
        let peer = Peer::new(addr.to_string());
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
    fn status(&self) -> Status {
        debug!("raft status.ss: {:?}", self.inner.status().ss);
        let leader_id = self.raft.leader_id;
        Status {
            id: self.inner.raft.id,
            leader_id,
            uncommitteds: self.uncommitteds.len(),
            active_mailbox_sends: active_mailbox_sends(),
            active_mailbox_querys: active_mailbox_querys(),
            active_send_proposal_grpc_requests: send_proposal_active_requests(),
            active_send_message_grpc_requests: send_message_active_requests(),
            peers: self.peer_addrs(),
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
        // tokio::spawn(async move{
        let data = self.store.query(query).await.unwrap_or_default();
        if let Err(e) = chan.send(RaftResponse::Response {
            data
        }) {
            warn!("Message::Query, RaftResponse send error: {:?}", e);
        }
        // });
    }

    #[inline]
    fn send_wrong_leader(&self, chan: oneshot::Sender<RaftResponse>) {
        let leader_id = self.leader();
        // leader can't be an empty node
        let leader_addr = self
            .peers
            .get(&leader_id)
            .and_then(|peer| peer.as_ref().map(|p| p.addr.clone()));
        let raft_response = RaftResponse::WrongLeader {
            leader_id,
            leader_addr,
        };
        if let Err(e) = chan.send(raft_response) {
            warn!("send_wrong_leader, RaftResponse send error: {:?}", e);
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
    fn send_status(&self, chan: oneshot::Sender<RaftResponse>) {
        if let Err(e) = chan.send(RaftResponse::Status(self.status())) {
            warn!("Message::Status, RaftResponse send error: {:?}", e);
        }
    }

    #[inline]
    fn take_and_propose(&mut self, merger: &mut Merger) {
        if let Some((data, reply_chans)) = merger.take() {
            let seq = self.seq.fetch_add(1, Ordering::Relaxed);
            self.uncommitteds.insert(seq, reply_chans);
            let seq = serialize(&seq).unwrap();
            let data = serialize(&data).unwrap();
            if let Err(e) = self.propose(seq, data) {
                error!("propose to raft error, {:?}", e);
            }
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let mut heartbeat = Duration::from_millis(100);  //@TODO configurable
        let mut now = Instant::now();
        let mut snapshot_received = self.is_leader();
        let mut merger = Merger::new();
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
                        self.send_wrong_leader(chan);
                    } else {
                        // leader assign new id to peer
                        info!("received request from: {}", change.get_node_id());
                        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
                        self.uncommitteds.insert(seq, ReplyChan::One((chan, Instant::now())));
                        if let Err(e) = self.propose_conf_change(serialize(&seq).unwrap(), change) {
                            warn!("propose_conf_change, error: {:?}", e);
                        }
                    }
                }
                Ok(Some(Message::Raft(m))) => {
                    debug!(
                        "raft message: to={} from={} msg_type={:?}, commit={}, {:?}",
                        self.raft.id,
                        m.from,
                        m.msg_type,
                        m.get_commit(),
                        m
                    );
                    let msg_type = m.get_msg_type();
                    if !snapshot_received && msg_type == MessageType::MsgHeartbeat {
                        info!(
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
                    if !self.is_leader() {
                        debug!("Message::Propose, send_wrong_leader {:?}", proposal);
                        self.send_wrong_leader(chan);
                    } else {
                        merger.add(proposal, chan);
                        self.take_and_propose(&mut merger);
                    }
                }

                Ok(Some(Message::Query { query, chan })) => {
                    if !self.is_leader() {
                        debug!("[forward_query] query.len: {:?}", query.len());
                        self.forward_query(query, chan).await;
                    } else {
                        debug!("Message::Query, {:?}", query);
                        self.send_query(&query, chan).await;
                    }
                }

                Ok(Some(Message::RequestId { chan })) => {
                    if !self.is_leader() {
                        // TODO: retry strategy in case of failure
                        info!("requested Id, but not leader");
                        self.send_wrong_leader(chan);
                    } else {
                        self.send_leader_id(chan);
                    }
                }
                Ok(Some(Message::Status { chan })) => {
                    self.send_status(chan);
                }
                Ok(Some(Message::ReportUnreachable { node_id })) => {
                    debug!("Message::ReportUnreachable, node_id: {}", node_id);
                    self.report_unreachable(node_id);
                }
                Ok(None) => {
                    error!("Recv None");
                    return Err(Error::RecvError("Recv None".into()));
                    //unreachable!()
                }
                Err(_) => {
                    self.take_and_propose(&mut merger);
                }
            }

            let elapsed = now.elapsed();
            now = Instant::now();
            if elapsed > heartbeat {
                heartbeat = Duration::from_millis(100);
                if elapsed > Duration::from_millis(500) {
                    warn!("raft tick elapsed: {:?}", elapsed);
                }
                self.tick();
            } else {
                heartbeat -= elapsed;
            }

            let on_ready_now = Instant::now();
            if let Err(e) = self.on_ready().await {
                error!("raft on_ready(..) error: {:?}, elapsed: {:?}", e, on_ready_now.elapsed());
                return Err(e);
            }
            if on_ready_now.elapsed() > Duration::from_millis(200) {
                warn!("raft on_ready(..) elapsed: {:?}", on_ready_now.elapsed());
            }
        }
    }

    #[inline]
    async fn on_ready(
        &mut self,
    ) -> Result<()> {
        if !self.has_ready() {
            return Ok(());
        }

        let mut ready = self.ready();

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

        //for message in ready.take_messages() {
        for message in ready.messages.drain(..) {
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
                max_retries: 5,
                timeout: Duration::from_millis(300),
            };
            // if let Err(e) = self.msg_tx.try_send(message_sender) {
            //     log::warn!("msg_tx.try_send, error: {:?}", e.to_string());
            // }
            tokio::spawn(message_sender.send());
        }

        if !ready.snapshot().is_empty() {
            let snapshot = ready.snapshot();
            self.store.restore(snapshot.get_data()).await?;
            let store = self.mut_store();
            store.apply_snapshot(snapshot.clone())?;
        }

        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            let store = self.mut_store();
            store.set_hard_state(hs)?;
        }

        if let Some(committed_entries) = ready.committed_entries.take() {
            // if let Some(committed_entries) = ready.take_committed_entries() {
            //log::info!("on_ready, committed_entries: {}", committed_entries.len());
            // let committed_entries_len = committed_entries.len();
            // let mut _last_apply_index = 0;
            for entry in committed_entries {
                //for entry in ready.take_committed_entries() {
                // Mostly, you need to save the last apply index to resume applying
                // after restart. Here we just ignore this because we use a Memory storage.
                // _last_apply_index = entry.get_index();
                debug!(
                    "entry.get_entry_type(): {:?}, entry.get_data().is_empty():{}",
                    entry.get_entry_type(),
                    entry.get_data().is_empty(),
                );
                if entry.get_data().is_empty() {
                    // Emtpy entry, when the peer becomes Leader it will send an empty entry.
                    continue;
                }
                match entry.get_entry_type() {
                    EntryType::EntryNormal => self.handle_normal(&entry).await?,
                    EntryType::EntryConfChange => {
                        self.handle_config_change(&entry).await?
                    }
                    EntryType::EntryConfChangeV2 => unimplemented!(),
                }
            }
        }

        self.advance(ready);
        Ok(())
    }

    #[inline]
    async fn handle_config_change(
        &mut self,
        entry: &Entry,
    ) -> Result<()> {
        info!("handle_config_change, entry: {:?}", entry);
        let seq: u64 = deserialize(entry.get_context())?;
        let change: ConfChange = protobuf::Message::parse_from_bytes(entry.get_data())?;
        let id = change.get_node_id();

        let change_type = change.get_change_type();

        match change_type {
            ConfChangeType::AddNode => {
                let addr: String = deserialize(change.get_context())?;
                info!("adding {} ({}) to peers", addr, id);
                self.add_peer(&addr, id);
            }
            ConfChangeType::RemoveNode => {
                if change.get_node_id() == self.id() {
                    self.should_quit = true;
                    warn!("quiting the cluster");
                } else {
                    self.peers.remove(&change.get_node_id());
                }
            }
            _ => unimplemented!(),
        }

        if let Ok(cs) = self.apply_conf_change(&change) {
            let last_applied = self.raft.raft_log.applied;
            let snapshot = self.store.snapshot().await?;
            {
                let store = self.mut_store();
                store.set_conf_state(&cs)?;
                store.compact(last_applied)?;
                let _ = store.create_snapshot(snapshot)?;
            }
        }

        if let Some(sender) = self.uncommitteds.remove(&seq) {
            let response = match change_type {
                ConfChangeType::AddNode => RaftResponse::JoinSuccess {
                    assigned_id: id,
                    peer_addrs: self.peer_addrs(),
                },
                ConfChangeType::RemoveNode => RaftResponse::Ok,
                _ => unimplemented!(),
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
    async fn handle_normal(
        &mut self,
        entry: &Entry,
    ) -> Result<()> {
        let seq: u64 = deserialize(entry.get_context())?;
        debug!(
            "[handle_normal] seq:{}, senders.len(): {}",
            seq, self.uncommitteds.len()
        );

        match (deserialize::<Proposals>(entry.get_data())?, self.uncommitteds.remove(&seq)) {
            (Proposals::One(data), chan) => {
                let reply = self.store.apply(&data).await;
                if let Some(ReplyChan::One((chan, inst))) = chan {
                    let res = match reply{
                        Ok(data) => RaftResponse::Response { data },
                        Err(e) => RaftResponse::Error(e.to_string()),
                    };
                    if let Err(_resp) = chan.send(res) {
                        warn!(
                            "[handle_normal] send RaftResponse error, seq:{}, cost time: {:?}", seq, inst.elapsed()
                        );
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
                    let reply = self.store.apply(&data).await;
                    if let Some((chan, inst)) = chans.as_mut().and_then(|cs| cs.pop()) {
                        if inst.elapsed().as_secs() > 3{
                            warn!(
                                "[handle_normal] cost time, {:?}, chan is canceled: {}",
                                inst.elapsed(), chan.is_canceled()
                            );
                        }
                        let res = match reply{
                            Ok(data) => RaftResponse::Response { data },
                            Err(e) => RaftResponse::Error(e.to_string()),
                        };
                        if let Err(_resp) = chan.send(res) {
                            warn!(
                                "[handle_normal] send RaftResponse error, seq:{}, cost time: {:?}", seq, inst.elapsed()
                            );
                        }
                    }
                }
            }
        }

        debug!(
            "creating snapshot, now: {:?}, last_snap_time: {:?}",
            Instant::now(),
            self.last_snap_time
        );
        if Instant::now() > self.last_snap_time + Duration::from_secs(600) {  //@TODO configurable
            //@TODO 600secs
            self.last_snap_time = Instant::now();
            let last_applied = self.raft.raft_log.applied;
            let snapshot = self.store.snapshot().await?;
            let store = self.mut_store();
            store.compact(last_applied)?;
            let first_index = store.first_index().unwrap_or(0);
            let last_index = store.last_index().unwrap_or(0);
            let result = store.create_snapshot(snapshot);
            info!(
                "create snapshot cost time: {:?}, first_index: {:?}, last_index: {:?}, {}, create snapshot result: {:?}",
                Instant::now() - self.last_snap_time,
                first_index,
                last_index,
                (last_index as i64 - first_index as i64),
                result
            );
        }
        Ok(())
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
