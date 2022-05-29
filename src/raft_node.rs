use parking_lot::RwLock;
use std::collections::vec_deque::VecDeque;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bincode::{deserialize, serialize};
use log::*;
use raft::eraftpb::{ConfChange, ConfChangeType, Entry, EntryType, Message as RaftMessage};
use raft::{prelude::*, raw_node::RawNode, Config};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

use crate::error::{Error, Result};
use crate::message::{Message, RaftResponse, Status};
use crate::raft::Store;
use crate::raft_service::raft_service_client::RaftServiceClient;
use crate::raft_service::{Message as RiteraftMessage, Query};
use crate::storage::{LogStore, MemStorage};

pub type RaftGrpcClient = RaftServiceClient<tonic::transport::channel::Channel>;

struct MessageSender {
    message: RaftMessage,
    client: RaftGrpcClient,
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

        let message = protobuf::Message::write_to_bytes(&self.message).unwrap();
        let msg = RiteraftMessage { inner: message };
        loop {
            let message_request = Request::new(msg.clone());
            match self.client.send_message(message_request).await {
                Ok(_) => {
                    return;
                }
                Err(e) => {
                    error!(
                        "error sending message after {} retries: {}",
                        self.max_retries, e
                    );
                    if current_retry < self.max_retries {
                        current_retry += 1;
                        tokio::time::sleep(self.timeout).await;
                    } else {
                        error!(
                            "error sending message after {} retries: {}",
                            self.max_retries, e
                        );
                        if let Err(e) = self
                            .chan
                            .send(Message::ReportUnreachable {
                                node_id: self.client_id,
                            })
                            .await
                        {
                            error!(
                                "error sending message after {} retries: {}",
                                self.max_retries, e
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
    client: RaftGrpcClient,
    chan: oneshot::Sender<RaftResponse>,
    max_retries: usize,
    timeout: Duration,
}

impl QuerySender {
    async fn send(mut self) {
        let mut current_retry = 0usize;
        loop {
            let message_request = Request::new(Query {
                inner: self.query.clone(),
            });
            match self.client.send_query(message_request).await {
                Ok(grpc_response) => {
                    let raft_response =
                        deserialize(&grpc_response.into_inner().inner).expect("deserialize error");
                    if let Err(e) = self.chan.send(raft_response) {
                        error!(
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
                        error!(
                            "error sending proposal after {} retries: {}",
                            self.max_retries, e
                        );
                        if let Err(e) = self.chan.send(RaftResponse::Error) {
                            error!(
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
    client: Arc<RwLock<Option<RaftServiceClient<Channel>>>>,
}

impl Peer {
    pub fn new(addr: String) -> Peer {
        debug!("connecting to node at {}...", addr);
        Peer {
            addr,
            client: Arc::new(RwLock::new(None)),
        }
    }

    #[inline]
    fn _endpoint(&self) -> Result<Endpoint> {
        let concurrency_limit = 10;
        let client_timeout = Duration::from_secs(8);
        let endpoint = Channel::from_shared(format!("http://{}", self.addr))
            .map(|endpoint| {
                endpoint
                    .concurrency_limit(concurrency_limit)
                    .timeout(client_timeout)
            })
            .map_err(|e| Error::Other(Box::new(e)))?;
        Ok(endpoint)
    }

    #[inline]
    async fn _connect(endpoint: &Endpoint) -> Result<RaftServiceClient<Channel>> {
        let channel = tokio::time::timeout(Duration::from_secs(8), endpoint.connect())
            .await
            .map_err(|e| Error::Other(Box::new(e)))?
            .map_err(|e| Error::Other(Box::new(e)))?;
        let client = RaftServiceClient::new(channel);
        Ok(client)
    }

    #[inline]
    async fn connect(&self) -> Result<RaftServiceClient<Channel>> {
        if let Some(c) = self.client.read().as_ref() {
            return Ok(c.clone());
        }
        let endpoint = self._endpoint()?;
        let c = Self::_connect(&endpoint).await?;
        self.client.write().replace(c.clone());
        Ok(c)
    }

    #[inline]
    pub async fn client(&self) -> Result<RaftServiceClient<Channel>> {
        self.connect().await
    }

    #[inline]
    pub fn _addr(&self) -> &str {
        &self.addr
    }
}

pub struct RaftNode<S: Store> {
    inner: RawNode<MemStorage>,
    pub peers: HashMap<u64, Option<Peer>>,
    pub rcv: mpsc::Receiver<Message>,
    pub snd: mpsc::Sender<Message>,
    store: S,
    msg_tx: mpsc::Sender<MessageSender>,
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
        let config = Self::new_config(id, 10, 5);

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
        let last_snap_time = Instant::now() + Duration::from_secs(600);

        inner.raft.become_candidate();
        inner.raft.become_leader();

        let msg_tx = Self::start_message_sender();

        RaftNode {
            inner,
            rcv,
            peers,
            store,
            msg_tx,
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
        let last_snap_time = Instant::now() + Duration::from_secs(600);
        let msg_tx = Self::start_message_sender();
        Ok(RaftNode {
            inner,
            rcv,
            peers,
            store,
            msg_tx,
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
            type Queues = HashMap<u64, (Arc<AtomicBool>, VecDeque<MessageSender>)>;
            let mut queues: Queues = HashMap::new();

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
                match timeout(Duration::from_millis(10), rx.recv()).await {
                    Ok(Some(msg)) => {
                        let (_, q) = queues
                            .entry(msg.client_id)
                            .or_insert((Arc::new(AtomicBool::new(false)), VecDeque::new()));
                        q.push_back(msg);
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

    // #[inline]
    // pub fn peer_mut(&mut self, id: u64) -> Option<&mut Peer> {
    //     match self.peers.get_mut(&id) {
    //         None => None,
    //         Some(v) => v.as_mut(),
    //     }
    // }

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
        Status {
            id: self.inner.raft.id,
            leader_id: self.raft.leader_id,
            peers: self.peer_addrs(),
        }
    }

    // forward query request to leader
    #[inline]
    async fn forward_query(&self, query: Vec<u8>, chan: oneshot::Sender<RaftResponse>) {
        let id = self.leader();
        let leader_client = match self.peer(id) {
            Some(peer) => peer.client().await,
            None => {
                if let Err(e) = chan.send(RaftResponse::WrongLeader {
                    leader_id: id,
                    leader_addr: None,
                }) {
                    error!(
                        "forward_query, Message::Query, RaftResponse send error: {:?}",
                        e
                    );
                }
                return;
            }
        };

        let leader_client = match leader_client {
            Ok(leader_client) => leader_client,
            Err(_e) => {
                if let Err(e) = chan.send(RaftResponse::WrongLeader {
                    leader_id: id,
                    leader_addr: None,
                }) {
                    error!(
                        "forward_query, Message::Query, RaftResponse send error: {:?}",
                        e
                    );
                }
                return;
            }
        };

        let query_sender = QuerySender {
            query,
            client: leader_client,
            chan,
            timeout: Duration::from_millis(1000),
            max_retries: 3,
        };
        tokio::spawn(query_sender.send());
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
        // TODO handle error here
        if let Err(e) = chan.send(raft_response) {
            error!("send_wrong_leader, RaftResponse send error: {:?}", e);
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let mut heartbeat = Duration::from_millis(100);
        let mut now = Instant::now();
        // A map to contain sender to client responses
        let mut client_send = HashMap::new();
        let mut snapshot_received = self.is_leader();
        info!("snapshot_received: {:?}", snapshot_received);
        info!("has_leader: {:?}", self.has_leader());
        loop {
            if self.should_quit {
                warn!("Quitting raft");
                return Ok(());
            }
            match timeout(heartbeat, self.rcv.recv()).await {
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
                        client_send.insert(seq, chan);
                        if let Err(e) = self.propose_conf_change(serialize(&seq).unwrap(), change) {
                            error!("propose_conf_change, error: {:?}", e);
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
                            error!(
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
                        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
                        client_send.insert(seq, chan);
                        let seq = serialize(&seq).unwrap();
                        debug!(
                            "Message::Propose, seq: {:?}, client_send len: {}",
                            seq,
                            client_send.len()
                        );
                        self.propose(seq, proposal).unwrap();
                    }
                }

                Ok(Some(Message::Query { query, chan })) => {
                    if !self.is_leader() {
                        debug!("[forward_query] query.len: {:?}", query.len());
                        self.forward_query(query, chan).await;
                    } else {
                        debug!("Message::Query, {:?}", query);
                        if let Err(e) = chan.send(RaftResponse::Response {
                            data: self.store.query(&query).await.unwrap_or_default(),
                        }) {
                            error!("Message::Query, RaftResponse send error: {:?}", e);
                        }
                    }
                }

                Ok(Some(Message::RequestId { chan })) => {
                    if !self.is_leader() {
                        // TODO: retry strategy in case of failure
                        info!("requested Id, but not leader");
                        self.send_wrong_leader(chan);
                    } else {
                        //let id = self.reserve_next_peer_id();
                        if let Err(e) = chan.send(RaftResponse::RequestId {
                            leader_id: self.leader(),
                        }) {
                            error!("Message::RequestId, RaftResponse send error: {:?}", e);
                        }
                    }
                }
                Ok(Some(Message::Status { chan })) => {
                    if let Err(e) = chan.send(RaftResponse::Status(self.status())) {
                        error!("Message::Status, RaftResponse send error: {:?}", e);
                    }
                }
                Ok(Some(Message::ReportUnreachable { node_id })) => {
                    debug!("Message::ReportUnreachable, node_id: {}", node_id);
                    self.report_unreachable(node_id);
                }
                Ok(_) => unreachable!(),
                Err(_) => (),
            }

            let elapsed = now.elapsed();
            now = Instant::now();
            if elapsed > heartbeat {
                heartbeat = Duration::from_millis(100);
                self.tick();
            } else {
                heartbeat -= elapsed;
            }

            self.on_ready(&mut client_send).await?;
        }
    }

    #[inline]
    async fn on_ready(
        &mut self,
        client_send: &mut HashMap<u64, oneshot::Sender<RaftResponse>>,
    ) -> Result<()> {
        if !self.has_ready() {
            return Ok(());
        }

        let mut ready = self.ready();

        if !ready.entries().is_empty() {
            let entries = ready.entries();
            let store = self.mut_store();
            store.append(entries).unwrap();
        }

        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            let store = self.mut_store();
            store.set_hard_state(hs).unwrap();
        }

        for message in ready.messages.drain(..) {
            //for message in ready.take_messages() {
            let client = match self.peer(message.get_to()) {
                Some(peer) => peer.client().await,
                None => continue,
            };

            let client = match client {
                Ok(c) => c,
                Err(e) => {
                    log::warn!("node_id: {}, {:?}", message.get_to(), e);
                    continue;
                }
            };

            let message_sender = MessageSender {
                client_id: message.get_to(),
                client, //: client.clone(),
                chan: self.snd.clone(),
                message,
                timeout: Duration::from_millis(100),
                max_retries: 3,
            };
            if let Err(e) = self.msg_tx.try_send(message_sender) {
                log::warn!("msg_tx.try_send, error: {:?}", e.to_string());
            }
            // tokio::spawn(message_sender.send());
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
                    EntryType::EntryNormal => self.handle_normal(&entry, client_send).await?,
                    EntryType::EntryConfChange => {
                        self.handle_config_change(&entry, client_send).await?
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
        senders: &mut HashMap<u64, oneshot::Sender<RaftResponse>>,
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

        if let Some(sender) = senders.remove(&seq) {
            let response = match change_type {
                ConfChangeType::AddNode => RaftResponse::JoinSuccess {
                    assigned_id: id,
                    peer_addrs: self.peer_addrs(),
                },
                ConfChangeType::RemoveNode => RaftResponse::Ok,
                _ => unimplemented!(),
            };
            if sender.send(response).is_err() {
                error!("error sending response")
            }
        }
        Ok(())
    }

    #[inline]
    async fn handle_normal(
        &mut self,
        entry: &Entry,
        senders: &mut HashMap<u64, oneshot::Sender<RaftResponse>>,
    ) -> Result<()> {
        let seq: u64 = deserialize(entry.get_context())?;
        let data = self.store.apply(entry.get_data()).await?;
        if let Some(sender) = senders.remove(&seq) {
            debug!(
                "[handle_normal] apply reply data.len: {:?}, seq:{}",
                data.len(),
                seq
            );
            if let Err(resp) = sender.send(RaftResponse::Response { data }) {
                warn!(
                    "[handle_normal] send RaftResponse error, {:?}, seq:{}",
                    resp, seq
                );
            }
        }

        debug!(
            "creating snapshot, now: {:?}, last_snap_time: {:?}",
            Instant::now(),
            self.last_snap_time
        );
        if Instant::now() > self.last_snap_time + Duration::from_secs(600) {
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
