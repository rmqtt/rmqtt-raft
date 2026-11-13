//! Real loopback regression coverage for the fork's public lifecycle APIs.
//! These tests create only in-memory stores and listeners on 127.0.0.1.

use std::collections::HashSet;
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{anyhow, ensure, Context, Result};
use async_trait::async_trait;
use futures::future::join_all;
use rmqtt_raft::{Config, Error, Mailbox, Raft, Status, Store};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, Instant};

const POLL: Duration = Duration::from_millis(40);
const WAIT: Duration = Duration::from_secs(12);

#[derive(Default)]
struct Stored {
    commands: Vec<Vec<u8>>,
    restores: usize,
}

#[derive(Clone, Default)]
struct MemoryStore(Arc<Mutex<Stored>>);

impl MemoryStore {
    fn commands(&self) -> Vec<Vec<u8>> {
        self.0.lock().unwrap().commands.clone()
    }

    fn restores(&self) -> usize {
        self.0.lock().unwrap().restores
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn apply(&mut self, message: &[u8]) -> rmqtt_raft::Result<Vec<u8>> {
        self.0.lock().unwrap().commands.push(message.to_vec());
        Ok(message.to_vec())
    }

    async fn query(&self, _query: &[u8]) -> rmqtt_raft::Result<Vec<u8>> {
        postcard::to_stdvec(&self.commands()).map_err(|error| error.to_string().into())
    }

    async fn snapshot(&self) -> rmqtt_raft::Result<Vec<u8>> {
        postcard::to_stdvec(&self.commands()).map_err(|error| error.to_string().into())
    }

    async fn restore(&mut self, snapshot: &[u8]) -> rmqtt_raft::Result<()> {
        let commands = postcard::from_bytes(snapshot).map_err(|error| error.to_string())?;
        let mut stored = self.0.lock().unwrap();
        stored.commands = commands;
        stored.restores += 1;
        Ok(())
    }
}

struct RunningNode {
    id: u64,
    addr: SocketAddr,
    mailbox: Mailbox,
    store: MemoryStore,
    runtime: Option<JoinHandle<rmqtt_raft::Result<()>>>,
}

impl Drop for RunningNode {
    fn drop(&mut self) {
        // Also release listeners when an assertion or deadline fails.
        if let Some(runtime) = &self.runtime {
            runtime.abort();
        }
    }
}

fn unused_addr() -> SocketAddr {
    static USED: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();
    loop {
        // Keep the reservation live until its port is recorded. Avoid selecting
        // the late leader's still-unbound port again for another test node.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        if USED
            .get_or_init(Mutex::default)
            .lock()
            .unwrap()
            .insert(addr.port())
        {
            return addr;
        }
    }
}

fn config() -> Config {
    Config {
        grpc_timeout: Duration::from_secs(2),
        join_connect_retry_timeout: Duration::from_secs(8),
        grpc_breaker_retry_interval: Duration::from_millis(100),
        proposal_batch_timeout: Duration::from_millis(10),
        snapshot_interval: Duration::from_millis(500),
        ..Config::default()
    }
}

fn start_node(id: u64, addr: SocketAddr, leader: Option<(u64, SocketAddr)>) -> Result<RunningNode> {
    let store = MemoryStore::default();
    let logger = slog::Logger::root(slog::Discard, slog::o!());
    let raft = Raft::new(addr, store.clone(), logger, config())?;
    let mailbox = raft.mailbox();
    let runtime = tokio::spawn(async move {
        match leader {
            Some((leader_id, leader_addr)) => {
                raft.join_as_learner(
                    id,
                    addr.to_string(),
                    Some(leader_id),
                    leader_addr.to_string(),
                )
                .await
            }
            None => raft.lead(id, addr.to_string()).await,
        }
    });
    Ok(RunningNode {
        id,
        addr,
        mailbox,
        store,
        runtime: Some(runtime),
    })
}

async fn wait_statuses(
    nodes: &[RunningNode],
    description: &str,
    condition: impl Fn(&[Status]) -> bool,
) -> Result<Vec<Status>> {
    let deadline = Instant::now() + WAIT;
    let mut last = String::new();
    while Instant::now() < deadline {
        let observations = join_all(
            nodes
                .iter()
                .map(|node| timeout(Duration::from_millis(400), node.mailbox.status())),
        )
        .await;
        let mut statuses = Vec::new();
        for (node, observation) in nodes.iter().zip(observations) {
            match observation {
                Ok(Ok(status)) => statuses.push(status),
                other => last = format!("node {}: {other:?}", node.id),
            }
        }
        if statuses.len() == nodes.len() {
            if condition(&statuses) {
                return Ok(statuses);
            }
            last = format!("{statuses:?}");
        }
        sleep(POLL).await;
    }
    Err(anyhow!("timed out waiting for {description}: {last}"))
}

fn members(status: &Status, voters: &[u64], learners: &[u64]) -> bool {
    let mut actual_voters = status.voters.clone();
    let mut actual_learners = status.learners.clone();
    actual_voters.sort_unstable();
    actual_learners.sort_unstable();
    actual_voters == voters && actual_learners == learners
}

fn status_postcard_roundtrip(status: &Status) -> Result<()> {
    let wire = postcard::to_stdvec(status)?;
    let restored: Status = postcard::from_bytes(&wire)?;
    ensure!(restored.id == status.id && restored.leader_id == status.leader_id);
    ensure!(
        restored.role == status.role,
        "status role changed during serialization"
    );
    ensure!(restored.voters == status.voters && restored.learners == status.learners);
    ensure!(
        (
            restored.applied_index,
            restored.committed_index,
            restored.last_index
        ) == (
            status.applied_index,
            status.committed_index,
            status.last_index
        ),
        "status log progress changed during serialization"
    );
    ensure!(
        (restored.snapshot_restored, restored.snapshot_index)
            == (status.snapshot_restored, status.snapshot_index),
        "status snapshot progress changed during serialization"
    );
    ensure!(restored.peer_replication.len() == status.peer_replication.len());
    for (id, expected) in &status.peer_replication {
        let actual = restored
            .peer_replication
            .get(id)
            .context("missing serialized peer progress")?;
        ensure!(
            (
                actual.matched,
                actual.next_index,
                actual.committed_index,
                actual.pending_snapshot,
                actual.recent_active,
                actual.paused,
                actual.state.as_str()
            ) == (
                expected.matched,
                expected.next_index,
                expected.committed_index,
                expected.pending_snapshot,
                expected.recent_active,
                expected.paused,
                expected.state.as_str()
            ),
            "replication progress changed during serialization for peer {id}"
        );
    }
    Ok(())
}

async fn propose(node: &RunningNode, command: &[u8]) -> Result<()> {
    let reply = timeout(WAIT, node.mailbox.send_proposal(command.to_vec()))
        .await
        .context("proposal deadline")??;
    ensure!(
        reply == command,
        "state machine returned a different proposal result"
    );
    Ok(())
}

async fn stop_nodes(nodes: &mut [RunningNode], removed: &[u64]) -> Result<()> {
    // Request all shutdowns together so the cleanup does not rely on a quorum
    // continuing to exist while individual runtimes are awaited.
    let results = join_all(
        nodes
            .iter()
            .map(|node| timeout(WAIT, node.mailbox.shutdown())),
    )
    .await;
    for (node, result) in nodes.iter().zip(results) {
        let result = result.with_context(|| format!("shutdown deadline for {}", node.id))?;
        if !removed.contains(&node.id) {
            result.with_context(|| format!("shutdown response for {}", node.id))?;
        }
    }
    for node in nodes.iter_mut() {
        // Leave the handle in the node while awaiting it: cancellation of the
        // test's outer deadline must still abort it via RunningNode::drop.
        let runtime = node.runtime.as_mut().unwrap();
        timeout(WAIT, runtime)
            .await
            .with_context(|| format!("runtime exit deadline for {}", node.id))?
            .with_context(|| format!("runtime panicked for {}", node.id))??;
        node.runtime.take();
        let listener = TcpListener::bind(node.addr)
            .with_context(|| format!("listener leaked on node {} at {}", node.id, node.addr))?;
        drop(listener);
    }
    Ok(())
}

async fn membership_scenario() -> Result<()> {
    let mut nodes = vec![start_node(1, unused_addr(), None)?];
    wait_statuses(&nodes, "initial leader", |s| {
        s[0].is_leader() && s[0].local_caught_up()
    })
    .await?;
    for command in [b"before-join-a".as_slice(), b"before-join-b".as_slice()] {
        propose(&nodes[0], command).await?;
    }

    nodes.push(start_node(2, unused_addr(), Some((1, nodes[0].addr)))?);
    let learner_statuses = wait_statuses(&nodes, "learner 2 snapshot and catch-up", |s| {
        s.iter()
            .all(|status| members(status, &[1], &[2]) && status.local_caught_up())
            && s[0].peer_caught_up(2)
            && nodes[1].store.commands() == nodes[0].store.commands()
    })
    .await?;
    for status in &learner_statuses {
        status_postcard_roundtrip(status)?;
    }
    ensure!(
        nodes[1].store.restores() > 0,
        "learner did not restore an initial snapshot"
    );
    ensure!(
        nodes[0].mailbox.status().await?.available(),
        "learner changed voter quorum availability"
    );
    nodes[0]
        .mailbox
        .promote_learner(2, nodes[1].addr.to_string())
        .await?;
    wait_statuses(&nodes, "learner 2 promotion", |s| {
        s.iter().all(|status| members(status, &[1, 2], &[]))
    })
    .await?;

    nodes.push(start_node(3, unused_addr(), Some((1, nodes[0].addr)))?);
    wait_statuses(&nodes, "learner 3 snapshot and catch-up", |s| {
        s.iter()
            .all(|status| members(status, &[1, 2], &[3]) && status.local_caught_up())
            && s[0].peer_caught_up(3)
    })
    .await?;
    nodes[0]
        .mailbox
        .promote_learner(3, nodes[2].addr.to_string())
        .await?;
    wait_statuses(&nodes, "three-voter membership", |s| {
        s.iter().all(|status| members(status, &[1, 2, 3], &[]))
    })
    .await?;
    propose(&nodes[0], b"all-three-voters").await?;
    wait_statuses(&nodes, "replicated state on all voters", |s| {
        s.iter().all(Status::local_caught_up)
            && nodes
                .iter()
                .all(|node| node.store.commands() == nodes[0].store.commands())
            && s[0].peer_caught_up(2)
    })
    .await?;

    // The follower must forward proposals and read queries through actual gRPC
    // and decode the 0.7.2 postcard response, not merely the local mailbox path.
    propose(&nodes[2], b"forwarded-from-follower").await?;
    wait_statuses(&nodes, "follower proposal replicated on all voters", |s| {
        s.iter().all(Status::local_caught_up)
            && nodes
                .iter()
                .all(|node| node.store.commands() == nodes[0].store.commands())
    })
    .await?;
    let response = timeout(WAIT, nodes[2].mailbox.query(Vec::new()))
        .await
        .context("follower query deadline")??;
    let queried: Vec<Vec<u8>> = postcard::from_bytes(&response)?;
    ensure!(
        queried == nodes[0].store.commands(),
        "follower query returned different application state"
    );
    ensure!(queried.last().map(Vec::as_slice) == Some(b"forwarded-from-follower".as_slice()));

    // Acceptance of the request is distinct from observing the new leader.
    nodes[0].mailbox.transfer_leader(2).await?;
    wait_statuses(&nodes, "leadership transfer to node 2", |s| {
        s.iter().all(|status| status.leader_id == 2) && s[1].is_leader()
    })
    .await?;
    ensure!(
        matches!(nodes[0].mailbox.remove_node(3).await, Err(Error::NotLeader)),
        "follower did not reject removal as NotLeader"
    );
    ensure!(
        matches!(
            nodes[0].mailbox.transfer_leader(3).await,
            Err(Error::NotLeader)
        ),
        "follower did not reject leader transfer as NotLeader"
    );
    ensure!(
        matches!(
            nodes[0]
                .mailbox
                .promote_learner(3, nodes[2].addr.to_string())
                .await,
            Err(Error::NotLeader)
        ),
        "follower did not reject promotion as NotLeader"
    );
    nodes[1].mailbox.remove_node(3).await?;
    wait_statuses(&nodes[..2], "node 3 removal", |s| {
        s.iter().all(|status| members(status, &[1, 2], &[]))
    })
    .await?;
    propose(&nodes[1], b"after-removal").await?;
    wait_statuses(&nodes[..2], "replication after removal", |s| {
        s.iter().all(Status::local_caught_up)
            && nodes[0].store.commands() == nodes[1].store.commands()
            && nodes[0].store.commands().last().map(Vec::as_slice)
                == Some(b"after-removal".as_slice())
    })
    .await?;
    stop_nodes(&mut nodes, &[3]).await
}

#[tokio::test]
async fn learner_membership_replication_and_shutdown() -> Result<()> {
    timeout(Duration::from_secs(50), membership_scenario())
        .await
        .context("membership scenario exceeded its total deadline")?
}

#[tokio::test]
async fn learner_retries_initial_connection_until_leader_is_available() -> Result<()> {
    timeout(Duration::from_secs(25), async {
        let leader_addr = unused_addr();
        let learner = start_node(2, unused_addr(), Some((1, leader_addr)))?;
        // Deliberately leave the leader address closed for the first attempts.
        // Readiness itself is always established from status, not this delay.
        sleep(Duration::from_millis(250)).await;
        let leader = start_node(1, leader_addr, None)?;
        let mut nodes = vec![leader, learner];
        wait_statuses(&nodes, "retrying learner to join the late leader", |s| {
            s.iter()
                .all(|status| members(status, &[1], &[2]) && status.local_caught_up())
                && s[0].peer_caught_up(2)
        })
        .await?;
        propose(&nodes[0], b"after-connect-retry").await?;
        wait_statuses(&nodes, "replication after connection retry", |_| {
            nodes[0].store.commands() == nodes[1].store.commands()
                && !nodes[0].store.commands().is_empty()
        })
        .await?;
        stop_nodes(&mut nodes, &[]).await
    })
    .await
    .context("connection retry scenario exceeded its total deadline")?
}
