#![allow(clippy::result_large_err)]

use std::time::Duration;

// Re-exporting necessary types and modules for external use.
pub use crate::error::{Error, Result};
pub use crate::message::Status;
pub use crate::raft::{Mailbox, Raft, Store};
pub use tikv_raft::{ReadOnlyOption, StateRole};

// Importing modules for internal use.
mod error;
mod message;
mod raft;
mod raft_node;
mod raft_server;
mod raft_service;
mod storage;
mod timeout_recorder;

/// Configuration options for the Raft-based system.
#[derive(Clone)]
pub struct Config {
    #[cfg(feature = "reuseaddr")]
    /// Whether to reuse local addresses. This option is enabled only if the `reuseaddr` feature is active.
    pub reuseaddr: bool,

    #[cfg(all(
        feature = "reuseport",
        not(any(target_os = "solaris", target_os = "illumos"))
    ))]
    /// Whether to reuse local ports. This option is enabled only if the `reuseport` feature is active
    /// and the target OS is not Solaris or Illumos.
    pub reuseport: bool,

    /// The timeout duration for gRPC calls.
    pub grpc_timeout: Duration,

    /// The maximum number of concurrent gRPC calls.
    pub grpc_concurrency_limit: usize,

    /// The maximum size of gRPC messages in bytes.
    pub grpc_message_size: usize,

    /// The threshold for the gRPC circuit breaker. If the number of failed requests exceeds this threshold,
    /// the circuit breaker will trip.
    pub grpc_breaker_threshold: u64,

    /// The interval at which the gRPC circuit breaker will retry after tripping.
    pub grpc_breaker_retry_interval: Duration,

    /// The maximum number of proposals to batch together before processing.
    pub proposal_batch_size: usize,

    /// The timeout duration for collecting proposals into a batch. If this timeout is reached,
    /// the collected proposals will be processed regardless of the batch size.
    pub proposal_batch_timeout: Duration,

    /// The interval at which snapshots are generated.
    pub snapshot_interval: Duration,

    /// The interval at which heartbeat messages are sent to maintain leader election and cluster health.
    pub heartbeat: Duration,

    /// Configuration options for the Raft protocol.
    pub raft_cfg: tikv_raft::Config,
}

impl Default for Config {
    /// Provides default values for the `Config` struct.
    fn default() -> Self {
        Self {
            #[cfg(feature = "reuseaddr")]
            reuseaddr: false,

            #[cfg(all(
                feature = "reuseport",
                not(any(target_os = "solaris", target_os = "illumos"))
            ))]
            reuseport: false,

            grpc_timeout: Duration::from_secs(6),
            grpc_concurrency_limit: 200,
            grpc_message_size: 50 * 1024 * 1024, // 50 MB
            grpc_breaker_threshold: 4,
            grpc_breaker_retry_interval: Duration::from_millis(2500),
            proposal_batch_size: 50,
            proposal_batch_timeout: Duration::from_millis(200),
            snapshot_interval: Duration::from_secs(600),
            heartbeat: Duration::from_millis(100),
            raft_cfg: tikv_raft::Config {
                election_tick: 10,
                heartbeat_tick: 5,
                check_quorum: true,
                pre_vote: true,
                ..Default::default()
            },
        }
    }
}
