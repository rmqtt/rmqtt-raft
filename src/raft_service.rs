use std::time::Duration;

use tonic::transport::{Channel, Endpoint};

use raft_service_client::RaftServiceClient;

use crate::error::Result;

tonic::include_proto!("raftservice");

pub(crate) type RaftServiceClientType = RaftServiceClient<Channel>;

#[inline]
pub(crate) fn endpoint(saddr: &str, concurrency_limit: usize, timeout: Duration) -> Result<Endpoint> {
    let endpoint = Channel::from_shared(format!("http://{}", saddr))
        .map(|endpoint| {
            endpoint
                .concurrency_limit(concurrency_limit)
                .connect_timeout(timeout)
                .timeout(timeout)
        }).map_err(anyhow::Error::new)?;
    Ok(endpoint)
}

#[inline]
pub(crate) async fn connect(saddr: &str, concurrency_limit: usize, timeout: Duration) -> Result<RaftServiceClientType> {
    Ok(RaftServiceClientType::new(endpoint(saddr, concurrency_limit, timeout)?.connect().await?))
}