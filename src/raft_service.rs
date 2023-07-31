use std::time::Duration;

use tonic::transport::{Channel, Endpoint};

use raft_service_client::RaftServiceClient;

use crate::error::Result;

tonic::include_proto!("raftservice");

pub(crate) type RaftServiceClientType = RaftServiceClient<Channel>;

#[inline]
pub(crate) fn endpoint(
    saddr: &str,
    concurrency_limit: usize,
    timeout: Duration,
) -> Result<Endpoint> {
    let endpoint = Channel::from_shared(format!("http://{}", saddr))
        .map(|endpoint| {
            endpoint
                .concurrency_limit(concurrency_limit)
                .connect_timeout(timeout)
                .timeout(timeout)
        })
        .map_err(anyhow::Error::new)?;
    Ok(endpoint)
}

#[inline]
pub(crate) async fn connect(
    saddr: &str,
    concurrency_limit: usize,
    timeout: Duration,
) -> Result<RaftServiceClientType> {
    Ok(RaftServiceClientType::new(
        endpoint(saddr, concurrency_limit, timeout)?
            .connect()
            .await?,
    ))
}

#[inline]
#[cfg(all(feature = "socket2", feature = "tokio-stream"))]
pub fn bind(
    laddr: std::net::SocketAddr,
    backlog: i32,
    _reuseaddr: bool,
    _reuseport: bool,
) -> anyhow::Result<tokio_stream::wrappers::TcpListenerStream> {
    use socket2::{Domain, SockAddr, Socket, Type};
    let builder = Socket::new(Domain::for_address(laddr), Type::STREAM, None)?;
    builder.set_nonblocking(true)?;
    #[cfg(unix)]
    #[cfg(feature = "reuseaddr")]
    builder.set_reuse_address(_reuseaddr)?;
    #[cfg(unix)]
    #[cfg(feature = "reuseport")]
    builder.set_reuse_port(_reuseport)?;
    builder.bind(&SockAddr::from(laddr))?;
    builder.listen(backlog)?;
    let listener = tokio_stream::wrappers::TcpListenerStream::new(
        tokio::net::TcpListener::from_std(std::net::TcpListener::from(builder))?,
    );
    Ok(listener)
}