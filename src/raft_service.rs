use std::time::Duration;

use tonic::transport::{Channel, Endpoint};

use raft_service_client::RaftServiceClient;

use crate::error::Result;

tonic::include_proto!("raftservice");

pub(crate) type RaftServiceClientType = RaftServiceClient<Channel>;

/// Creates a gRPC `Endpoint` for connecting to a Raft service.
///
/// This function constructs a gRPC `Endpoint` configured with the specified address, concurrency
/// limit, and timeout settings. The `Endpoint` is used to establish a connection to the Raft
/// service.
///
/// # Parameters
/// - `saddr`: The server address in the form of a string (e.g., "127.0.0.1:50051").
/// - `concurrency_limit`: The maximum number of concurrent requests allowed.
/// - `timeout`: The connection timeout duration.
///
/// # Returns
/// Returns a `Result` containing the configured `Endpoint` on success, or an error if the endpoint
/// creation fails.
#[inline]
pub(crate) fn endpoint(
    saddr: &str,
    concurrency_limit: usize,
    timeout: Duration,
) -> Result<Endpoint> {
    let endpoint = Channel::from_shared(format!("http://{saddr}"))
        .map(|endpoint| {
            endpoint
                .concurrency_limit(concurrency_limit)
                .connect_timeout(timeout)
                .timeout(timeout)
        })
        .map_err(anyhow::Error::new)?;
    Ok(endpoint)
}

/// Establishes a connection to the Raft service and returns a client.
///
/// This asynchronous function creates a new `RaftServiceClient` instance, using the provided
/// address, concurrency limit, message size, and timeout settings. The client is configured with
/// the specified message size for both encoding and decoding.
///
/// # Parameters
/// - `saddr`: The server address in the form of a string (e.g., "127.0.0.1:50051").
/// - `concurrency_limit`: The maximum number of concurrent requests allowed.
/// - `message_size`: The maximum size of messages for encoding and decoding.
/// - `timeout`: The connection timeout duration.
///
/// # Returns
/// Returns a `Result` containing the `RaftServiceClient` instance on success, or an error if the
/// connection fails.
#[inline]
pub(crate) async fn connect(
    saddr: &str,
    concurrency_limit: usize,
    message_size: usize,
    timeout: Duration,
) -> Result<RaftServiceClientType> {
    Ok(RaftServiceClientType::new(
        endpoint(saddr, concurrency_limit, timeout)?
            .connect()
            .await?,
    )
    .max_decoding_message_size(message_size)
    .max_encoding_message_size(message_size))
}

/// Binds a TCP listener to the specified address and returns a `TcpListenerStream`.
///
/// This function sets up a TCP listener with options for socket reuse and a backlog queue. It
/// returns a `TcpListenerStream` that can be used to accept incoming connections. This is
/// particularly useful for scenarios requiring high-performance and customizable socket options.
///
/// # Parameters
/// - `laddr`: The local address to bind in the form of `std::net::SocketAddr`.
/// - `backlog`: The maximum number of pending connections in the backlog queue.
/// - `_reuseaddr`: Whether to enable the `SO_REUSEADDR` option on Unix-like systems.
/// - `_reuseport`: Whether to enable the `SO_REUSEPORT` option on Unix-like systems.
///
/// # Returns
/// Returns a `Result` containing the `TcpListenerStream` on success, or an error if the binding fails.
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
