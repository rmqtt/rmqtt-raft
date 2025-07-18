use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bincode::serialize;
use futures::channel::{mpsc, oneshot};
use futures::SinkExt;
use log::{info, warn};
use prost::Message as _;
use tikv_raft::eraftpb::{ConfChange, Message as RaftMessage};
use tokio::time::timeout;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::message::{Message, RaftResponse};
use crate::raft_service::raft_service_server::{RaftService, RaftServiceServer};
use crate::raft_service::{
    self, ConfChange as RiteraftConfChange, Empty, Message as RiteraftMessage,
};
use crate::{error, Config};

/// A gRPC server that handles Raft-related requests.
pub struct RaftServer {
    snd: mpsc::Sender<Message>,
    laddr: SocketAddr,
    timeout: Duration,
    cfg: Arc<Config>,
}

impl RaftServer {
    /// Creates a new instance of `RaftServer`.
    ///
    /// This function initializes a new `RaftServer` with the specified parameters.
    ///
    /// # Parameters
    /// - `snd`: A sender for Raft messages.
    /// - `laddr`: The local address where the server will listen for incoming requests.
    /// - `cfg`: Configuration for the server, including gRPC timeouts and other settings.
    ///
    /// # Returns
    /// Returns a new `RaftServer` instance.
    pub fn new(snd: mpsc::Sender<Message>, laddr: SocketAddr, cfg: Arc<Config>) -> Self {
        RaftServer {
            snd,
            laddr,
            timeout: cfg.grpc_timeout,
            cfg,
        }
    }

    /// Starts the gRPC server to handle Raft requests.
    ///
    /// This function sets up the gRPC server and listens for incoming requests. It uses
    /// the `RaftServiceServer` to handle requests and manage configuration options.
    ///
    /// # Returns
    /// Returns a `Result` indicating whether the server started successfully or if an error occurred.
    pub async fn run(self) -> error::Result<()> {
        let laddr = self.laddr;
        let _cfg = self.cfg.clone();
        info!("listening gRPC requests on: {}", laddr);
        let svc = RaftServiceServer::new(self)
            .max_decoding_message_size(_cfg.grpc_message_size)
            .max_encoding_message_size(_cfg.grpc_message_size);
        let server = Server::builder().add_service(svc);

        #[cfg(any(feature = "reuseport", feature = "reuseaddr"))]
        #[cfg(all(feature = "socket2", feature = "tokio-stream"))]
        {
            log::info!(
                "reuseaddr: {}, reuseport: {}",
                _cfg.reuseaddr,
                _cfg.reuseport
            );
            let listener = raft_service::bind(laddr, 1024, _cfg.reuseaddr, _cfg.reuseport)?;
            server.serve_with_incoming(listener).await?;
        }
        #[cfg(not(any(feature = "reuseport", feature = "reuseaddr")))]
        server.serve(laddr).await?;

        info!("server has quit");
        Ok(())
    }
}

#[tonic::async_trait]
impl RaftService for RaftServer {
    /// Handles requests for a new Raft node ID.
    ///
    /// This method sends a `RequestId` message to the Raft node and waits for a response.
    /// It returns the node ID if successful or an error status if not.
    ///
    /// # Parameters
    /// - `req`: The incoming request containing no additional data.
    ///
    /// # Returns
    /// Returns a `Response` containing the node ID or an error status.
    async fn request_id(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<raft_service::IdRequestReponse>, Status> {
        let mut sender = self.snd.clone();
        let (tx, rx) = oneshot::channel();
        let _ = sender.send(Message::RequestId { chan: tx }).await;
        //let response = rx.await;
        let reply = timeout(self.timeout, rx)
            .await
            .map_err(|_e| Status::unavailable("recv timeout for reply"))?
            .map_err(|_e| Status::unavailable("recv canceled for reply"))?;
        match reply {
            RaftResponse::WrongLeader {
                leader_id,
                leader_addr,
            } => {
                warn!("sending wrong leader, leader_id: {leader_id}, leader_addr: {leader_addr:?}");
                Ok(Response::new(raft_service::IdRequestReponse {
                    code: raft_service::ResultCode::WrongLeader as i32,
                    data: serialize(&(leader_id, leader_addr))
                        .map_err(|e| Status::unavailable(e.to_string()))?,
                }))
            }
            RaftResponse::RequestId { leader_id } => {
                Ok(Response::new(raft_service::IdRequestReponse {
                    code: raft_service::ResultCode::Ok as i32,
                    data: serialize(&leader_id).map_err(|e| Status::unavailable(e.to_string()))?,
                }))
            }
            _ => unreachable!(),
        }
    }

    /// Handles configuration change requests.
    ///
    /// This method processes a configuration change request by sending it to the Raft node
    /// and waits for a response. It returns the result of the configuration change operation.
    ///
    /// # Parameters
    /// - `req`: The incoming request containing the configuration change data.
    ///
    /// # Returns
    /// Returns a `Response` containing the result of the configuration change or an error status.
    async fn change_config(
        &self,
        req: Request<RiteraftConfChange>,
    ) -> Result<Response<raft_service::RaftResponse>, Status> {
        let change = ConfChange::decode(req.into_inner().inner.as_ref())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let mut sender = self.snd.clone();

        let (tx, rx) = oneshot::channel();

        let message = Message::ConfigChange { change, chan: tx };

        match sender.send(message).await {
            Ok(_) => (),
            Err(_) => warn!("send error"),
        }

        let mut reply = raft_service::RaftResponse::default();

        match timeout(self.timeout, rx).await {
            Ok(Ok(raft_response)) => {
                reply.inner =
                    serialize(&raft_response).map_err(|e| Status::unavailable(e.to_string()))?;
            }
            Ok(_) => (),
            Err(e) => {
                reply.inner = serialize(&RaftResponse::Error("timeout".into()))
                    .map_err(|e| Status::unavailable(e.to_string()))?;
                warn!("timeout waiting for reply, {:?}", e);
            }
        }

        Ok(Response::new(reply))
    }

    /// Handles sending Raft messages.
    ///
    /// This method processes a Raft message by sending it to the Raft node and returns
    /// the result of the send operation.
    ///
    /// # Parameters
    /// - `request`: The incoming request containing the Raft message data.
    ///
    /// # Returns
    /// Returns a `Response` indicating success or an error status.
    async fn send_message(
        &self,
        request: Request<RiteraftMessage>,
    ) -> Result<Response<raft_service::RaftResponse>, Status> {
        let message = RaftMessage::decode(request.into_inner().inner.as_ref())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        match self.snd.clone().try_send(Message::Raft(Box::new(message))) {
            Ok(()) => {
                let response = RaftResponse::Ok;
                Ok(Response::new(raft_service::RaftResponse {
                    inner: serialize(&response).map_err(|e| Status::unavailable(e.to_string()))?,
                }))
            }
            Err(_) => Err(Status::unavailable("error for try send message")),
        }
    }

    /// Handles sending proposals.
    ///
    /// This method sends a proposal to the Raft node and waits for a response. It returns
    /// the result of the proposal send operation.
    ///
    /// # Parameters
    /// - `req`: The incoming request containing the proposal data.
    ///
    /// # Returns
    /// Returns a `Response` containing the result of the proposal send operation or an error status.
    async fn send_proposal(
        &self,
        req: Request<raft_service::Proposal>,
    ) -> Result<Response<raft_service::RaftResponse>, Status> {
        let proposal = req.into_inner().inner;
        let mut sender = self.snd.clone();
        let (tx, rx) = oneshot::channel();
        let message = Message::Propose { proposal, chan: tx };

        match sender.try_send(message) {
            Ok(()) => match timeout(self.timeout, rx).await {
                Ok(Ok(raft_response)) => match serialize(&raft_response) {
                    Ok(resp) => Ok(Response::new(raft_service::RaftResponse { inner: resp })),
                    Err(e) => {
                        warn!("serialize error, {}", e);
                        Err(Status::unavailable("serialize error"))
                    }
                },
                Ok(Err(e)) => {
                    warn!("recv error for reply, {}", e);
                    Err(Status::unavailable("recv error for reply"))
                }
                Err(e) => {
                    warn!("timeout waiting for reply, {}", e);
                    Err(Status::unavailable("timeout waiting for reply"))
                }
            },
            Err(e) => {
                warn!("error for try send message, {}", e);
                Err(Status::unavailable("error for try send message"))
            }
        }
    }

    /// Handles sending queries.
    ///
    /// This method sends a query to the Raft node and waits for a response. It returns
    /// the result of the query send operation.
    ///
    /// # Parameters
    /// - `req`: The incoming request containing the query data.
    ///
    /// # Returns
    /// Returns a `Response` containing the result of the query send operation or an error status.
    async fn send_query(
        &self,
        req: Request<raft_service::Query>,
    ) -> Result<Response<raft_service::RaftResponse>, Status> {
        let query = req.into_inner().inner;
        let mut sender = self.snd.clone();
        let (tx, rx) = oneshot::channel();
        let message = Message::Query { query, chan: tx };
        let mut reply = raft_service::RaftResponse::default();
        match sender.try_send(message) {
            Ok(()) => {
                // if we don't receive a response after 2secs, we timeout
                match timeout(self.timeout, rx).await {
                    Ok(Ok(raft_response)) => {
                        reply.inner = serialize(&raft_response)
                            .map_err(|e| Status::unavailable(e.to_string()))?;
                    }
                    Ok(Err(e)) => {
                        reply.inner = serialize(&RaftResponse::Error(e.to_string()))
                            .map_err(|e| Status::unavailable(e.to_string()))?;
                        warn!("send query error, {}", e);
                    }
                    Err(_e) => {
                        reply.inner = serialize(&RaftResponse::Error("timeout".into()))
                            .map_err(|e| Status::unavailable(e.to_string()))?;
                        warn!("timeout waiting for send query reply");
                    }
                }
            }
            Err(e) => {
                reply.inner = serialize(&RaftResponse::Error(e.to_string()))
                    .map_err(|e| Status::unavailable(e.to_string()))?;
                warn!("send query error, {}", e)
            }
        }

        Ok(Response::new(reply))
    }
}
