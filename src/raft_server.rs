use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use bincode::serialize;
use log::{error, info, warn};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::message::{Message, RaftResponse};
use crate::raft_service::raft_service_server::{RaftService, RaftServiceServer};
use crate::raft_service::{
    self, ConfChange as RiteraftConfChange, Empty, Message as RiteraftMessage,
};

pub struct RaftServer {
    snd: mpsc::Sender<Message>,
    addr: SocketAddr,
}

impl RaftServer {
    pub fn new<A: ToSocketAddrs>(snd: mpsc::Sender<Message>, addr: A) -> Self {
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        RaftServer { snd, addr }
    }

    pub async fn run(self) {
        let addr = self.addr;
        info!("listening gRPC requests on: {}", addr);
        let svc = RaftServiceServer::new(self);
        Server::builder()
            .add_service(svc)
            .serve(addr)
            .await
            .expect("error running server");
        warn!("server has quit");
    }
}

#[tonic::async_trait]
impl RaftService for RaftServer {
    async fn request_id(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<raft_service::IdRequestReponse>, Status> {
        let sender = self.snd.clone();
        let (tx, rx) = oneshot::channel();
        let _ = sender.send(Message::RequestId { chan: tx }).await;
        let response = rx.await.unwrap();
        match response {
            RaftResponse::WrongLeader {
                leader_id,
                leader_addr,
            } => {
                warn!("sending wrong leader");
                Ok(Response::new(raft_service::IdRequestReponse {
                    code: raft_service::ResultCode::WrongLeader as i32,
                    data: serialize(&(leader_id, leader_addr)).unwrap(),
                }))
            }
            RaftResponse::RequestId { leader_id } => {
                Ok(Response::new(raft_service::IdRequestReponse {
                    code: raft_service::ResultCode::Ok as i32,
                    data: serialize(&leader_id).unwrap(),
                }))
            }
            _ => unreachable!(),
        }
    }

    async fn change_config(
        &self,
        req: Request<RiteraftConfChange>,
    ) -> Result<Response<raft_service::RaftResponse>, Status> {
        //let change = req.into_inner();
        let change = protobuf::Message::parse_from_bytes(req.into_inner().inner.as_ref())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let sender = self.snd.clone();

        let (tx, rx) = oneshot::channel();

        let message = Message::ConfigChange { change, chan: tx };

        match sender.send(message).await {
            Ok(_) => (),
            Err(_) => error!("send error"),
        }

        let mut reply = raft_service::RaftResponse::default();

        // if we don't receive a response after 2secs, we timeout
        match timeout(Duration::from_secs(5), rx).await {
            Ok(Ok(raft_response)) => {
                reply.inner = serialize(&raft_response).expect("serialize error");
            }
            Ok(_) => (),
            Err(e) => {
                reply.inner = serialize(&RaftResponse::Error).unwrap();
                error!("timeout waiting for reply, {:?}", e);
            }
        }

        Ok(Response::new(reply))
    }

    async fn send_message(
        &self,
        request: Request<RiteraftMessage>,
    ) -> Result<Response<raft_service::RaftResponse>, Status> {
        // let message = request.into_inner();
        let message = protobuf::Message::parse_from_bytes(request.into_inner().inner.as_ref())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // again this ugly shit to serialize the message
        let sender = self.snd.clone();
        match sender.send(Message::Raft(Box::new(message))).await {
            Ok(_) => (),
            Err(e) => error!("send message error, {:?}", e.to_string()),
        }

        let response = RaftResponse::Ok;
        Ok(Response::new(raft_service::RaftResponse {
            inner: serialize(&response).unwrap(),
        }))
    }

    async fn forward(
        &self,
        req: Request<raft_service::Proposal>,
    ) -> Result<Response<raft_service::RaftResponse>, Status> {
        let proposal = req.into_inner().inner;
        let sender = self.snd.clone();
        let (tx, rx) = oneshot::channel();
        let message = Message::Propose { proposal, chan: tx };
        match sender.send(message).await {
            Ok(_) => (),
            Err(_) => error!("send forward error"),
        }

        let mut reply = raft_service::RaftResponse::default();

        // if we don't receive a response after 2secs, we timeout
        match timeout(Duration::from_secs(5), rx).await {
            Ok(Ok(raft_response)) => {
                reply.inner = serialize(&raft_response).expect("serialize error");
            }
            Ok(_) => (),
            Err(_e) => {
                reply.inner = serialize(&RaftResponse::Error).unwrap();
                error!("timeout waiting for reply");
            }
        }

        Ok(Response::new(reply))
    }

    async fn send_query(
        &self,
        req: Request<raft_service::Query>,
    ) -> Result<Response<raft_service::RaftResponse>, Status> {
        let query = req.into_inner().inner;
        let sender = self.snd.clone();
        let (tx, rx) = oneshot::channel();
        let message = Message::Query { query, chan: tx };
        match sender.send(message).await {
            Ok(_) => (),
            Err(_) => error!("send query error"),
        }
        let mut reply = raft_service::RaftResponse::default();
        // if we don't receive a response after 2secs, we timeout
        match timeout(Duration::from_secs(5), rx).await {
            Ok(Ok(raft_response)) => {
                reply.inner = serialize(&raft_response).expect("serialize error");
            }
            Ok(_) => (),
            Err(_e) => {
                reply.inner = serialize(&RaftResponse::Error).unwrap();
                error!("timeout waiting for reply");
            }
        }

        Ok(Response::new(reply))
    }
}
