use thiserror::Error as ThisError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("raft error: `{0}`")]
    RaftError(#[from] raft::Error),
    #[error("Error joining the cluster")]
    JoinError,
    #[error("gprc error: `{0}`")]
    Grpc(#[from] tonic::transport::Error),
    #[error("error calling remote procedure: `{0}`")]
    RemoteCall(#[from] tonic::Status),
    #[error("io error: {0}")]
    Io(String),
    #[error("unexpected error, {0}")]
    Other(#[source] Box<dyn std::error::Error + Sync + Send + 'static>),
    #[error("unexpected error")]
    Unknown,
    #[error("leader does not exist")]
    LeaderNotExist,
    #[error("Not a Leader")]
    NotLeader,
    #[error("protobuf error: `{0}`")]
    ProtobufError(#[from] protobuf::ProtobufError),
    #[error("timeout")]
    Elapsed,
    #[error("{0}")]
    Msg(String),
    #[error("send error, {0}")]
    SendError(String),
    #[error("recv error, {0}")]
    RecvError(String),
}

impl Error {
    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }
}

impl From<prost::DecodeError> for Error {
    fn from(e: prost::DecodeError) -> Self {
        Self::Other(Box::new(e))
    }
}

impl From<prost::EncodeError> for Error {
    fn from(e: prost::EncodeError) -> Self {
        Self::Other(Box::new(e))
    }
}

impl From<tokio::io::Error> for Error {
    fn from(e: tokio::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}

impl From<bincode::Error> for Error {
    fn from(e: bincode::Error) -> Self {
        Self::Other(e)
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(e: std::string::FromUtf8Error) -> Self {
        Self::Other(Box::new(e))
    }
}

impl From<String> for Error {
    fn from(e: String) -> Self {
        Self::Msg(e)
    }
}

impl From<&str> for Error {
    fn from(e: &str) -> Self {
        Self::Msg(e.to_owned())
    }
}