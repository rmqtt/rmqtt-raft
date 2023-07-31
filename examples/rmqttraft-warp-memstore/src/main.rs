#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use async_trait::async_trait;
use bincode::{deserialize, serialize};
use rmqtt_raft::{Mailbox, Raft, Result as RaftResult, Store, Config};
use serde::{Deserialize, Serialize};
use slog::Drain;
use slog::info;
use std::collections::HashMap;
use std::convert::From;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use structopt::StructOpt;
use warp::{Filter, reply};

#[derive(Debug, StructOpt)]
struct Options {
    #[structopt(long)]
    id: u64,
    #[structopt(long)]
    raft_laddr: String,
    #[structopt(name = "peer-addr", long)]
    peer_addrs: Vec<String>,
    #[structopt(long)]
    web_server: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub enum Message {
    Insert { key: String, value: String },
    Get { key: String },
}

#[derive(Clone)]
struct HashStore(Arc<RwLock<HashMap<String, String>>>);

impl HashStore {
    fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }
    fn get(&self, key: &str) -> Option<String> {
        self.0.read().unwrap().get(key).cloned()
    }
}

#[async_trait]
impl Store for HashStore {
    async fn apply(&mut self, message: &[u8]) -> RaftResult<Vec<u8>> {
        let message: Message = deserialize(message).unwrap();
        let message: Vec<u8> = match message {
            Message::Insert { key, value } => {
                let mut db = self.0.write().unwrap();
                let v = serialize(&value).unwrap();
                db.insert(key, value);
                v
            }
            _ => Vec::new(),
        };
        Ok(message)
    }

    async fn query(&self, query: &[u8]) -> RaftResult<Vec<u8>> {
        let query: Message = deserialize(query).unwrap();
        let data: Vec<u8> = match query {
            Message::Get { key } => {
                if let Some(val) = self.get(&key) {
                    serialize(&val).unwrap()
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        };
        Ok(data)
    }

    async fn snapshot(&self) -> RaftResult<Vec<u8>> {
        Ok(serialize(&self.0.read().unwrap().clone())?)
    }

    async fn restore(&mut self, snapshot: &[u8]) -> RaftResult<()> {
        let new: HashMap<String, String> = deserialize(snapshot).unwrap();
        let mut db = self.0.write().unwrap();
        let _ = std::mem::replace(&mut *db, new);
        Ok(())
    }
}

fn with_mailbox(
    mailbox: Arc<Mailbox>,
) -> impl Filter<Extract=(Arc<Mailbox>, ), Error=Infallible> + Clone {
    warp::any().map(move || mailbox.clone())
}

fn with_store(store: HashStore) -> impl Filter<Extract=(HashStore, ), Error=Infallible> + Clone {
    warp::any().map(move || store.clone())
}

async fn put(
    mailbox: Arc<Mailbox>,
    key: String,
    value: String,
) -> Result<impl warp::Reply, Infallible> {
    let message = Message::Insert { key, value };
    let message = serialize(&message).unwrap();
    let result = mailbox.send(message).await;
    match result {
        Ok(r) => {
            let result: String = deserialize(&r).unwrap();
            Ok(reply::json(&result))
        }
        Err(e) => Ok(reply::json(&format!("put error, {:?}", e))),
    }
}

async fn get(store: HashStore, key: String) -> Result<impl warp::Reply, Infallible> {
    let response = store.get(&key);
    Ok(reply::json(&response))
}

async fn leave(mailbox: Arc<Mailbox>) -> Result<impl warp::Reply, Infallible> {
    mailbox.leave().await.unwrap();
    Ok(reply::json(&"OK".to_string()))
}

async fn status(mailbox: Arc<Mailbox>) -> Result<impl warp::Reply, Infallible> {
    let response = mailbox.status().await.unwrap();
    Ok(reply::json(&response))
}

//target\release\rmqttraft-warp-memstore.exe --id 1 --raft-laddr "127.0.0.1:5001" --peer-addr "127.0.0.1:5002" --peer-addr "127.0.0.1:5003" --web-server "0.0.0.0:8081"
//target\release\rmqttraft-warp-memstore.exe --id 2 --raft-laddr "127.0.0.1:5002" --peer-addr "127.0.0.1:5001" --peer-addr "127.0.0.1:5003" --web-server "0.0.0.0:8082"
//target\release\rmqttraft-warp-memstore.exe --id 3 --raft-laddr "127.0.0.1:5003" --peer-addr "127.0.0.1:5001" --peer-addr "127.0.0.1:5002" --web-server "0.0.0.0:8083"
//target\release\rmqttraft-warp-memstore.exe --id 4 --raft-laddr "127.0.0.1:5004" --peer-addr "127.0.0.1:5001" --peer-addr "127.0.0.1:5002" --web-server "0.0.0.0:8084"
//target\release\rmqttraft-warp-memstore.exe --id 5 --raft-laddr "127.0.0.1:5005" --peer-addr "127.0.0.1:5001" --peer-addr "127.0.0.1:5002" --web-server "0.0.0.0:8085"

//./target/release/rmqttraft-warp-memstore --id 1 --raft-laddr "127.0.0.1:5001" --peer-addr "127.0.0.1:5002" --peer-addr "127.0.0.1:5003" --web-server "0.0.0.0:8081"
//./target/release/rmqttraft-warp-memstore --id 2 --raft-laddr "127.0.0.1:5002" --peer-addr "127.0.0.1:5001" --peer-addr "127.0.0.1:5003" --web-server "0.0.0.0:8082"
//./target/release/rmqttraft-warp-memstore --id 3 --raft-laddr "127.0.0.1:5003" --peer-addr "127.0.0.1:5001" --peer-addr "127.0.0.1:5002" --web-server "0.0.0.0:8083"

//target\debug\rmqttraft-warp-memstore.exe --id 1 --raft-laddr "127.0.0.1:5001" --peer-addr "127.0.0.1:5002" --peer-addr "127.0.0.1:5003" --web-server "0.0.0.0:8081"
//target\debug\rmqttraft-warp-memstore.exe --id 2 --raft-laddr "127.0.0.1:5002" --peer-addr "127.0.0.1:5001" --peer-addr "127.0.0.1:5003" --web-server "0.0.0.0:8082"
//target\debug\rmqttraft-warp-memstore.exe --id 3 --raft-laddr "127.0.0.1:5003" --peer-addr "127.0.0.1:5001" --peer-addr "127.0.0.1:5002" --web-server "0.0.0.0:8083"

//./target/debug/rmqttraft-warp-memstore --id 1 --raft-laddr "127.0.0.1:5001" --peer-addr "127.0.0.1:5002" --peer-addr "127.0.0.1:5003" --web-server "0.0.0.0:8081" > out_1.log 2>&1 &
//./target/debug/rmqttraft-warp-memstore --id 2 --raft-laddr "127.0.0.1:5002" --peer-addr "127.0.0.1:5001" --peer-addr "127.0.0.1:5003" --web-server "0.0.0.0:8082" > out_2.log 2>&1 &
//./target/debug/rmqttraft-warp-memstore --id 3 --raft-laddr "127.0.0.1:5003" --peer-addr "127.0.0.1:5001" --peer-addr "127.0.0.1:5002" --web-server "0.0.0.0:8083" > out_3.log 2>&1 &


// wrk -c 100 -t4 -d60s -H "Connection: keep-alive" "http://127.0.0.1:8081/put/key1/val-1"
// wrk -c 100 -t4 -d60s -H "Connection: keep-alive" "http://127.0.0.1:8082/put/key1/val-2"
// wrk -c 100 -t6 -d60s -H "Connection: keep-alive" "http://127.0.0.1:8083/get/key1"

// ab -n 5000 -c 20 "http://127.0.0.1:8081/put/key1/val-1"
// ab -n 5000 -c 50 "http://127.0.0.1:8082/put/key1/val-2"
// ab -n 5000 -c 20 "http://127.0.0.1:8083/get/key1"

// ab -n 50000 -c 1000 "http://127.0.0.1:8081/put/key1/val-1"
// ab -n 50000 -c 1000 "http://127.0.0.1:8082/put/key2/val-1"
// ab -n 50000 -c 1000 "http://127.0.0.1:8083/put/key3/val-1"

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, slog_o!("version" => env!("CARGO_PKG_VERSION")));

    // converts log to slog
    let _log_guard = slog_stdlog::init().unwrap();

    let options = Options::from_args();
    let store = HashStore::new();
    info!(logger, "peer_addrs: {:?}", options.peer_addrs);
    let cfg = Config {
        reuseaddr: true,
        reuseport: true,
        ..Default::default()
    };
    let raft = Raft::new(options.raft_laddr, store.clone(), logger.clone(), cfg)?;
    let leader_info = raft.find_leader_info(options.peer_addrs).await?;
    info!(logger, "leader_info: {:?}", leader_info);

    let mailbox = Arc::new(raft.mailbox());
    let (raft_handle, mailbox) = match leader_info {
        Some((leader_id, leader_addr)) => {
            info!(logger, "running in follower mode");
            let handle = tokio::spawn(raft.join(options.id, Some(leader_id), leader_addr));
            (handle, mailbox)
        }
        None => {
            info!(logger, "running in leader mode");
            let handle = tokio::spawn(raft.lead(options.id));
            (handle, mailbox)
        }
    };

    let put_kv = warp::get()
        .and(warp::path!("put" / String / String))
        .and(with_mailbox(mailbox.clone()))
        .and_then(|key, value, mailbox: Arc<Mailbox>| put(mailbox, key, value));

    let get_kv = warp::get()
        .and(warp::path!("get" / String))
        .and(with_store(store.clone()))
        .and_then(|key, store: HashStore| get(store, key));

    let leave_kv = warp::get()
        .and(warp::path!("leave"))
        .and(with_mailbox(mailbox.clone()))
        .and_then(leave);

    let status = warp::get()
        .and(warp::path!("status"))
        .and(with_mailbox(mailbox.clone()))
        .and_then(status);

    let routes = put_kv.or(get_kv).or(leave_kv).or(status);

    if let Some(addr) = options.web_server {
        let _server = tokio::spawn(async move {
            warp::serve(routes)
                .run(SocketAddr::from_str(&addr).unwrap())
                .await;
        });
    }

    tokio::try_join!(raft_handle)?.0?;
    Ok(())
}
