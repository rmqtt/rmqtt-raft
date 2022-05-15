# RmqttRaft - A raft framework, for regular people 

This is an attempt to create a layer on top of
[tikv/raft-rs](https://github.com/tikv/raft-rs), that is easier to use and
implement. This is not supposed to be the most featureful raft, but instead a
convenient interface to get started quickly, and have a working raft in no
time.

The interface is strongly inspired by the one used by [canonical/raft](https://github.com/canonical/raft).

## Getting started

In order to "raft" storage, we need to implement the `Storage` trait for it.
Bellow is an example with `HashStore`, which is a thread-safe wrapper around an
`HashMap`:

```rust
/// convienient data structure to pass Message in the raft
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

```

Only 4 methods need to be implemented for the Store: 
- `Store::apply`: applies a commited entry to the store.  
- `Store::query`  query a entry from the store;
- `Store::snapshot`: returns snapshot data for the store. 
- `Store::restore`: applies the snapshot passed as argument.

### running the raft

```rust
#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    
    let store = HashStore::new();

    let raft = Raft::new(options.raft_addr, store.clone(), logger.clone());
    let leader_info = raft.find_leader_info(options.peer_addrs).await?;

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

    tokio::try_join!(raft_handle)?;
    Ok(())
}

```

The `mailbox` gives you a way to interact with the raft, for sending a message, or leaving the cluster for example.


## Credit

This work is based on  [riteraft](https://github.com/ritelabs/riteraft), but more adjustments and improvements have been made to the code .

## License

This library is licensed under either of:

* MIT license [LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT
* Apache License 2.0 [LICENSE-APACHE](LICENSE-APACHE) or https://opensource.org/licenses/Apache-2.0

at your option.

