# Dynamic peers on rmqtt-raft 0.7.2

## Baselines and provenance

| Reference | Commit | Purpose |
| --- | --- | --- |
| Official `0.7.2` / `base/rmqtt-raft-0.7.2` | `3a524ae9908f30a80feab831237d99a9a47f9654` | Unmodified upstream baseline |
| Existing `0.6.0-dynamic-peers` | `5bc11e0cf7f6d617c20599d445614abda38087d7` | Original fork, retained for RMQTT 19 |
| Membership and replication changes | `6a3c3a71bb762516376885a09b5f4b743caf127a` | Source of the membership, status and join/snapshot changes |
| Runtime shutdown | `5bc11e0cf7f6d617c20599d445614abda38087d7` | Source of explicit Raft and gRPC shutdown |

The new implementation lives on `0.7.2-dynamic-peers`. Port commits record
their original SHA in `Ported-from` trailers. The crate retains version `0.7.2`
and uses upstream's vendored `rmqtt-raft-core 0.8.1`, tonic/prost 0.13 and
`postcard`; it does not downgrade these dependencies to the 0.6.0 stack.

The upstream tag contains tonic 0.14 build/server API calls while declaring
tonic 0.13 dependencies. A separate fork commit restores `tonic_build` and
`Server::builder().add_service(...)` so both default and `reuse` builds work.
The baseline branch remains exactly at the upstream tag, including this
original build inconsistency.

## Ported behavior

- `Raft::join_as_learner()` joins without immediately becoming a voter.
- `Mailbox::promote_learner()`, `remove_node()` and `transfer_leader()` expose
  coordinated membership operations.
- `Status` reports voters, learners, applied/committed/last indexes, snapshot
  restoration and per-peer replication progress. Its catch-up helpers retain
  their existing semantics.
- Availability counts voting members rather than learners and uses a strict
  voter majority for the current simple configuration changes.
- Initial join connections have a bounded retry budget, exponential backoff
  and per-node jitter (`Config::join_connect_retry_timeout`, default 20 seconds).
- Snapshot transport success/failure is reported to the Raft progress tracker.
  New-member snapshots are generated after applying the configuration entry.
  Joining followers retain the out-of-range heartbeat protection.
- `Mailbox::shutdown()` explicitly stops the local Raft event loop and signals
  the gRPC listener to stop. Dropping all mailbox handles is not a shutdown
  request.

## API and compatibility boundaries

The caller must check that a learner has caught up before promoting it.
`promote_learner()` submits the configuration change; it does not independently
enforce that precondition. A successful `transfer_leader()` means the request
was accepted; poll status to observe the new leader.

Follower availability checks leader connectivity, so availability does not
replace `local_caught_up()`. The voter-count helper retains the simple
configuration semantics of the original fork; it is not a joint-consensus
dual-majority verifier.

The retry budget covers the initial connection when joining a known leader.
It does not retry every possible discovery, membership or state-machine error.
Shutdown's timeout covers its acknowledgement; a blocked enqueue can still wait
for queue capacity, as in the original fork.

All added membership requests and responses use the 0.7.2 `postcard` codec.
There is no automatic conversion of 0.6.0 `bincode` log or snapshot wrappers,
and mixed-version operation is not an upgrade guarantee. Application-level
`Store` snapshot bytes remain the application's responsibility. The RMQTT 24
integration must also use matching core/protobuf types and a validated,
pinned fork revision.

## Regression tests

From the repository root, with Rust, Cargo and `protoc` available. This library
ignores `Cargo.lock`; run `cargo generate-lockfile` first if the checkout has no
local lockfile, then use that same lockfile for both feature configurations:

```sh
cargo test --locked
cargo test --locked --features reuse
```

`tests/dynamic_membership.rs` starts in-memory nodes on loopback addresses.
It exercises snapshot restoration, learner catch-up and promotion, three-voter
replication, follower-originated proposals and queries, leader transfer,
non-leader rejection, member removal, delayed initial connection, explicit
shutdown and listener release. It also round-trips live status through
`postcard`. The unit tests retain the original quorum, heartbeat, retry and
shutdown checks.

The tests use bounded deadlines and clean up their own runtimes and listeners.
They do not connect to a business cluster. These are functional regressions,
not throughput or latency benchmarks, and do not establish the full RMQTT 24
Broker/Operator integration.

Validated locally with Rust/Cargo 1.96.1 and protoc 33.4: both commands pass
13 unit tests and 2 loopback integration tests. The existing timeout-recorder
documentation example remains ignored. A failed snapshot transport regression
also checks the failure reports and the release of the sending counter.
