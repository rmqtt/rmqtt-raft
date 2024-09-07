use tikv_raft::prelude::*;
use tikv_raft::storage::MemStorage as CoreMemStorage;
use tikv_raft::GetEntriesContext;

use crate::error::Result;

/// A trait defining operations for a log store in a Raft implementation.
///
/// The `LogStore` trait extends the `Storage` trait with additional methods to manage Raft log entries,
/// hard state, configuration state, and snapshots. Implementations of this trait should support appending
/// log entries, updating the hard state and configuration state, creating and applying snapshots, and
/// compacting the log.
///
/// # Methods
/// - `append`: Append a list of log entries to the log store.
/// - `set_hard_state`: Set the hard state for the Raft state machine.
/// - `set_hard_state_comit`: Set the commit index in the hard state.
/// - `set_conf_state`: Set the configuration state for the Raft state machine.
/// - `create_snapshot`: Create a snapshot with the given data.
/// - `apply_snapshot`: Apply a snapshot to the log store.
/// - `compact`: Compact the log store up to the given index.
pub trait LogStore: Storage {
    /// Appends a list of log entries to the log store.
    fn append(&mut self, entries: &[Entry]) -> Result<()>;
    /// Sets the hard state for the Raft state machine.
    fn set_hard_state(&mut self, hard_state: &HardState) -> Result<()>;

    /// Sets the commit index in the hard state.
    fn set_hard_state_comit(&mut self, comit: u64) -> Result<()>;

    /// Sets the configuration state for the Raft state machine.
    fn set_conf_state(&mut self, conf_state: &ConfState) -> Result<()>;

    /// Creates a snapshot with the given data.
    fn create_snapshot(&mut self, data: prost::bytes::Bytes) -> Result<()>;

    /// Applies a snapshot to the log store.
    fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<()>;

    /// Compacts the log store up to the given index.
    fn compact(&mut self, index: u64) -> Result<()>;
}

/// An in-memory implementation of the `LogStore` trait using Tikv's `MemStorage`.
///
/// The `MemStorage` struct provides an in-memory storage backend for Raft logs and state. It uses Tikv's
/// `CoreMemStorage` as the underlying storage engine and includes additional methods for managing snapshots.
///
/// # Fields
/// - `core`: The underlying `CoreMemStorage` used for log and state management.
/// - `snapshot`: The currently held snapshot.
pub struct MemStorage {
    core: CoreMemStorage,
    snapshot: Snapshot,
}

impl MemStorage {
    /// Creates a new `MemStorage` instance with default settings.
    ///
    /// This function initializes `CoreMemStorage` and sets the `snapshot` to its default value.
    ///
    /// # Returns
    /// Returns a new `MemStorage` instance.
    #[inline]
    pub fn create() -> Self {
        let core = CoreMemStorage::default();
        let snapshot = Default::default();
        Self { core, snapshot }
    }
}

impl LogStore for MemStorage {
    /// Appends a list of log entries to the in-memory log store.
    ///
    /// This method acquires a write lock on the underlying `CoreMemStorage` and appends the provided
    /// entries.
    ///
    /// # Parameters
    /// - `entries`: The entries to be appended.
    ///
    /// # Returns
    /// Returns a `Result` indicating success or failure.
    #[inline]
    fn append(&mut self, entries: &[Entry]) -> Result<()> {
        let mut store = self.core.wl();
        store.append(entries)?;
        Ok(())
    }

    /// Sets the hard state for the Raft state machine.
    ///
    /// This method acquires a write lock on the underlying `CoreMemStorage` and updates the hard state.
    ///
    /// # Parameters
    /// - `hard_state`: The new hard state to set.
    ///
    /// # Returns
    /// Returns a `Result` indicating success or failure.
    #[inline]
    fn set_hard_state(&mut self, hard_state: &HardState) -> Result<()> {
        let mut store = self.core.wl();
        store.set_hardstate(hard_state.clone());
        Ok(())
    }

    /// Sets the commit index in the hard state.
    ///
    /// This method updates the commit index in the hard state by first acquiring a write lock on the
    /// underlying `CoreMemStorage`, modifying the commit index, and then setting the updated hard state.
    ///
    /// # Parameters
    /// - `comit`: The commit index to set.
    ///
    /// # Returns
    /// Returns a `Result` indicating success or failure.
    #[inline]
    fn set_hard_state_comit(&mut self, comit: u64) -> Result<()> {
        let mut store = self.core.wl();
        let mut hard_state = store.hard_state().clone();
        hard_state.set_commit(comit);
        store.set_hardstate(hard_state);
        Ok(())
    }

    /// Sets the configuration state for the Raft state machine.
    ///
    /// This method acquires a write lock on the underlying `CoreMemStorage` and updates the configuration state.
    ///
    /// # Parameters
    /// - `conf_state`: The new configuration state to set.
    ///
    /// # Returns
    /// Returns a `Result` indicating success or failure.
    #[inline]
    fn set_conf_state(&mut self, conf_state: &ConfState) -> Result<()> {
        let mut store = self.core.wl();
        store.set_conf_state(conf_state.clone());
        Ok(())
    }

    /// Creates a snapshot with the given data.
    ///
    /// This method initializes a new snapshot with the provided data and stores it in the `snapshot` field.
    ///
    /// # Parameters
    /// - `data`: The data to be included in the snapshot.
    ///
    /// # Returns
    /// Returns a `Result` indicating success or failure.
    #[inline]
    fn create_snapshot(&mut self, data: prost::bytes::Bytes) -> Result<()> {
        let mut snapshot = self.core.snapshot(0, 0)?;
        snapshot.set_data(data.to_vec());
        self.snapshot = snapshot;
        Ok(())
    }

    /// Applies a snapshot to the in-memory log store.
    ///
    /// This method acquires a write lock on the underlying `CoreMemStorage` and applies the provided snapshot.
    ///
    /// # Parameters
    /// - `snapshot`: The snapshot to apply.
    ///
    /// # Returns
    /// Returns a `Result` indicating success or failure.
    #[inline]
    fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
        let mut store = self.core.wl();
        store.apply_snapshot(snapshot)?;
        Ok(())
    }

    /// Compacts the log store up to the given index.
    ///
    /// This method acquires a write lock on the underlying `CoreMemStorage` and compacts the log up to the specified index.
    ///
    /// # Parameters
    /// - `index`: The index up to which to compact the log.
    ///
    /// # Returns
    /// Returns a `Result` indicating success or failure.
    #[inline]
    fn compact(&mut self, index: u64) -> Result<()> {
        let mut store = self.core.wl();
        store.compact(index)?;
        Ok(())
    }
}

impl Storage for MemStorage {
    /// Retrieves the initial state of the Raft state machine.
    ///
    /// This method returns the initial state from the underlying `CoreMemStorage`.
    ///
    /// # Returns
    /// Returns a `Result` containing the `RaftState` on success.
    #[inline]
    fn initial_state(&self) -> tikv_raft::Result<RaftState> {
        let raft_state = self.core.initial_state()?;
        Ok(raft_state)
    }

    /// Retrieves a range of log entries.
    ///
    /// This method acquires a read lock on the underlying `CoreMemStorage` and returns log entries
    /// in the specified range.
    ///
    /// # Parameters
    /// - `low`: The start index of the range (inclusive).
    /// - `high`: The end index of the range (exclusive).
    /// - `max_size`: The maximum size of the entries to return (optional).
    /// - `context`: Additional context for retrieving the entries.
    ///
    /// # Returns
    /// Returns a `Result` containing a vector of `Entry` objects on success.
    #[inline]
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> tikv_raft::Result<Vec<Entry>> {
        let entries = self.core.entries(low, high, max_size, context)?;
        Ok(entries)
    }

    /// Retrieves the term of the log entry at the specified index.
    ///
    /// This method returns the term of the log entry from the underlying `CoreMemStorage`.
    ///
    /// # Parameters
    /// - `idx`: The index of the log entry.
    ///
    /// # Returns
    /// Returns a `Result` containing the term of the entry on success.
    #[inline]
    fn term(&self, idx: u64) -> tikv_raft::Result<u64> {
        self.core.term(idx)
    }

    /// Retrieves the first index of the log.
    ///
    /// This method returns the first index from the underlying `CoreMemStorage`.
    ///
    /// # Returns
    /// Returns a `Result` containing the first index on success.
    #[inline]
    fn first_index(&self) -> tikv_raft::Result<u64> {
        self.core.first_index()
    }

    /// Retrieves the last index of the log.
    ///
    /// This method returns the last index from the underlying `CoreMemStorage`.
    ///
    /// # Returns
    /// Returns a `Result` containing the last index on success.
    #[inline]
    fn last_index(&self) -> tikv_raft::Result<u64> {
        self.core.last_index()
    }

    /// Retrieves the current snapshot.
    ///
    /// This method returns a clone of the current snapshot held in the `snapshot` field.
    ///
    /// # Parameters
    /// - `_request_index`: The index for the snapshot request (not used in this implementation).
    /// - `_to`: The index up to which the snapshot is requested (not used in this implementation).
    ///
    /// # Returns
    /// Returns a `Result` containing the current `Snapshot` on success.
    #[inline]
    fn snapshot(&self, _request_index: u64, _to: u64) -> tikv_raft::Result<Snapshot> {
        Ok(self.snapshot.clone())
    }
}
