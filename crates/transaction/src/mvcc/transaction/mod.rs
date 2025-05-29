// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use core::mem;
use std::sync::Arc;

pub use crate::mvcc::version::*;
pub use write::*;

use oracle::*;
/// If your `K` does not implement [`Hash`](core::hash::Hash), you can use [`SerializableDb`] instead.
pub mod optimistic;
mod oracle;
pub mod read;
pub mod scan;
pub mod serializable;

mod write;
use crate::mvcc::conflict::Conflict;
use crate::mvcc::error::TransactionError;
use crate::mvcc::pending::PendingWrites;
use crate::mvcc::transaction::read::TransactionManagerRx;

/// A multi-writer multi-reader MVCC, ACID, Serializable Snapshot Isolation transaction manager.
pub struct TransactionManager<K, V, C, P> {
    inner: Arc<Oracle<C>>,
    _phantom: std::marker::PhantomData<(K, V, P)>,
}

impl<K, V, C, P> Clone for TransactionManager<K, V, C, P> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone(), _phantom: std::marker::PhantomData }
    }
}

impl<K, V, C, P> TransactionManager<K, V, C, P>
where
    C: Conflict<Key = K>,
    P: PendingWrites<Key = K, Value = V>,
{
    /// Create a new writable transaction with
    /// the default pending writes manager to store the pending writes.
    pub fn write(
        &self,
        pending_manager_opts: P::Options,
    ) -> Result<TransactionManagerTx<K, V, C, P>, TransactionError> {
        let read_ts = self.inner.read_ts();
        Ok(TransactionManagerTx {
            oracle: self.inner.clone(),
            version: read_ts,
            size: 0,
            count: 0,
            conflicts: C::new(),
            pending_writes: Some(P::new(pending_manager_opts)),
            duplicate_writes: Vec::new(),
            discarded: false,
            done_read: false,
        })
    }
}

impl<K, V, C, P> TransactionManager<K, V, C, P> {
    /// Create a new transaction manager with the given name (just for logging or debugging, use your crate name is enough)
    /// and the current version (provided by the database).
    pub fn new(name: &str, current_version: u64) -> Self {
        Self {
            inner: Arc::new({
                let next_ts = current_version;
                let orc = Oracle::new(
                    format!("{}.pending_reads", name).into(),
                    format!("{}.txn_timestamps", name).into(),
                    next_ts,
                );
                orc.rx.done(next_ts);
                orc.tx.done(next_ts);
                orc.increment_next_ts();
                orc
            }),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Returns the current read version of the transaction manager.

    pub fn version(&self) -> u64 {
        self.inner.read_ts()
    }
}

impl<K, V, C, P> TransactionManager<K, V, C, P> {
    /// Returns a timestamp which hints that any versions under this timestamp can be discard.
    /// This is useful when users want to implement compaction/merge functionality.
    pub fn discard_hint(&self) -> u64 {
        self.inner.discard_at_or_below()
    }

    /// Create a new writable transaction.
    pub fn read(&self) -> TransactionManagerRx<K, V, C, P> {
        TransactionManagerRx { db: self.clone(), version: self.inner.read_ts() }
    }
}
