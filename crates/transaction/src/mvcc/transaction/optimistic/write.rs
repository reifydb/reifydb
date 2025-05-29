// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use crate::mvcc::error::{MvccError, TransactionError};
use crate::mvcc::pending::{BTreePendingWrites, PendingWritesComparableRange};
use crate::mvcc::skipdbcore::types::Ref;
use crate::mvcc::transaction::TransactionManagerTx;
use crate::mvcc::transaction::scan::iter::TransactionIter;
use crate::mvcc::transaction::scan::range::TransactionRange;
use crate::mvcc::transaction::scan::rev_iter::WriteTransactionRevIter;
use crate::mvcc::transaction::scan::rev_range::WriteTransactionRevRange;
use std::borrow::Borrow;
use std::fmt::Debug;
use std::ops::RangeBounds;

/// A optimistic concurrency control transaction over the [`Optimistic`].
pub struct TransactionTx<K, V> {
    engine: Optimistic<K, V>,
    pub(in crate::mvcc) tx: TransactionManagerTx<K, V, BTreeConflict<K>, BTreePendingWrites<K, V>>,
}

impl<K, V> TransactionTx<K, V>
where
    K: Clone + Ord + Hash + Eq,
{
    pub fn new(db: Optimistic<K, V>) -> Self {
        let tx = db.inner.tm.write().unwrap();
        Self { engine: db, tx }
    }
}

impl<K, V> TransactionTx<K, V>
where
    K: Clone + Ord + Hash + Eq + Debug,
    V: Send + 'static + Debug,
{
    /// Commits the transaction, following these steps:
    ///
    /// 1. If there are no writes, return immediately.
    ///
    /// 2. Check if read rows were updated since txn started. If so, return `TransactionError::Conflict`.
    ///
    /// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
    ///
    /// 4. Batch up all writes, write them to database.
    ///
    /// 5. If callback is provided, Badger will return immediately after checking
    ///    for conflicts. Writes to the database will happen in the background.  If
    ///    there is a conflict, an error will be returned and the callback will not
    ///    run. If there are no conflicts, the callback will be called in the
    ///    background upon successful completion of writes or any error during write.

    pub fn commit(&mut self) -> Result<(), MvccError> {
        self.tx.commit(|operations| {
            self.engine.inner.mem_table.apply(operations);
            Ok(())
        })
    }
}

impl<K, V> TransactionTx<K, V>
where
    K: Clone + Ord + Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Acts like [`commit`](WriteTransaction::commit), but takes a callback, which gets run via a
    /// thread to avoid blocking this function. Following these steps:
    ///
    /// 1. If there are no writes, return immediately, callback will be invoked.
    ///
    /// 2. Check if read rows were updated since txn started. If so, return `TransactionError::Conflict`.
    ///
    /// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
    ///
    /// 4. Batch up all writes, write them to database.
    ///
    /// 5. Return immediately after checking for conflicts.
    ///    If there is a conflict, an error will be returned immediately and the callback will not
    ///    run. If there are no conflicts, the callback will be called in the
    ///    background upon successful completion of writes or any error during write.

    pub fn commit_with_callback<E, R>(
        &mut self,
        callback: impl FnOnce(Result<(), E>) -> R + Send + 'static,
    ) -> Result<std::thread::JoinHandle<R>, MvccError>
    where
        E: std::error::Error,
        R: Send + 'static,
    {
        let db = self.engine.clone();

        self.tx.commit_with_callback(
            move |ents| {
                db.inner.mem_table.apply(ents);
                Ok(())
            },
            callback,
        )
    }
}

impl<K, V> TransactionTx<K, V>
where
    K: Clone + Ord + Hash + Eq,
    V: 'static,
{
    /// Returns the read version of the transaction.
    pub fn version(&self) -> u64 {
        self.tx.version()
    }

    /// Rollback the transaction.
    pub fn rollback(&mut self) -> Result<(), TransactionError> {
        self.tx.rollback()
    }

    /// Returns true if the given key exists in the database.
    pub fn contains_key(&mut self, key: &K) -> Result<bool, TransactionError> {
        let version = self.tx.version();
        match self.tx.contains_key(key)? {
            Some(true) => Ok(true),
            Some(false) => Ok(false),
            None => Ok(self.engine.inner.mem_table.contains_key(key, version)),
        }
    }

    /// Get a value from the database.
    pub fn get<'a, 'b: 'a>(
        &'a mut self,
        key: &'b K,
    ) -> Result<Option<Ref<'a, K, V>>, TransactionError>
    where
        K: Clone,
    {
        let version = self.tx.version();
        match self.tx.get(key)? {
            Some(v) => {
                if v.value().is_some() {
                    Ok(Some(v.into()))
                } else {
                    Ok(None)
                }
            }
            None => Ok(self.engine.inner.mem_table.get(key, version).map(Into::into)),
        }
    }

    /// Insert a new key-value pair.
    pub fn set(&mut self, key: K, value: V) -> Result<(), TransactionError> {
        self.tx.set(key, value)
    }

    /// Remove a key.
    pub fn remove(&mut self, key: K) -> Result<(), TransactionError> {
        self.tx.remove(key)
    }

    /// Iterate over the entries of the write transaction.
    pub fn iter(
        &mut self,
    ) -> Result<TransactionIter<'_, K, V, BTreeConflict<K>>, TransactionError> {
        let version = self.tx.version();
        let (marker, pm) = self.tx.marker_with_pending_writes();
        let committed = self.engine.inner.mem_table.iter(version);
        let pending = pm.iter();

        Ok(TransactionIter::new(pending, committed, Some(marker)))
    }

    /// Iterate over the entries of the write transaction in reverse order.
    pub fn iter_rev(
        &mut self,
    ) -> Result<WriteTransactionRevIter<'_, K, V, BTreeConflict<K>>, TransactionError> {
        let version = self.tx.version();
        let (marker, pm) = self.tx.marker_with_pending_writes();
        let committed = self.engine.inner.mem_table.iter_rev(version);
        let pending = pm.iter().rev();

        Ok(WriteTransactionRevIter::new(pending, committed, Some(marker)))
    }

    /// Returns an iterator over the subset of entries of the database.
    pub fn range<'a, Q, R>(
        &'a mut self,
        range: R,
    ) -> Result<TransactionRange<'a, Q, R, K, V, BTreeConflict<K>>, TransactionError>
    where
        K: Clone + Borrow<Q>,
        R: RangeBounds<Q> + 'a,
        Q: Ord + ?Sized,
    {
        let version = self.tx.version();
        let (marker, pm) = self.tx.marker_with_pending_writes();
        let start = range.start_bound();
        let end = range.end_bound();
        let pending = pm.range_comparable((start, end));
        let committed = self.engine.inner.mem_table.range(range, version);

        Ok(TransactionRange::new(pending, committed, Some(marker)))
    }

    /// Returns an iterator over the subset of entries of the database in reverse order.
    pub fn range_rev<'a, Q, R>(
        &'a mut self,
        range: R,
    ) -> Result<WriteTransactionRevRange<'a, Q, R, K, V, BTreeConflict<K>>, TransactionError>
    where
        K: Clone + Borrow<Q>,
        R: RangeBounds<Q> + 'a,
        Q: Ord + ?Sized,
    {
        let version = self.tx.version();
        let (marker, pm) = self.tx.marker_with_pending_writes();
        let start = range.start_bound();
        let end = range.end_bound();
        let pending = pm.range_comparable((start, end));
        let committed = self.engine.inner.mem_table.range_rev(range, version);

        Ok(WriteTransactionRevRange::new(pending.rev(), committed, Some(marker)))
    }
}
