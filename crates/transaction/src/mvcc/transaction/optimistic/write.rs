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
use reifydb_storage::Version;
use crate::mvcc::error::{MvccError, TransactionError};
use crate::mvcc::pending::{BTreePendingWrites, PendingWritesComparableRange};
use crate::mvcc::types::TransactionValue;
use crate::mvcc::transaction::TransactionManagerTx;
use crate::mvcc::transaction::scan::iter::TransactionIter;
use crate::mvcc::transaction::scan::range::TransactionRange;
use crate::mvcc::transaction::scan::rev_iter::TransactionRevIter;
use crate::mvcc::transaction::scan::rev_range::TransactionRevRange;
use reifydb_persistence::{Key, Value};
use std::ops::RangeBounds;

/// A optimistic concurrency control transaction over the [`Optimistic`].
pub struct TransactionTx {
    engine: Optimistic,
    tm: TransactionManagerTx<BTreeConflict, BTreePendingWrites>,
}

impl TransactionTx {
    pub fn new(db: Optimistic) -> Self {
        let tm = db.inner.tm.write().unwrap();
        Self { engine: db, tm }
    }
}

impl TransactionTx {
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
        self.tm.commit(|operations| {
            self.engine.inner.store.apply(operations);
            Ok(())
        })
    }
}

impl TransactionTx {
    /// Returns the read version of the transaction.
    pub fn version(&self) -> u64 {
        self.tm.version()
    }

    pub fn as_of_version(&mut self, version: Version) {
        self.tm.as_of_version(version);
    }

    /// Rollback the transaction.
    pub fn rollback(&mut self) -> Result<(), TransactionError> {
        self.tm.rollback()
    }

    /// Returns true if the given key exists in the database.
    pub fn contains_key(&mut self, key: &Key) -> Result<bool, TransactionError> {
        let version = self.tm.version();
        match self.tm.contains_key(key)? {
            Some(true) => Ok(true),
            Some(false) => Ok(false),
            None => Ok(self.engine.inner.store.contains_key(key, version)),
        }
    }

    /// Get a value from the database.
    pub fn get<'a, 'b: 'a>(&'a mut self, key: &'b Key) -> Result<Option<TransactionValue>, TransactionError> {
        let version = self.tm.version();
        match self.tm.get(key)? {
            Some(v) => {
                if v.value().is_some() {
                    Ok(Some(v.into()))
                } else {
                    Ok(None)
                }
            }
            None => Ok(self.engine.inner.store.get(key, version).map(Into::into)),
        }
    }

    /// Set a new key-value pair.
    pub fn set(&mut self, key: Key, value: Value) -> Result<(), TransactionError> {
        self.tm.set(key, value)
    }

    /// Remove a key.
    pub fn remove(&mut self, key: Key) -> Result<(), TransactionError> {
        self.tm.remove(key)
    }

    /// Iterate over the entries of the write transaction.
    pub fn iter(&mut self) -> Result<TransactionIter<'_, BTreeConflict>, TransactionError> {
        let version = self.tm.version();
        let (marker, pm) = self.tm.marker_with_pending_writes();
        let committed = self.engine.inner.store.iter(version);
        let pending = pm.iter();

        Ok(TransactionIter::new(pending, committed, Some(marker)))
    }

    /// Iterate over the entries of the write transaction in reverse order.
    pub fn iter_rev(&mut self) -> Result<TransactionRevIter<'_, BTreeConflict>, TransactionError> {
        let version = self.tm.version();
        let (marker, pm) = self.tm.marker_with_pending_writes();
        let committed = self.engine.inner.store.iter_rev(version);
        let pending = pm.iter().rev();

        Ok(TransactionRevIter::new(pending, committed, Some(marker)))
    }

    /// Returns an iterator over the subset of entries of the database.
    pub fn range<'a, R>(
        &'a mut self,
        range: R,
    ) -> Result<TransactionRange<'a, R, BTreeConflict>, TransactionError>
    where
        R: RangeBounds<Key> + 'a,
    {
        let version = self.tm.version();
        let (marker, pm) = self.tm.marker_with_pending_writes();
        let start = range.start_bound();
        let end = range.end_bound();
        let pending = pm.range_comparable((start, end));
        let committed = self.engine.inner.store.range(range, version);

        Ok(TransactionRange::new(pending, committed, Some(marker)))
    }

    /// Returns an iterator over the subset of entries of the database in reverse order.
    pub fn range_rev<'a, R>(
        &'a mut self,
        range: R,
    ) -> Result<TransactionRevRange<'a, R, BTreeConflict>, TransactionError>
    where
        R: RangeBounds<Key> + 'a,
    {
        let version = self.tm.version();
        let (marker, pm) = self.tm.marker_with_pending_writes();
        let start = range.start_bound();
        let end = range.end_bound();
        let pending = pm.range_comparable((start, end));
        let committed = self.engine.inner.store.range_rev(range, version);

        Ok(TransactionRevRange::new(pending.rev(), committed, Some(marker)))
    }
}
