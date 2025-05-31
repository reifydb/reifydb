// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::mvcc::transaction::range_rev::TransactionRevRange;

use super::*;
use crate::mvcc::error::{MvccError, TransactionError};
use crate::mvcc::pending::{BTreePendingWrites, PendingWritesComparableRange};
use crate::mvcc::transaction::TransactionManagerTx;
use crate::mvcc::transaction::iter::TransactionIter;
use crate::mvcc::transaction::iter_rev::TransactionRevIter;
use crate::mvcc::transaction::range::TransactionRange;
use crate::mvcc::types::TransactionValue;
use reifydb_persistence::{Key, Value};
use reifydb_storage::{Contains, Get, Scan, ScanRange, ScanRangeRev, ScanRev};
use std::ops::Bound;
use std::ops::RangeBounds;

#[cfg(test)]
mod tests;

/// A serializable snapshot isolation transaction over the [`Serializable`],
pub struct SerializableTransaction {
    pub(in crate::mvcc) db: Serializable,
    pub(in crate::mvcc) wtm: TransactionManagerTx<BTreeConflict, BTreePendingWrites>,
}

impl SerializableTransaction {
    pub fn new(db: Serializable) -> Self {
        let wtm = db.inner.tm.write().unwrap();
        Self { db, wtm }
    }
}

impl SerializableTransaction {
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
        // self.wtm.commit(|ents| {
        //     self.db.inner.map.apply(ents);
        //     Ok(())
        // })
        unimplemented!()
    }
}

impl SerializableTransaction {
    /// Returns the read version of the transaction.
    pub fn version(&self) -> u64 {
        self.wtm.version()
    }

    /// Rollback the transaction.

    pub fn rollback(&mut self) -> Result<(), TransactionError> {
        self.wtm.rollback()
    }

    /// Returns true if the given key exists in the database.

    pub fn contains_key(&mut self, key: &Key) -> Result<bool, TransactionError> {
        let version = self.wtm.version();
        match self.wtm.contains_key(key)? {
            Some(true) => Ok(true),
            Some(false) => Ok(false),
            None => Ok(self.db.inner.map.contains(key, version)),
        }
    }

    /// Get a value from the database.
    pub fn get<'a, 'b: 'a>(
        &'a mut self,
        key: &'b Key,
    ) -> Result<Option<TransactionValue>, TransactionError> {
        let version = self.wtm.version();
        match self.wtm.get(key)? {
            Some(v) => {
                if v.value().is_some() {
                    Ok(Some(v.into()))
                } else {
                    Ok(None)
                }
            }
            None => Ok(self.db.inner.map.get(key, version).map(Into::into)),
        }
    }

    /// Insert a new key-value pair.
    pub fn set(&mut self, key: Key, value: Value) -> Result<(), TransactionError> {
        self.wtm.set(key, value)
    }

    /// Remove a key.
    pub fn remove(&mut self, key: Key) -> Result<(), TransactionError> {
        self.wtm.remove(key)
    }

    /// Iterate over the entries of the write transaction.
    pub fn scan(&mut self) -> Result<TransactionIter<'_, BTreeConflict>, TransactionError> {
        let version = self.wtm.version();
        let (mut marker, pm) = self.wtm.marker_with_pending_writes();

        let start: Bound<Key> = Bound::Unbounded;
        let end: Bound<Key> = Bound::Unbounded;
        marker.mark_range((start, end));
        let committed = self.db.inner.map.scan(version);
        let pending = pm.iter();

        Ok(TransactionIter::new(pending, committed, None))
    }

    /// Iterate over the entries of the write transaction in reverse order.
    pub fn scan_rev(&mut self) -> Result<TransactionRevIter<'_, BTreeConflict>, TransactionError> {
        let version = self.wtm.version();
        let (mut marker, pm) = self.wtm.marker_with_pending_writes();
        let start: Bound<Key> = Bound::Unbounded;
        let end: Bound<Key> = Bound::Unbounded;
        marker.mark_range((start, end));
        let committed = self.db.inner.map.scan_rev(version);
        let pending = pm.iter().rev();

        Ok(TransactionRevIter::new(pending, committed, None))
    }

    /// Returns an iterator over the subset of entries of the database.
    pub fn scan_range<'a, R>(
        &'a mut self,
        range: R,
    ) -> Result<TransactionRange<'a, R, BTreeConflict>, TransactionError>
    where
        R: RangeBounds<Key> + 'a,
    {
        let version = self.wtm.version();
        let (mut marker, pm) = self.wtm.marker_with_pending_writes();
        let start = range.start_bound();
        let end = range.end_bound();
        marker.mark_range((start, end));
        let pending = pm.range_comparable((start, end));
        let committed = self.db.inner.map.scan_range(range, version);

        Ok(TransactionRange::new(pending, committed, Some(marker)))
    }

    /// Returns an iterator over the subset of entries of the database in reverse order.
    pub fn scan_range_rev<'a, R>(
        &'a mut self,
        range: R,
    ) -> Result<TransactionRevRange<'a, R, BTreeConflict>, TransactionError>
    where
        R: RangeBounds<Key> + 'a,
    {
        let version = self.wtm.version();
        let (mut marker, pm) = self.wtm.marker_with_pending_writes();
        let start = range.start_bound();
        let end = range.end_bound();
        marker.mark_range((start, end));
        let pending = pm.range_comparable((start, end)).rev();
        let committed = self.db.inner.map.scan_range_rev(range, version);

        Ok(TransactionRevRange::new(pending, committed, Some(marker)))
    }
}
