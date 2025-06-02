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
use crate::mvcc::transaction::TransactionManagerTx;
use crate::mvcc::transaction::iter::TransactionIter;
use crate::mvcc::transaction::iter_rev::TransactionRevIter;
use crate::mvcc::transaction::range::TransactionRange;
use crate::mvcc::transaction::range_rev::TransactionRevRange;
use crate::mvcc::types::TransactionValue;
use reifydb_storage::{Key, KeyRange, Value, Version};
use std::ops::RangeBounds;

/// A optimistic concurrency control transaction over the [`Optimistic`].
pub struct TransactionTx<S: Storage> {
    engine: Optimistic<S>,
    tm: TransactionManagerTx<BTreeConflict, LocalClock, BTreePendingWrites>,
}

impl<S: Storage> TransactionTx<S> {
    pub fn new(db: Optimistic<S>) -> Self {
        let tm = db.tm.write().unwrap();
        Self { engine: db, tm }
    }
}

impl<S: Storage> TransactionTx<S> {
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
        self.tm.commit(|pending| {
            self.engine
                .storage
                .apply((pending.into_iter().map(|p| (p.action, p.version)).collect()));
            Ok(())
        })
    }
}

impl<S: Storage> TransactionTx<S> {
    /// Returns the read version of the transaction.
    pub fn version(&self) -> Version {
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
            None => Ok(self.engine.storage.contains(key, version)),
        }
    }

    pub fn get(&mut self, key: &Key) -> Result<Option<TransactionValue>, TransactionError> {
        let version = self.tm.version();
        match self.tm.get(key)? {
            Some(v) => {
                if v.value().is_some() {
                    Ok(Some(v.into()))
                } else {
                    Ok(None)
                }
            }
            None => Ok(self.engine.storage.get(key, version).map(Into::into)),
        }
    }

    pub fn set(&mut self, key: Key, value: Value) -> Result<(), TransactionError> {
        self.tm.set(key, value)
    }

    pub fn remove(&mut self, key: Key) -> Result<(), TransactionError> {
        self.tm.remove(key)
    }

    pub fn scan(&mut self) -> Result<TransactionIter<'_, S, BTreeConflict>, TransactionError> {
        let version = self.tm.version();
        let (marker, pm) = self.tm.marker_with_pending_writes();
        let pending = pm.iter();
        let commited = self.engine.storage.scan(version);

        Ok(TransactionIter::new(pending, commited, Some(marker)))
    }

    pub fn scan_rev(
        &mut self,
    ) -> Result<TransactionRevIter<'_, S, BTreeConflict>, TransactionError> {
        let version = self.tm.version();
        let (marker, pm) = self.tm.marker_with_pending_writes();
        let pending = pm.iter().rev();
        let commited = self.engine.storage.scan_rev(version);

        Ok(TransactionRevIter::new(pending, commited, Some(marker)))
    }

    pub fn scan_range<'a>(
        &'a mut self,
        range: KeyRange,
    ) -> Result<TransactionRange<'a, S, BTreeConflict>, TransactionError> {
        let version = self.tm.version();
        let (marker, pm) = self.tm.marker_with_pending_writes();
        let start = range.start_bound();
        let end = range.end_bound();
        let pending = pm.range_comparable((start, end));
        let commited = self.engine.storage.scan_range(range, version);

        Ok(TransactionRange::new(pending, commited, Some(marker)))
    }

    pub fn scan_range_rev<'a>(
        &'a mut self,
        range: KeyRange,
    ) -> Result<TransactionRevRange<'a, S, BTreeConflict>, TransactionError> {
        let version = self.tm.version();
        let (marker, pm) = self.tm.marker_with_pending_writes();
        let start = range.start_bound();
        let end = range.end_bound();
        let pending = pm.range_comparable((start, end));
        let commited = self.engine.storage.scan_range_rev(range, version);

        Ok(TransactionRevRange::new(pending.rev(), commited, Some(marker)))
    }
}
