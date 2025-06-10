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
use crate::mvcc::transaction::iter_rev::TransactionIterRev;
use crate::mvcc::transaction::range::TransactionRange;
use crate::mvcc::transaction::range_rev::TransactionRangeRev;
use crate::mvcc::types::TransactionValue;
use reifydb_storage::{Delta, Key, KeyRange, Value, Version};
use std::collections::HashMap;
use std::ops::RangeBounds;

pub struct TransactionTx<S: Storage> {
    engine: Optimistic<S>,
    tm: TransactionManagerTx<BTreeConflict, LocalClock, BTreePendingWrites>,
}

impl<S: Storage> TransactionTx<S> {
    pub fn new(engine: Optimistic<S>) -> Self {
        let tm = engine.tm.write().unwrap();
        Self { engine, tm }
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
    pub fn commit(&mut self) -> Result<(), MvccError> {
        self.tm.commit(|pending| {
            let mut grouped: HashMap<Version, Vec<Delta>> = HashMap::new();

            for p in pending {
                grouped.entry(p.version).or_default().push(p.delta);
            }

            for (version, deltas) in grouped {
                self.engine.storage.apply(deltas, version);
            }

            Ok(())
        })
    }
}

impl<S: Storage> TransactionTx<S> {
    pub fn version(&self) -> Version {
        self.tm.version()
    }

    pub fn as_of_version(&mut self, version: Version) {
        self.tm.as_of_version(version);
    }

    pub fn rollback(&mut self) -> Result<(), TransactionError> {
        self.tm.rollback()
    }

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
        let (marker, pw) = self.tm.marker_with_pending_writes();
        let pending = pw.iter();
        let commited = self.engine.storage.scan(version);

        Ok(TransactionIter::new(pending, commited, Some(marker)))
    }

    pub fn scan_rev(
        &mut self,
    ) -> Result<TransactionIterRev<'_, S, BTreeConflict>, TransactionError> {
        let version = self.tm.version();
        let (marker, pw) = self.tm.marker_with_pending_writes();
        let pending = pw.iter().rev();
        let commited = self.engine.storage.scan_rev(version);

        Ok(TransactionIterRev::new(pending, commited, Some(marker)))
    }

    pub fn scan_range<'a>(
        &'a mut self,
        range: KeyRange,
    ) -> Result<TransactionRange<'a, S, BTreeConflict>, TransactionError> {
        let version = self.tm.version();
        let (marker, pw) = self.tm.marker_with_pending_writes();
        let start = range.start_bound();
        let end = range.end_bound();
        let pending = pw.range_comparable((start, end));
        let commited = self.engine.storage.scan_range(range, version);

        Ok(TransactionRange::new(pending, commited, Some(marker)))
    }

    pub fn scan_range_rev<'a>(
        &'a mut self,
        range: KeyRange,
    ) -> Result<TransactionRangeRev<'a, S, BTreeConflict>, TransactionError> {
        let version = self.tm.version();
        let (marker, pw) = self.tm.marker_with_pending_writes();
        let start = range.start_bound();
        let end = range.end_bound();
        let pending = pw.range_comparable((start, end));
        let commited = self.engine.storage.scan_range_rev(range, version);

        Ok(TransactionRangeRev::new(pending.rev(), commited, Some(marker)))
    }

    pub fn scan_prefix<'a>(
        &'a mut self,
        prefix: &Key,
    ) -> Result<TransactionRange<'a, S, BTreeConflict>, TransactionError> {
        self.scan_range(KeyRange::prefix(prefix))
    }

    pub fn scan_prefix_rev<'a>(
        &'a mut self,
        prefix: &Key,
    ) -> Result<TransactionRangeRev<'a, S, BTreeConflict>, TransactionError> {
        self.scan_range_rev(KeyRange::prefix(prefix))
    }
}
