// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use crate::mvcc::pending::{BTreePendingWrites, PendingWritesComparableRange};
use crate::mvcc::transaction::TransactionManagerTx;
use crate::mvcc::transaction::iter::TransactionIter;
use crate::mvcc::transaction::iter_rev::TransactionIterRev;
use crate::mvcc::transaction::range::TransactionRange;
use crate::mvcc::transaction::range_rev::TransactionRangeRev;
use crate::mvcc::transaction::version::StdVersionProvider;
use crate::mvcc::types::TransactionValue;
use reifydb_core::CowVec;
use reifydb_core::delta::Delta;
use reifydb_core::hook::transaction::{PostCommitHook, PreCommitHook};
use reifydb_core::row::EncodedRow;
use std::collections::HashMap;
use std::ops::RangeBounds;

pub struct WriteTransaction<VS: VersionedStorage, UT: UnversionedTransaction> {
    engine: Optimistic<VS, UT>,
    tm: TransactionManagerTx<BTreeConflict, StdVersionProvider<UT>, BTreePendingWrites>,
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> WriteTransaction<VS, UT> {
    pub fn new(engine: Optimistic<VS, UT>) -> crate::Result<Self> {
        let tm = engine.tm.write()?;
        Ok(Self { engine, tm })
    }
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> WriteTransaction<VS, UT> {
    /// Commits the transaction, following these steps:
    ///
    /// 1. If there are no writes, return immediately.
    ///
    /// 2. Check if read rows were updated since txn started. If so, return `transaction_conflict()`.
    ///
    /// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
    ///
    /// 4. Batch up all writes, write them to database.
    ///
    pub fn commit(&mut self) -> Result<(), reifydb_core::Error> {
        self.tm.commit(|pending| {
            let mut grouped: HashMap<Version, CowVec<Delta>> = HashMap::new();

            for p in pending {
                grouped.entry(p.version).or_default().push(p.delta);
            }

            for (version, deltas) in grouped {
                self.engine.hooks.trigger(PreCommitHook { deltas: deltas.clone(), version })?;

                self.engine.versioned.apply(deltas.clone(), version)?;

                self.engine.hooks.trigger(PostCommitHook { deltas, version })?;
            }

            Ok(())
        })
    }
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> WriteTransaction<VS, UT> {
    pub fn version(&self) -> Version {
        self.tm.version()
    }

    pub fn as_of_version(&mut self, version: Version) {
        self.tm.as_of_version(version);
    }

    pub fn rollback(&mut self) -> Result<(), reifydb_core::Error> {
        self.tm.rollback()
    }

    pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool, reifydb_core::Error> {
        let version = self.tm.version();
        match self.tm.contains_key(key)? {
            Some(true) => Ok(true),
            Some(false) => Ok(false),
            None => self.engine.versioned.contains(key, version),
        }
    }

    pub fn get(
        &mut self,
        key: &EncodedKey,
    ) -> Result<Option<TransactionValue>, reifydb_core::Error> {
        let version = self.tm.version();
        match self.tm.get(key)? {
            Some(v) => {
                if v.row().is_some() {
                    Ok(Some(v.into()))
                } else {
                    Ok(None)
                }
            }
            None => Ok(self.engine.versioned.get(key, version)?.map(TransactionValue::from)),
        }
    }

    pub fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<(), reifydb_core::Error> {
        self.tm.set(key, row)
    }

    pub fn remove(&mut self, key: &EncodedKey) -> Result<(), reifydb_core::Error> {
        self.tm.remove(key)
    }

    pub fn scan(&mut self) -> Result<TransactionIter<'_, VS, BTreeConflict>, reifydb_core::Error> {
        let version = self.tm.version();
        let (marker, pw) = self.tm.marker_with_pending_writes();
        let pending = pw.iter();
        let commited = self.engine.versioned.scan(version)?;

        Ok(TransactionIter::new(pending, commited, Some(marker)))
    }

    pub fn scan_rev(
        &mut self,
    ) -> Result<TransactionIterRev<'_, VS, BTreeConflict>, reifydb_core::Error> {
        let version = self.tm.version();
        let (marker, pw) = self.tm.marker_with_pending_writes();
        let pending = pw.iter().rev();
        let commited = self.engine.versioned.scan_rev(version)?;

        Ok(TransactionIterRev::new(pending, commited, Some(marker)))
    }

    pub fn scan_range(
        &mut self,
        range: EncodedKeyRange,
    ) -> Result<TransactionRange<'_, VS, BTreeConflict>, reifydb_core::Error> {
        let version = self.tm.version();
        let (marker, pw) = self.tm.marker_with_pending_writes();
        let start = range.start_bound();
        let end = range.end_bound();
        let pending = pw.range_comparable((start, end));
        let commited = self.engine.versioned.scan_range(range, version)?;

        Ok(TransactionRange::new(pending, commited, Some(marker)))
    }

    pub fn scan_range_rev(
        &mut self,
        range: EncodedKeyRange,
    ) -> Result<TransactionRangeRev<'_, VS, BTreeConflict>, reifydb_core::Error> {
        let version = self.tm.version();
        let (marker, pw) = self.tm.marker_with_pending_writes();
        let start = range.start_bound();
        let end = range.end_bound();
        let pending = pw.range_comparable((start, end));
        let commited = self.engine.versioned.scan_range_rev(range, version)?;

        Ok(TransactionRangeRev::new(pending.rev(), commited, Some(marker)))
    }

    pub fn scan_prefix<'a>(
        &'a mut self,
        prefix: &EncodedKey,
    ) -> Result<TransactionRange<'a, VS, BTreeConflict>, reifydb_core::Error> {
        self.scan_range(EncodedKeyRange::prefix(prefix))
    }

    pub fn scan_prefix_rev<'a>(
        &'a mut self,
        prefix: &EncodedKey,
    ) -> Result<TransactionRangeRev<'a, VS, BTreeConflict>, reifydb_core::Error> {
        self.scan_range_rev(EncodedKeyRange::prefix(prefix))
    }
}
