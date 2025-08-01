// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::*;
use crate::svl::range::SvlRange;
use crate::svl::range_rev::SvlRangeRev;
use crate::svl::scan::SvlScan;
use crate::svl::scan_rev::SvlScanRev;
use reifydb_core::interface::{BoxedUnversionedIter, ReadTransaction, WriteTransaction};
use std::collections::HashMap;
use std::mem::take;
use std::ops::RangeBounds;
use std::sync::atomic::Ordering;

pub struct SvlWriteTransaction<US> {
    svl: Arc<SvlInner<US>>,
    pending: HashMap<EncodedKey, Delta>,
    completed: bool,
}

impl<US> ReadTransaction for SvlWriteTransaction<US>
where
    US: UnversionedStorage,
{
    type Item = Unversioned;
    type Iter<'a> = BoxedUnversionedIter<'a>;

    fn get(&mut self, key: &EncodedKey) -> reifydb_core::Result<Option<Self::Item>> {
        if let Some(delta) = self.pending.get(key) {
            return match delta {
                Delta::Insert { row, .. }
                | Delta::Update { row, .. }
                | Delta::Upsert { row, .. } => {
                    Ok(Some(Unversioned { key: key.clone(), row: row.clone() }))
                }
                Delta::Remove { .. } => Ok(None),
            };
        }

        let storage = self.svl.storage.read().unwrap();
        storage.get(key)
    }

    fn contains_key(&mut self, key: &EncodedKey) -> reifydb_core::Result<bool> {
        if let Some(delta) = self.pending.get(key) {
            return match delta {
                Delta::Insert { .. } | Delta::Update { .. } | Delta::Upsert { .. } => Ok(true),
                Delta::Remove { .. } => Ok(false),
            };
        }

        // Then check storage
        let storage = self.svl.storage.read().unwrap();
        storage.contains(key)
    }

    fn scan(&mut self) -> crate::Result<Self::Iter<'_>> {
        let (pending_items, committed_items) = self.prepare_scan_data(None, false)?;
        let iter = SvlScan::new(pending_items.into_iter(), committed_items.into_iter());
        Ok(Box::new(iter))
    }

    fn scan_rev(&mut self) -> crate::Result<Self::Iter<'_>> {
        let (pending_items, committed_items) = self.prepare_scan_data(None, true)?;
        let iter = SvlScanRev::new(pending_items.into_iter(), committed_items.into_iter());
        Ok(Box::new(iter))
    }

    fn range(&mut self, range: EncodedKeyRange) -> crate::Result<Self::Iter<'_>> {
        let (pending_items, committed_items) =
            self.prepare_scan_data(Some(range.clone()), false)?;
        let iter = SvlRange::new(pending_items.into_iter(), committed_items.into_iter());
        Ok(Box::new(iter))
    }

    fn range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<Self::Iter<'_>> {
        let (pending_items, committed_items) = self.prepare_scan_data(Some(range.clone()), true)?;
        let iter = SvlRangeRev::new(pending_items.into_iter(), committed_items.into_iter());
        Ok(Box::new(iter))
    }
}

impl<US> SvlWriteTransaction<US>
where
    US: UnversionedStorage,
{
    pub(super) fn new(svl: Arc<SvlInner<US>>) -> Self {
        Self { svl, pending: HashMap::new(), completed: false }
    }

    /// Helper method to prepare scan data by cloning and sorting pending items
    /// and collecting committed items from storage.
    fn prepare_scan_data(
        &mut self,
        range: Option<EncodedKeyRange>,
        reverse: bool,
    ) -> crate::Result<(Vec<(EncodedKey, Delta)>, Vec<Unversioned>)> {
        // Clone and optionally filter pending items from the buffer
        let mut pending_items: Vec<(EncodedKey, Delta)> = match &range {
            Some(r) => self
                .pending
                .iter()
                .filter(|(k, _)| r.contains(&**k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            None => self.pending.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        };

        // Sort pending items by key (forward or reverse)
        if reverse {
            pending_items.sort_by(|(l, _), (r, _)| r.cmp(l));
        } else {
            pending_items.sort_by(|(l, _), (r, _)| l.cmp(r));
        }

        // Get committed items from storage, collecting them to release the lock
        let committed_items: Vec<Unversioned> = {
            let storage = self.svl.storage.read().unwrap();
            match (range, reverse) {
                (Some(r), true) => storage.scan_range_rev(r)?.collect(),
                (Some(r), false) => storage.scan_range(r)?.collect(),
                (None, true) => storage.scan_rev()?.collect(),
                (None, false) => storage.scan()?.collect(),
            }
        };

        Ok((pending_items, committed_items))
    }
}

impl<US> WriteTransaction for SvlWriteTransaction<US>
where
    US: UnversionedStorage,
{
    fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> crate::Result<()> {
        let delta = if self.pending.contains_key(key) {
            Delta::Update { key: key.clone(), row }
        } else {
            Delta::Insert { key: key.clone(), row }
        };
        self.pending.insert(key.clone(), delta);
        Ok(())
    }

    fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
        self.pending.insert(key.clone(), Delta::Remove { key: key.clone() });
        Ok(())
    }

    fn commit(mut self) -> crate::Result<()> {
        let deltas: Vec<Delta> =
            take(&mut self.pending).into_iter().map(|(_, delta)| delta).collect();

        if !deltas.is_empty() {
            let mut storage = self.svl.storage.write().unwrap();
            storage.apply(CowVec::new(deltas))?;
        }

        self.completed = true;
        Ok(())
    }

    fn rollback(mut self) -> crate::Result<()> {
        self.pending.clear();
        self.completed = true;
        Ok(())
    }
}

impl<S> Drop for SvlWriteTransaction<S> {
    fn drop(&mut self) {
        if !self.completed {
            // Auto-rollback: just clear the buffer
            self.pending.clear();
        }

        // Release write lock atomically
        self.svl.write_active.store(false, Ordering::Release);
    }
}
