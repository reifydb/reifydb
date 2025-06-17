// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{GetHooks, Unversioned};
use reifydb_core::delta::Delta;
use reifydb_core::row::EncodedRow;
use reifydb_core::{AsyncCowVec, EncodedKey, EncodedKeyRange};

pub trait UnversionedStorage:
    Send
    + Sync
    + Clone
    + GetHooks
    + UnversionedApply
    + UnversionedGet
    + UnversionedContains
    + UnversionedSet
    + UnversionedRemove
    + UnversionedScan
    + UnversionedScanRev
    + UnversionedScanRange
    + UnversionedScanRangeRev
{
}

pub trait UnversionedApply {
    fn apply_unversioned(&mut self, delta: AsyncCowVec<Delta>);
}

pub trait UnversionedGet {
    fn get_unversioned(&self, key: &EncodedKey) -> Option<Unversioned>;
}

pub trait UnversionedContains {
    fn contains_unversioned(&self, key: &EncodedKey) -> bool;
}

pub trait UnversionedSet: UnversionedApply {
    fn set_unversioned(&mut self, key: &EncodedKey, row: EncodedRow) {
        Self::apply_unversioned(
            self,
            AsyncCowVec::new(vec![Delta::Set { key: key.clone(), row: row.clone() }]),
        )
    }
}

pub trait UnversionedRemove: UnversionedApply {
    fn remove_unversioned(&mut self, key: &EncodedKey) {
        Self::apply_unversioned(self, AsyncCowVec::new(vec![Delta::Remove { key: key.clone() }]))
    }
}

pub trait UnversionedIter: Iterator<Item = Unversioned> {}
impl<T> UnversionedIter for T where T: Iterator<Item = Unversioned> {}

pub trait UnversionedScan {
    type ScanIter<'a>: UnversionedIter
    where
        Self: 'a;

    fn scan_unversioned(&self) -> Self::ScanIter<'_>;
}

pub trait UnversionedScanRev {
    type ScanIterRev<'a>: UnversionedIter
    where
        Self: 'a;

    fn scan_rev_unversioned(&self) -> Self::ScanIterRev<'_>;
}

pub trait UnversionedScanRange {
    type ScanRange<'a>: UnversionedIter
    where
        Self: 'a;

    fn scan_range_unversioned(&self, range: EncodedKeyRange) -> Self::ScanRange<'_>;

    fn scan_prefix_unversioned(&self, prefix: &EncodedKey) -> Self::ScanRange<'_> {
        self.scan_range_unversioned(EncodedKeyRange::prefix(prefix))
    }
}

pub trait UnversionedScanRangeRev {
    type ScanRangeRev<'a>: UnversionedIter
    where
        Self: 'a;

    fn scan_range_rev_unversioned(&self, range: EncodedKeyRange) -> Self::ScanRangeRev<'_>;

    fn scan_prefix_rev_unversioned(&self, prefix: &EncodedKey) -> Self::ScanRangeRev<'_> {
        self.scan_range_rev_unversioned(EncodedKeyRange::prefix(prefix))
    }
}
