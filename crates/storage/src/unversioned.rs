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
    + UnversionedSet
    + UnversionedRemove
    + UnversionedScan
    + UnversionedScanRange
{
}

pub trait UnversionedApply {
    fn apply_unversioned(&mut self, delta: AsyncCowVec<Delta>);
}

pub trait UnversionedGet {
    fn get_unversioned(&self, key: &EncodedKey) -> Option<Unversioned>;
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

pub trait UnversionedScanIterator: Iterator<Item = Unversioned> {}
impl<T> UnversionedScanIterator for T where T: Iterator<Item = Unversioned> {}

pub trait UnversionedScan {
    type ScanIter<'a>: UnversionedScanIterator
    where
        Self: 'a;

    fn scan_unversioned(&self) -> Self::ScanIter<'_>;
}

pub trait UnversionedScanRangeIterator: Iterator<Item = Unversioned> {}

impl<T> UnversionedScanRangeIterator for T where T: Iterator<Item = Unversioned> {}

pub trait UnversionedScanRange {
    type ScanRangeIter<'a>: UnversionedScanRangeIterator
    where
        Self: 'a;

    fn scan_range(&self, range: EncodedKeyRange) -> Self::ScanRangeIter<'_>;

    fn scan_prefix(&self, prefix: &EncodedKey) -> Self::ScanRangeIter<'_> {
        self.scan_range(EncodedKeyRange::prefix(prefix))
    }
}
