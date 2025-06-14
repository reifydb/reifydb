// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{GetHooks, Unversioned};
use reifydb_core::delta::Delta;
use reifydb_core::row::EncodedRow;
use reifydb_core::{AsyncCowVec, EncodedKey};

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
