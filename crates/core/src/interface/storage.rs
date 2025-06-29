// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::delta::Delta;
use crate::interface::GetHooks;
use crate::row::EncodedRow;
use crate::{AsyncCowVec, EncodedKey, EncodedKeyRange, Version};

#[derive(Debug)]
pub struct Versioned {
    pub key: EncodedKey,
    pub row: EncodedRow,
    pub version: Version,
}

#[derive(Debug)]
pub struct Unversioned {
    pub key: EncodedKey,
    pub row: EncodedRow,
}

pub trait Storage: VersionedStorage + UnversionedStorage {}

pub trait VersionedStorage:
    Send
    + Sync
    + Clone
    + GetHooks
    + VersionedApply
    + VersionedGet
    + VersionedContains
    + VersionedScan
    + VersionedScanRev
    + VersionedScanRange
    + VersionedScanRangeRev
{
}

pub trait VersionedApply {
    fn apply(&self, delta: AsyncCowVec<Delta>, version: Version);
}

pub trait VersionedGet {
    fn get(&self, key: &EncodedKey, version: Version) -> Option<Versioned>;
}

pub trait VersionedContains {
    fn contains(&self, key: &EncodedKey, version: Version) -> bool;
}

pub trait VersionedIter: Iterator<Item = Versioned> {}
impl<T> VersionedIter for T where T: Iterator<Item = Versioned> {}

pub trait VersionedScan {
    type ScanIter<'a>: VersionedIter
    where
        Self: 'a;

    fn scan(&self, version: Version) -> Self::ScanIter<'_>;
}

pub trait VersionedScanRev {
    type ScanIterRev<'a>: VersionedIter
    where
        Self: 'a;

    fn scan_rev(&self, version: Version) -> Self::ScanIterRev<'_>;
}

pub trait VersionedScanRange {
    type ScanRangeIter<'a>: VersionedIter
    where
        Self: 'a;

    fn scan_range(&self, range: EncodedKeyRange, version: Version) -> Self::ScanRangeIter<'_>;

    fn scan_prefix(&self, prefix: &EncodedKey, version: Version) -> Self::ScanRangeIter<'_> {
        self.scan_range(EncodedKeyRange::prefix(prefix), version)
    }
}

pub trait VersionedScanRangeRev {
    type ScanRangeIterRev<'a>: VersionedIter
    where
        Self: 'a;

    fn scan_range_rev(
        &self,
        range: EncodedKeyRange,
        version: Version,
    ) -> Self::ScanRangeIterRev<'_>;

    fn scan_prefix_rev(&self, prefix: &EncodedKey, version: Version) -> Self::ScanRangeIterRev<'_> {
        self.scan_range_rev(EncodedKeyRange::prefix(prefix), version)
    }
}

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
