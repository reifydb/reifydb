// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{GetHooks, Versioned};
use reifydb_core::delta::Delta;
use reifydb_core::{AsyncCowVec, EncodedKey, EncodedKeyRange, Version};

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

    fn scan_range_rev(&self, range: EncodedKeyRange, version: Version) -> Self::ScanRangeIterRev<'_>;

    fn scan_prefix_rev(&self, prefix: &EncodedKey, version: Version) -> Self::ScanRangeIterRev<'_> {
        self.scan_range_rev(EncodedKeyRange::prefix(prefix), version)
    }
}
