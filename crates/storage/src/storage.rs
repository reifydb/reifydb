// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Stored;
use reifydb_core::delta::Delta;
use reifydb_core::hook::Hooks;
use reifydb_core::{AsyncCowVec, Key, KeyRange, Version};

pub trait Storage:
    Send + Sync + Apply + Clone + Get + GetHooks + Contains + Scan + ScanRev + ScanRange + ScanRangeRev
{
}

pub trait GetHooks {
    fn hooks(&self) -> Hooks;
}

pub trait Apply {
    fn apply(&self, delta: AsyncCowVec<Delta>, version: Version);
}

pub trait Get {
    fn get(&self, key: &Key, version: Version) -> Option<Stored>;
}

pub trait Contains {
    fn contains(&self, key: &Key, version: Version) -> bool;
}

pub trait ScanIterator: Iterator<Item = Stored> {}
impl<T> ScanIterator for T where T: Iterator<Item = Stored> {}

pub trait Scan {
    type ScanIter<'a>: ScanIterator
    where
        Self: 'a;

    fn scan(&self, version: Version) -> Self::ScanIter<'_>;
}

pub trait ScanIteratorRev: Iterator<Item = Stored> {}
impl<T> ScanIteratorRev for T where T: Iterator<Item = Stored> {}

pub trait ScanRev {
    type ScanIterRev<'a>: ScanIteratorRev
    where
        Self: 'a;

    fn scan_rev(&self, version: Version) -> Self::ScanIterRev<'_>;
}

pub trait ScanRangeIterator: Iterator<Item = Stored> {}

impl<T> ScanRangeIterator for T where T: Iterator<Item = Stored> {}

pub trait ScanRange {
    type ScanRangeIter<'a>: ScanRangeIterator
    where
        Self: 'a;

    fn scan_range(&self, range: KeyRange, version: Version) -> Self::ScanRangeIter<'_>;

    fn scan_prefix(&self, prefix: &Key, version: Version) -> Self::ScanRangeIter<'_> {
        self.scan_range(KeyRange::prefix(prefix), version)
    }
}

pub trait ScanRangeIteratorRev: Iterator<Item = Stored> {}

impl<T> ScanRangeIteratorRev for T where T: Iterator<Item = Stored> {}

pub trait ScanRangeRev {
    type ScanRangeIterRev<'a>: ScanRangeIteratorRev
    where
        Self: 'a;

    fn scan_range_rev(&self, range: KeyRange, version: Version) -> Self::ScanRangeIterRev<'_>;

    fn scan_prefix_rev(&self, prefix: &Key, version: Version) -> Self::ScanRangeIterRev<'_> {
        self.scan_range_rev(KeyRange::prefix(prefix), version)
    }
}
