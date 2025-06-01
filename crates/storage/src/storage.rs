// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{StoredValue, Version};
use reifydb_persistence::{Action, Key, KeyRange};

pub trait Storage:
    Send + Sync + Apply + Get + Contains + Scan + ScanRev + ScanRange + ScanRangeRev
{
}

pub trait Apply {
    fn apply(&self, actions: Vec<(Action, Version)>);
}

pub trait Get {
    fn get(&self, key: &Key, version: Version) -> Option<StoredValue>;
}

pub trait Contains {
    fn contains(&self, key: &Key, version: Version) -> bool;
}

pub trait ScanIterator: Iterator<Item = StoredValue> {}
impl<T> ScanIterator for T where T: Iterator<Item = StoredValue> {}

pub trait Scan {
    type ScanIter<'a>: ScanIterator
    where
        Self: 'a;

    fn scan(&self, version: Version) -> Self::ScanIter<'_>;
}

pub trait ScanIteratorRev: Iterator<Item = StoredValue> {}
impl<T> ScanIteratorRev for T where T: Iterator<Item = StoredValue> {}

pub trait ScanRev {
    type ScanIterRev<'a>: ScanIteratorRev
    where
        Self: 'a;

    fn scan_rev(&self, version: Version) -> Self::ScanIterRev<'_>;
}

pub trait ScanRangeIterator: Iterator<Item = StoredValue> {}

impl<T> ScanRangeIterator for T where T: Iterator<Item = StoredValue> {}

pub trait ScanRange {
    type ScanRangeIter<'a>: ScanRangeIterator
    where
        Self: 'a;

    fn scan_range(&self, range: KeyRange, version: Version) -> Self::ScanRangeIter<'_>;
}

pub trait ScanRangeIteratorRev: Iterator<Item = StoredValue> {}

impl<T> ScanRangeIteratorRev for T where T: Iterator<Item = StoredValue> {}

pub trait ScanRangeRev {
    type ScanRangeIterRev<'a>: ScanRangeIteratorRev
    where
        Self: 'a;

    fn scan_range_rev(&self, range: KeyRange, version: Version) -> Self::ScanRangeIterRev<'_>;
}
