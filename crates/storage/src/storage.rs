// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{StoredValue, Version};
use reifydb_persistence::{Action, Key};
use std::ops::RangeBounds;

pub trait Storage<R>:
    Send + Sync + Apply + Get + Contains + Scan + ScanRev + ScanRange<R> + ScanRangeRev<R>
where
    R: RangeBounds<Key>,
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

pub trait ScanRangeIterator<R>: Iterator<Item = StoredValue>
where
    R: RangeBounds<Key>,
{
}

impl<T, R> ScanRangeIterator<R> for T
where
    T: Iterator<Item = StoredValue>,
    R: RangeBounds<Key>,
{
}

pub trait ScanRange<R>
where
    R: RangeBounds<Key>,
{
    type ScanRangeIter<'a>: ScanRangeIterator<R>
    where
        Self: 'a;

    fn scan_range(&self, range: R, version: Version) -> Self::ScanRangeIter<'_>;
}

pub trait ScanRangeIteratorRev<R>: Iterator<Item = StoredValue>
where
    R: RangeBounds<Key>,
{
}

impl<T, R> ScanRangeIteratorRev<R> for T
where
    T: Iterator<Item = StoredValue>,
    R: RangeBounds<Key>,
{
}

pub trait ScanRangeRev<R>
where
    R: RangeBounds<Key>,
{
    type ScanRangeIterRev<'a>: ScanRangeIteratorRev<R>
    where
        Self: 'a;

    fn scan_range_rev(&self, range: R, version: Version) -> Self::ScanRangeIterRev<'_>;
}
