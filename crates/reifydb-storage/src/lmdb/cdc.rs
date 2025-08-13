// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::lmdb::Lmdb;
use crossbeam_skiplist::map::Entry;
use reifydb_core::interface::{
    CdcCount, CdcEvent, CdcEventKey, CdcGet, CdcRange, CdcScan, CdcStorage,
};
use reifydb_core::Version;
use std::collections::Bound;

pub struct Range<'a> {
    iter: Box<dyn Iterator<Item = Entry<'a, CdcEventKey, CdcEvent>> + 'a>,
}

impl<'a> Iterator for Range<'a> {
    type Item = CdcEvent;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|entry| entry.value().clone())
    }
}

pub struct Scan<'a> {
    iter: Box<dyn Iterator<Item = Entry<'a, CdcEventKey, CdcEvent>> + 'a>,
}

impl<'a> Iterator for Scan<'a> {
    type Item = CdcEvent;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|entry| entry.value().clone())
    }
}

impl CdcGet for Lmdb {
    fn get(&self, _version: Version) -> reifydb_core::Result<Vec<CdcEvent>> {
        todo!()
    }
}

impl CdcRange for Lmdb {
    type RangeIter<'a> = Range<'a>;

    fn range(
        &self,
        _start: Bound<Version>,
        _end: Bound<Version>,
    ) -> reifydb_core::Result<Self::RangeIter<'_>> {
        todo!()
    }
}

impl CdcScan for Lmdb {
    type ScanIter<'a> = Scan<'a>;

    fn scan(&self) -> reifydb_core::Result<Self::ScanIter<'_>> {
        todo!()
    }
}

impl CdcCount for Lmdb {
    fn count(&self, _version: Version) -> reifydb_core::Result<usize> {
        todo!()
    }
}

impl CdcStorage for Lmdb {}
