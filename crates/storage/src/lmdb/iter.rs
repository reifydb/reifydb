// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::lmdb::Lmdb;
use crate::{VersionedScan, Versioned};
use reifydb_core::Version;

impl VersionedScan for Lmdb {
    type ScanIter<'a> = Iter;

    fn scan(&self, version: Version) -> Self::ScanIter<'_> {
        todo!()
    }
}

pub struct Iter {}

impl Iterator for Iter {
    type Item = Versioned;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
