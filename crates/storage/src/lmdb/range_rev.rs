// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::lmdb::Lmdb;
use crate::{VersionedScanRangeRev, Versioned};
use reifydb_core::{EncodedKeyRange, Version};

impl VersionedScanRangeRev for Lmdb {
    type ScanRangeIterRev<'a> = RangeRev;

    fn scan_range_rev(&self, range: EncodedKeyRange, version: Version) -> Self::ScanRangeIterRev<'_> {
        todo!()
    }
}

pub struct RangeRev {}

impl Iterator for RangeRev {
    type Item = Versioned;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
