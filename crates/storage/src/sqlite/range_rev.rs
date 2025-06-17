// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Unversioned;
use crate::sqlite::Sqlite;
use crate::unversioned::UnversionedScanRangeRev;
use reifydb_core::EncodedKeyRange;

impl UnversionedScanRangeRev for Sqlite {
    type ScanRangeRev<'a>
        = UnversionedRangeRev
    where
        Self: 'a;

    fn scan_range_rev_unversioned(&self, range: EncodedKeyRange) -> Self::ScanRangeRev<'_> {
        todo!()
    }
}

pub struct UnversionedRangeRev {}

impl Iterator for UnversionedRangeRev {
    type Item = Unversioned;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
