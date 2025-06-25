// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::Unversioned;
use crate::sqlite::Sqlite;
use crate::unversioned::UnversionedScanRange;
use reifydb_core::EncodedKeyRange;

impl UnversionedScanRange for Sqlite {
    type ScanRange<'a>
        = UnversionedRange
    where
        Self: 'a;

    fn scan_range_unversioned(&self, _range: EncodedKeyRange) -> Self::ScanRange<'_> {
        todo!()
    }
}

pub struct UnversionedRange {}

impl Iterator for UnversionedRange {
    type Item = Unversioned;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
