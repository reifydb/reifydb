// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::sqlite::Sqlite;
use reifydb_core::EncodedKeyRange;
use reifydb_core::interface::{Unversioned, UnversionedScanRange};

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
