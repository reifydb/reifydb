// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::sqlite::Sqlite;
use reifydb_core::interface::{Unversioned, UnversionedScanRangeRev};
use reifydb_core::{EncodedKeyRange, Error};

impl UnversionedScanRangeRev for Sqlite {
    type ScanRangeRev<'a>
        = UnversionedRangeRev
    where
        Self: 'a;

    fn scan_range_rev(&self, _range: EncodedKeyRange) -> Result<Self::ScanRangeRev<'_>, Error> {
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
