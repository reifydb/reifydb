// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::sqlite::Sqlite;
use reifydb_core::interface::{Unversioned, UnversionedScanRange};
use reifydb_core::{EncodedKeyRange, Error};

impl UnversionedScanRange for Sqlite {
    type ScanRange<'a>
        = UnversionedRange
    where
        Self: 'a;

    fn scan_range(&self, _range: EncodedKeyRange) -> Result<Self::ScanRange<'_>, Error> {
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
