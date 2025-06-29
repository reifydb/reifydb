// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::interface::{Unversioned, UnversionedScanRev};
use crate::sqlite::Sqlite;

pub struct UnversionedIterRev {}

impl<'a> Iterator for UnversionedIterRev {
    type Item = Unversioned;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl UnversionedScanRev for Sqlite {
    type ScanIterRev<'a> = crate::sqlite::iter::UnversionedIter;

    fn scan_rev_unversioned(&self) -> Self::ScanIterRev<'_> {
        todo!()
    }
}
