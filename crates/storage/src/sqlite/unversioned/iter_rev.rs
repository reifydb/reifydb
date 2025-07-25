// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::sqlite::Sqlite;
use reifydb_core::Error;
use reifydb_core::interface::{Unversioned, UnversionedScanRev};

pub struct UnversionedIterRev {}

impl<'a> Iterator for UnversionedIterRev {
    type Item = Unversioned;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl UnversionedScanRev for Sqlite {
    type ScanIterRev<'a> = crate::sqlite::unversioned::iter::UnversionedIter;

    fn scan_rev(&self) -> Result<Self::ScanIterRev<'_>, Error> {
        todo!()
    }
}
