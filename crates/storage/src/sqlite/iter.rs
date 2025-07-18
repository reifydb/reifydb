// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::sqlite::Sqlite;
use reifydb_core::Error;
use reifydb_core::interface::{Unversioned, UnversionedScan};

pub struct UnversionedIter {}

impl<'a> Iterator for UnversionedIter {
    type Item = Unversioned;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl UnversionedScan for Sqlite {
    type ScanIter<'a> = UnversionedIter;

    fn scan(&self) -> Result<Self::ScanIter<'_>, Error> {
        todo!()
    }
}
