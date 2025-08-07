// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::lmdb::Lmdb;
use reifydb_core::interface::{
    Unversioned, UnversionedRangeRev as RangeRevInterface, Versioned, VersionedRangeRev,
};
use reifydb_core::{EncodedKeyRange, Result, Version};

impl VersionedRangeRev for Lmdb {
    type RangeIterRev<'a> = RangeRev;

    fn range_rev(
        &self,
        _range: EncodedKeyRange,
        _version: Version,
    ) -> Result<Self::RangeIterRev<'_>> {
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

impl RangeRevInterface for Lmdb {
    type RangeRev<'a>
        = UnversionedRangeRev
    where
        Self: 'a;

    fn range_rev(&self, _range: EncodedKeyRange) -> Result<Self::RangeRev<'_>> {
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
