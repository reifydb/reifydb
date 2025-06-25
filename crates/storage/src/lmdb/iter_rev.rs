// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::lmdb::Lmdb;
use crate::unversioned::UnversionedScanRev;
use crate::{Unversioned, Versioned, VersionedScanRev};
use reifydb_core::Version;

impl VersionedScanRev for Lmdb {
    type ScanIterRev<'a> = IterRev;

    fn scan_rev(&self, _version: Version) -> Self::ScanIterRev<'_> {
        todo!()
    }
}

pub struct IterRev {}

impl Iterator for IterRev {
    type Item = Versioned;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

pub struct UnversionedIterRev {}

impl<'a> Iterator for UnversionedIterRev {
    type Item = Unversioned;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl UnversionedScanRev for Lmdb {
    type ScanIterRev<'a> = UnversionedIterRev;

    fn scan_rev_unversioned(&self) -> Self::ScanIterRev<'_> {
        todo!()
    }
}
