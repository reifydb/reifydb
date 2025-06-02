// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::lmdb::Lmdb;
use crate::{ScanRev, StoredValue, Version};

impl ScanRev for Lmdb {
    type ScanIterRev<'a> = IterRev;

    fn scan_rev(&self, version: Version) -> Self::ScanIterRev<'_> {
        todo!()
    }
}

pub struct IterRev {}

impl Iterator for IterRev {
    type Item = StoredValue;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
