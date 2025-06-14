// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::mvcc::transaction::TransactionValue;
use reifydb_core::{EncodedKey, EncodedKeyRange};
use reifydb_storage::{UnversionedStorage, VersionedStorage};

pub trait Rx<VS: VersionedStorage, US: UnversionedStorage> {
    fn get(&self, key: &EncodedKey) -> crate::Result<Option<TransactionValue>>;

    fn contains_key(&self, key: &EncodedKey) -> crate::Result<bool>;

    fn scan(&self) -> crate::Result<VS::ScanIter<'_>>;

    fn scan_rev(&self) -> crate::Result<VS::ScanIterRev<'_>>;

    fn scan_range(&self, range: EncodedKeyRange) -> crate::Result<VS::ScanRangeIter<'_>>;

    fn scan_range_rev(&self, range: EncodedKeyRange) -> crate::Result<VS::ScanRangeIterRev<'_>>;

    fn scan_prefix(&self, prefix: &EncodedKey) -> crate::Result<VS::ScanRangeIter<'_>>;

    fn scan_prefix_rev(&self, prefix: &EncodedKey) -> crate::Result<VS::ScanRangeIterRev<'_>>;
}
