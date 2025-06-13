// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::mvcc::transaction::TransactionValue;
use reifydb_core::{EncodedKey, EncodedKeyRange};
use reifydb_storage::Storage;

pub trait Rx<S: Storage> {
    fn get(&self, key: &EncodedKey) -> crate::Result<Option<TransactionValue>>;

    fn contains_key(&self, key: &EncodedKey) -> crate::Result<bool>;

    fn scan(&self) -> crate::Result<S::ScanIter<'_>>;

    fn scan_rev(&self) -> crate::Result<S::ScanIterRev<'_>>;

    fn scan_range(&self, range: EncodedKeyRange) -> crate::Result<S::ScanRangeIter<'_>>;

    fn scan_range_rev(&self, range: EncodedKeyRange) -> crate::Result<S::ScanRangeIterRev<'_>>;

    fn scan_prefix(&self, prefix: &EncodedKey) -> crate::Result<S::ScanRangeIter<'_>>;

    fn scan_prefix_rev(&self, prefix: &EncodedKey) -> crate::Result<S::ScanRangeIterRev<'_>>;
}
