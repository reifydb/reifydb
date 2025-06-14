// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::bypass::BypassTx;
use crate::mvcc::conflict::BTreeConflict;
use crate::mvcc::transaction::TransactionValue;
use crate::mvcc::transaction::iter::TransactionIter;
use crate::mvcc::transaction::iter_rev::TransactionIterRev;
use crate::mvcc::transaction::range::TransactionRange;
use crate::mvcc::transaction::range_rev::TransactionRangeRev;
use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodedKey, EncodedKeyRange};
use reifydb_storage::{UnversionedStorage, VersionedStorage};

pub trait Tx<VS: VersionedStorage, US: UnversionedStorage> {
    fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<TransactionValue>>;

    fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool>;

    fn scan(&mut self) -> crate::Result<TransactionIter<'_, VS, BTreeConflict>>;

    fn scan_rev(&mut self) -> crate::Result<TransactionIterRev<'_, VS, BTreeConflict>>;

    fn scan_range(
        &mut self,
        range: EncodedKeyRange,
    ) -> crate::Result<TransactionRange<'_, VS, BTreeConflict>>;

    fn scan_range_rev(
        &mut self,
        range: EncodedKeyRange,
    ) -> crate::Result<TransactionRangeRev<'_, VS, BTreeConflict>>;

    fn scan_prefix(
        &mut self,
        prefix: &EncodedKey,
    ) -> crate::Result<TransactionRange<'_, VS, BTreeConflict>>;

    fn scan_prefix_rev(
        &mut self,
        prefix: &EncodedKey,
    ) -> crate::Result<TransactionRangeRev<'_, VS, BTreeConflict>>;

    fn set(&mut self, key: EncodedKey, row: EncodedRow) -> crate::Result<()>;

    fn remove(&mut self, key: EncodedKey) -> crate::Result<()>;

    fn commit(self) -> crate::Result<()>;

    fn rollback(self) -> crate::Result<()>;

    fn bypass(&mut self) -> BypassTx<US>;
}
