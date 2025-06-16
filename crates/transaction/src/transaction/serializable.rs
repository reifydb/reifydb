// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::bypass::BypassTx;
use crate::mvcc::conflict::BTreeConflict;
use crate::mvcc::transaction::TransactionValue;
use crate::mvcc::transaction::iter::TransactionIter;
use crate::mvcc::transaction::iter_rev::TransactionIterRev;
use crate::mvcc::transaction::range::TransactionRange;
use crate::mvcc::transaction::range_rev::TransactionRangeRev;
use crate::mvcc::transaction::serializable::{Serializable, TransactionRx, TransactionTx};
use crate::{Rx, Transaction, Tx};
use reifydb_core::hook::Hooks;
use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodedKey, EncodedKeyRange};
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use std::sync::MutexGuard;

impl<VS: VersionedStorage, US: UnversionedStorage> Transaction<VS, US> for Serializable<VS, US> {
    type Rx = TransactionRx<VS, US>;
    type Tx = TransactionTx<VS, US>;

    fn begin_read_only(&self) -> crate::Result<Self::Rx> {
        Ok(self.begin_read_only())
    }

    fn begin(&self) -> crate::Result<Self::Tx> {
        Ok(self.begin())
    }

    fn hooks(&self) -> Hooks {
        self.hooks.clone()
    }

    fn versioned(&self) -> VS {
        self.versioned.clone()
    }
}

impl<VS: VersionedStorage, US: UnversionedStorage> Rx<VS, US> for TransactionRx<VS, US> {
    fn get(&self, key: &EncodedKey) -> crate::Result<Option<TransactionValue>> {
        Ok(TransactionRx::get(self, key))
    }

    fn contains_key(&self, key: &EncodedKey) -> crate::Result<bool> {
        Ok(TransactionRx::contains_key(self, key))
    }

    fn scan(&self) -> crate::Result<VS::ScanIter<'_>> {
        Ok(TransactionRx::scan(self))
    }

    fn scan_rev(&self) -> crate::Result<VS::ScanIterRev<'_>> {
        Ok(TransactionRx::scan_rev(self))
    }

    fn scan_range(&self, range: EncodedKeyRange) -> crate::Result<VS::ScanRangeIter<'_>> {
        Ok(TransactionRx::scan_range(self, range))
    }

    fn scan_range_rev(&self, range: EncodedKeyRange) -> crate::Result<VS::ScanRangeIterRev<'_>> {
        Ok(TransactionRx::scan_range_rev(self, range))
    }

    fn scan_prefix(&self, prefix: &EncodedKey) -> crate::Result<VS::ScanRangeIter<'_>> {
        Ok(TransactionRx::scan_prefix(self, prefix))
    }

    fn scan_prefix_rev(&self, prefix: &EncodedKey) -> crate::Result<VS::ScanRangeIterRev<'_>> {
        Ok(TransactionRx::scan_prefix_rev(self, prefix))
    }
}

impl<VS: VersionedStorage, US: UnversionedStorage> Tx<VS, US> for TransactionTx<VS, US> {
    fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<TransactionValue>> {
        Ok(TransactionTx::get(self, key)?)
    }

    fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
        Ok(TransactionTx::contains_key(self, key)?)
    }

    fn scan(&mut self) -> crate::Result<TransactionIter<'_, VS, BTreeConflict>> {
        Ok(TransactionTx::scan(self)?)
    }

    fn scan_rev(&mut self) -> crate::Result<TransactionIterRev<'_, VS, BTreeConflict>> {
        Ok(TransactionTx::scan_rev(self)?)
    }

    fn scan_range(
        &mut self,
        range: EncodedKeyRange,
    ) -> crate::Result<TransactionRange<'_, VS, BTreeConflict>> {
        Ok(TransactionTx::scan_range(self, range)?)
    }

    fn scan_range_rev(
        &mut self,
        range: EncodedKeyRange,
    ) -> crate::Result<TransactionRangeRev<'_, VS, BTreeConflict>> {
        Ok(TransactionTx::scan_range_rev(self, range)?)
    }

    fn scan_prefix(
        &mut self,
        prefix: &EncodedKey,
    ) -> crate::Result<TransactionRange<'_, VS, BTreeConflict>> {
        Ok(TransactionTx::scan_prefix(self, prefix)?)
    }

    fn scan_prefix_rev(
        &mut self,
        prefix: &EncodedKey,
    ) -> crate::Result<TransactionRangeRev<'_, VS, BTreeConflict>> {
        Ok(TransactionTx::scan_prefix_rev(self, prefix)?)
    }

    fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> crate::Result<()> {
        TransactionTx::set(self, key, row)?;
        Ok(())
    }

    fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
        TransactionTx::remove(self, key)?;
        Ok(())
    }

    fn commit(mut self) -> crate::Result<()> {
        TransactionTx::commit(&mut self)?;
        Ok(())
    }

    fn rollback(mut self) -> crate::Result<()> {
        TransactionTx::rollback(&mut self)?;
        Ok(())
    }

    fn bypass<'a>(&'a mut self) -> MutexGuard<'a, BypassTx<US>> {
        TransactionTx::bypass(self)
    }
}
