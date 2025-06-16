// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodedKey, EncodedKeyRange};
use reifydb_storage::memory::Memory;
use reifydb_transaction::mvcc::conflict::BTreeConflict;
use reifydb_transaction::mvcc::transaction::TransactionValue;
use reifydb_transaction::mvcc::transaction::iter::TransactionIter;
use reifydb_transaction::mvcc::transaction::iter_rev::TransactionIterRev;
use reifydb_transaction::mvcc::transaction::optimistic::{Optimistic, TransactionTx};
use reifydb_transaction::mvcc::transaction::range::TransactionRange;
use reifydb_transaction::mvcc::transaction::range_rev::TransactionRangeRev;
use reifydb_transaction::{Transaction, Tx};
use std::sync::MutexGuard;

pub struct TestTransaction {
    engine: Optimistic<Memory, Memory>,
    tx: TransactionTx<Memory, Memory>,
}

impl TestTransaction {
    pub fn new() -> Self {
        let engine = Optimistic::new(Memory::new(), Memory::new());
        let tx = engine.begin();
        Self { engine, tx }
    }

    pub fn versioned(&self) -> Memory {
        self.engine.versioned()
    }
}

impl Tx<Memory, Memory> for TestTransaction {
    fn get(&mut self, key: &EncodedKey) -> reifydb_transaction::Result<Option<TransactionValue>> {
        Ok(self.tx.get(key)?)
    }

    fn contains_key(&mut self, key: &EncodedKey) -> reifydb_transaction::Result<bool> {
        Ok(self.tx.contains_key(key)?)
    }

    fn scan(&mut self) -> reifydb_transaction::Result<TransactionIter<'_, Memory, BTreeConflict>> {
        Ok(self.tx.scan()?)
    }

    fn scan_rev(
        &mut self,
    ) -> reifydb_transaction::Result<TransactionIterRev<'_, Memory, BTreeConflict>> {
        Ok(self.tx.scan_rev()?)
    }

    fn scan_range(
        &mut self,
        range: EncodedKeyRange,
    ) -> reifydb_transaction::Result<TransactionRange<'_, Memory, BTreeConflict>> {
        Ok(self.tx.scan_range(range)?)
    }

    fn scan_range_rev(
        &mut self,
        range: EncodedKeyRange,
    ) -> reifydb_transaction::Result<TransactionRangeRev<'_, Memory, BTreeConflict>> {
        Ok(self.tx.scan_range_rev(range)?)
    }

    fn scan_prefix(
        &mut self,
        prefix: &EncodedKey,
    ) -> reifydb_transaction::Result<TransactionRange<'_, Memory, BTreeConflict>> {
        Ok(self.tx.scan_prefix(prefix)?)
    }

    fn scan_prefix_rev(
        &mut self,
        prefix: &EncodedKey,
    ) -> reifydb_transaction::Result<TransactionRangeRev<'_, Memory, BTreeConflict>> {
        Ok(self.tx.scan_prefix_rev(prefix)?)
    }

    fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> reifydb_transaction::Result<()> {
        Ok(self.tx.set(key, row)?)
    }

    fn remove(&mut self, key: &EncodedKey) -> reifydb_transaction::Result<()> {
        Ok(self.tx.remove(key)?)
    }

    fn commit(self) -> reifydb_transaction::Result<()> {
        Ok(self.tx.commit()?)
    }

    fn rollback(self) -> reifydb_transaction::Result<()> {
        Ok(self.tx.rollback()?)
    }

    fn bypass(&mut self) -> MutexGuard<reifydb_transaction::BypassTx<Memory>> {
        self.tx.bypass()
    }
}
