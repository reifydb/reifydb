// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::mvcc::transaction::optimistic::{Optimistic, TransactionTx};
use crate::{BypassTx, Rx, Transaction, Tx, VersionedIter};
use reifydb_core::interface::Versioned;
use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodedKey, EncodedKeyRange};
use reifydb_storage::memory::Memory;
use std::sync::MutexGuard;

pub struct TestTransaction {
    engine: Optimistic<Memory, Memory>,
    tx: TransactionTx<Memory, Memory>,
    unversioned: Memory,
}

impl Default for TestTransaction {
    fn default() -> Self {
        Self::new()
    }
}

impl TestTransaction {
    pub fn new() -> Self {
        let unversioned = Memory::default();
        let engine = Optimistic::new(Memory::new(), unversioned.clone());
        let tx = engine.begin();
        Self { engine, tx, unversioned }
    }

    pub fn versioned(&self) -> Memory {
        self.engine.versioned()
    }

    pub fn unversioned(&self) -> Memory {
        self.unversioned.clone()
    }
}

impl Rx for TestTransaction {
    fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<Versioned>> {
        Ok(self.tx.get(key)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        }))
    }

    fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
        Ok(self.tx.contains_key(key)?)
    }

    fn scan(&mut self) -> crate::Result<VersionedIter> {
        let iter = self.tx.scan()?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_rev(&mut self) -> crate::Result<VersionedIter> {
        let iter = self.tx.scan_rev()?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_range(&mut self, range: EncodedKeyRange) -> crate::Result<VersionedIter> {
        let iter = self.tx.scan_range(range)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<VersionedIter> {
        let iter = self.tx.scan_range_rev(range)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_prefix(&mut self, prefix: &EncodedKey) -> crate::Result<VersionedIter> {
        let iter = self.tx.scan_prefix(prefix)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<VersionedIter> {
        let iter = self.tx.scan_prefix_rev(prefix)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }
}

impl Tx<Memory, Memory> for TestTransaction {
    fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> crate::Result<()> {
        Ok(self.tx.set(key, row)?)
    }

    fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
        Ok(self.tx.remove(key)?)
    }

    fn commit(self) -> crate::Result<()> {
        self.tx.commit()
    }

    fn rollback(self) -> crate::Result<()> {
        self.tx.rollback()
    }

    fn bypass(&mut self) -> MutexGuard<BypassTx<Memory>> {
        self.tx.bypass()
    }
}
