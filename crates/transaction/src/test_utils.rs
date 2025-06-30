// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::mvcc::transaction::optimistic::{Optimistic, TransactionTx};
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{BoxedVersionedIter, Rx, Transaction, Tx, Versioned};
use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodedKey, EncodedKeyRange, Error};
use reifydb_storage::memory::Memory;
use std::sync::RwLockWriteGuard;

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
        let engine = Optimistic::new(Memory::new(), unversioned.clone(), Hooks::default());
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
    fn get(&mut self, key: &EncodedKey) -> Result<Option<Versioned>, Error> {
        Ok(self.tx.get(key)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        }))
    }

    fn contains_key(&mut self, key: &EncodedKey) -> Result<bool, Error> {
        Ok(self.tx.contains_key(key)?)
    }

    fn scan(&mut self) -> Result<BoxedVersionedIter, Error> {
        let iter = self.tx.scan()?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_rev(&mut self) -> Result<BoxedVersionedIter, Error> {
        let iter = self.tx.scan_rev()?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_range(&mut self, range: EncodedKeyRange) -> Result<BoxedVersionedIter, Error> {
        let iter = self.tx.scan_range(range)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_range_rev(&mut self, range: EncodedKeyRange) -> Result<BoxedVersionedIter, Error> {
        let iter = self.tx.scan_range_rev(range)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_prefix(&mut self, prefix: &EncodedKey) -> Result<BoxedVersionedIter, Error> {
        let iter = self.tx.scan_prefix(prefix)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_prefix_rev(&mut self, prefix: &EncodedKey) -> Result<BoxedVersionedIter, Error> {
        let iter = self.tx.scan_prefix_rev(prefix)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }
}

impl Tx<Memory, Memory> for TestTransaction {
    fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<(), Error> {
        Ok(self.tx.set(key, row)?)
    }

    fn remove(&mut self, key: &EncodedKey) -> Result<(), Error> {
        Ok(self.tx.remove(key)?)
    }

    fn commit(self) -> Result<(), Error> {
        self.tx.commit()
    }

    fn rollback(self) -> Result<(), Error> {
        self.tx.rollback()
    }

    fn unversioned(&mut self) -> RwLockWriteGuard<Memory> {
        self.tx.unversioned()
    }
}
