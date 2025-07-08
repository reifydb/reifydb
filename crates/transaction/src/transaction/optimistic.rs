// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::mvcc::transaction::optimistic::{Optimistic, TransactionRx, TransactionTx};
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{
    BoxedVersionedIter, Rx, Transaction, Tx, UnversionedStorage, Versioned, VersionedStorage,
};
use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodedKey, EncodedKeyRange, Error};
use std::sync::{RwLockReadGuard, RwLockWriteGuard};

impl<VS: VersionedStorage, US: UnversionedStorage> Transaction<VS, US> for Optimistic<VS, US> {
    type Rx = TransactionRx<VS, US>;
    type Tx = TransactionTx<VS, US>;

    fn begin_rx(&self) -> Result<Self::Rx, Error> {
        Ok(self.begin_rx())
    }

    fn begin_tx(&self) -> Result<Self::Tx, Error> {
        Ok(self.begin_tx())
    }

    fn hooks(&self) -> Hooks<US> {
        self.hooks.clone()
    }

    fn versioned(&self) -> VS {
        self.versioned.clone()
    }

    fn begin_unversioned_rx(&self) -> RwLockReadGuard<'_, US> {
        self.unversioned.read().unwrap()
    }

    fn begin_unversioned_tx(&self) -> RwLockWriteGuard<'_, US> {
        self.unversioned.write().unwrap()
    }
}

impl<VS: VersionedStorage, US: UnversionedStorage> Rx for TransactionRx<VS, US> {
    fn get(&mut self, key: &EncodedKey) -> Result<Option<Versioned>, Error> {
        Ok(TransactionRx::get(self, key).map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        }))
    }

    fn contains_key(&mut self, key: &EncodedKey) -> Result<bool, Error> {
        Ok(TransactionRx::contains_key(self, key))
    }

    fn scan(&mut self) -> Result<BoxedVersionedIter, Error> {
        let iter = TransactionRx::scan(self);
        Ok(Box::new(iter.into_iter()))
    }

    fn scan_rev(&mut self) -> Result<BoxedVersionedIter, Error> {
        let iter = TransactionRx::scan_rev(self);
        Ok(Box::new(iter.into_iter()))
    }

    fn scan_range(&mut self, range: EncodedKeyRange) -> Result<BoxedVersionedIter, Error> {
        let iter = TransactionRx::scan_range(self, range);
        Ok(Box::new(iter.into_iter()))
    }

    fn scan_range_rev(&mut self, range: EncodedKeyRange) -> Result<BoxedVersionedIter, Error> {
        let iter = TransactionRx::scan_range_rev(self, range);
        Ok(Box::new(iter.into_iter()))
    }

    fn scan_prefix(&mut self, prefix: &EncodedKey) -> Result<BoxedVersionedIter, Error> {
        let iter = TransactionRx::scan_prefix(self, prefix);
        Ok(Box::new(iter.into_iter()))
    }

    fn scan_prefix_rev(&mut self, prefix: &EncodedKey) -> Result<BoxedVersionedIter, Error> {
        let iter = TransactionRx::scan_prefix_rev(self, prefix);
        Ok(Box::new(iter.into_iter()))
    }
}

impl<VS: VersionedStorage, US: UnversionedStorage> Rx for TransactionTx<VS, US> {
    fn get(&mut self, key: &EncodedKey) -> Result<Option<Versioned>, Error> {
        Ok(TransactionTx::get(self, key)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        }))
    }

    fn contains_key(&mut self, key: &EncodedKey) -> Result<bool, Error> {
        Ok(TransactionTx::contains_key(self, key)?)
    }

    fn scan(&mut self) -> Result<BoxedVersionedIter, Error> {
        let iter = self.scan()?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_rev(&mut self) -> Result<BoxedVersionedIter, Error> {
        let iter = self.scan_rev()?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_range(&mut self, range: EncodedKeyRange) -> Result<BoxedVersionedIter, Error> {
        let iter = self.scan_range(range)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_range_rev(&mut self, range: EncodedKeyRange) -> Result<BoxedVersionedIter, Error> {
        let iter = self.scan_range_rev(range)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_prefix(&mut self, prefix: &EncodedKey) -> Result<BoxedVersionedIter, Error> {
        let iter = self.scan_prefix(prefix)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_prefix_rev(&mut self, prefix: &EncodedKey) -> Result<BoxedVersionedIter, Error> {
        let iter = self.scan_prefix_rev(prefix)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }
}

impl<VS: VersionedStorage, US: UnversionedStorage> Tx<VS, US> for TransactionTx<VS, US> {
    fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<(), Error> {
        TransactionTx::set(self, key, row)?;
        Ok(())
    }

    fn remove(&mut self, key: &EncodedKey) -> Result<(), Error> {
        TransactionTx::remove(self, key)?;
        Ok(())
    }

    fn commit(mut self) -> Result<(), Error> {
        TransactionTx::commit(&mut self)?;
        Ok(())
    }

    fn rollback(mut self) -> Result<(), Error> {
        TransactionTx::rollback(&mut self)?;
        Ok(())
    }

    fn unversioned(&mut self) -> RwLockWriteGuard<'_, US> {
        TransactionTx::unversioned(self)
    }
}
