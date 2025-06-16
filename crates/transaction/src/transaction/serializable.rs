// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::bypass::BypassTx;
use crate::mvcc::transaction::serializable::{Serializable, TransactionRx, TransactionTx};
use crate::{Rx, Transaction, Tx, VersionedIter};
use reifydb_core::hook::Hooks;
use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodedKey, EncodedKeyRange};
use reifydb_storage::{UnversionedStorage, Versioned, VersionedStorage};
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

impl<VS: VersionedStorage, US: UnversionedStorage> Rx for TransactionRx<VS, US> {
    fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<Versioned>> {
        Ok(TransactionRx::get(self, key).map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        }))
    }

    fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
        Ok(TransactionRx::contains_key(self, key))
    }

    fn scan(&mut self) -> crate::Result<VersionedIter> {
        let iter = self.scan()?;
        Ok(Box::new(iter))
    }

    fn scan_rev(&mut self) -> crate::Result<VersionedIter> {
        let iter = self.scan_rev()?;
        Ok(Box::new(iter))
    }

    fn scan_range(&mut self, range: EncodedKeyRange) -> crate::Result<VersionedIter> {
        let iter = self.scan_range(range)?;
        Ok(Box::new(iter))
    }

    fn scan_range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<VersionedIter> {
        let iter = self.scan_range_rev(range)?;
        Ok(Box::new(iter))
    }

    fn scan_prefix(&mut self, prefix: &EncodedKey) -> crate::Result<VersionedIter> {
        let iter = self.scan_prefix(prefix)?;
        Ok(Box::new(iter))
    }

    fn scan_prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<VersionedIter> {
        let iter = self.scan_prefix_rev(prefix)?;
        Ok(Box::new(iter))
    }
}

impl<VS: VersionedStorage, US: UnversionedStorage> Rx for TransactionTx<VS, US> {
    fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<Versioned>> {
        Ok(TransactionTx::get(self, key)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        }))
    }

    fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
        Ok(TransactionTx::contains_key(self, key)?)
    }

    fn scan(&mut self) -> crate::Result<VersionedIter> {
        let iter = self.scan()?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_rev(&mut self) -> crate::Result<VersionedIter> {
        let iter = self.scan_rev()?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_range(&mut self, range: EncodedKeyRange) -> crate::Result<VersionedIter> {
        let iter = self.scan_range(range)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<VersionedIter> {
        let iter = self.scan_range_rev(range)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_prefix(&mut self, prefix: &EncodedKey) -> crate::Result<VersionedIter> {
        let iter = self.scan_prefix(prefix)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }

    fn scan_prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<VersionedIter> {
        let iter = self.scan_prefix_rev(prefix)?.map(|tv| Versioned {
            key: tv.key().clone(),
            row: tv.row().clone(),
            version: tv.version(),
        });

        Ok(Box::new(iter))
    }
}

impl<VS: VersionedStorage, US: UnversionedStorage> Tx<VS, US> for TransactionTx<VS, US> {
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
