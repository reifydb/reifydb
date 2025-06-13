// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::mvcc::transaction::optimistic::{Optimistic, TransactionRx, TransactionTx};
use crate::{Rx, Transaction, Tx};
use reifydb_core::EncodedKey;
use reifydb_core::hook::Hooks;
use reifydb_core::row::EncodedRow;
use reifydb_storage::Storage;

impl<S: Storage> Transaction<S> for Optimistic<S> {
    type Rx = TransactionRx<S>;
    type Tx = TransactionTx<S>;

    fn begin_read_only(&self) -> crate::Result<Self::Rx> {
        Ok(self.begin_read_only())
    }

    fn begin(&self) -> crate::Result<Self::Tx> {
        Ok(self.begin())
    }

    fn hooks(&self) -> Hooks {
        self.hooks.clone()
    }

    fn storage(&self) -> S {
        self.storage.clone()
    }
}

impl<S: Storage> Rx for TransactionRx<S> {}

impl<S: Storage> Rx for TransactionTx<S> {}

impl<S: Storage> Tx for TransactionTx<S> {
    fn set(&mut self, key: EncodedKey, row: EncodedRow) -> crate::Result<()> {
        TransactionTx::set(self, key, row)?;
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
}
