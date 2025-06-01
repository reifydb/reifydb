// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::transaction::svl::lock::RwLock;
use crate::transaction::svl::transaction::{TransactionRx, TransactionTx};
pub use error::Error;
use reifydb_storage::Storage;
use std::ops::Deref;

mod error;
mod lock;
mod transaction;

pub struct Svl<S: Storage> {
    inner: RwLock<SvlInner<S>>,
}

pub struct SvlInner<S: Storage> {
    pub storage: S,
}

impl<S: Storage> Svl<S> {
    pub fn new(storage: S) -> Self {
        Self { inner: RwLock::new(SvlInner { storage }) }
    }
}

impl<S: Storage> crate::Transaction<S> for Svl<S> {
    type Rx = TransactionRx<S>;
    type Tx = TransactionTx<S>;

    fn begin_read_only(&self) -> crate::Result<Self::Rx> {
        let guard = self.inner.read();
        Ok(TransactionRx::new(guard))
    }

    fn begin(&self) -> crate::Result<Self::Tx> {
        let guard = self.inner.write();
        Ok(TransactionTx::new(guard))
    }
}
