// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::svl::catalog::Catalog;
use crate::svl::lock::RwLock;
use crate::svl::transaction::{Transaction, TransactionMut};
pub use error::Error;

mod catalog;
mod error;
mod lock;
mod schema;
mod store;
mod transaction;

pub struct Engine<S: ::store::StoreEngine> {
    inner: RwLock<EngineInner<S>>,
}

pub struct EngineInner<S: ::store::StoreEngine> {
    pub store: S,
    pub catalog: Catalog,
}

impl<S: ::store::StoreEngine> Engine<S> {
    pub fn new(store: S) -> Self {
        Self { inner: RwLock::new(EngineInner { store, catalog: Catalog::new() }) }
    }
}

impl<S: ::store::StoreEngine> crate::TransactionEngine<S> for Engine<S> {
    type Rx = Transaction<S>;
    type Tx = TransactionMut<S>;

    fn begin_read_only(&self) -> crate::Result<Self::Rx> {
        let guard = self.inner.read();
        Ok(Transaction::new(guard))
    }

    fn begin(&self) -> crate::Result<Self::Tx> {
        let guard = self.inner.write();
        Ok(TransactionMut::new(guard))
    }
}
