// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::transaction::svl::catalog::Catalog;
use crate::transaction::svl::lock::RwLock;
use crate::transaction::svl::transaction::{Transaction, TransactionMut};
pub use error::Error;

mod catalog;
mod error;
mod lock;
mod schema;
mod store;
mod transaction;

pub struct Svl<P: ::persistence::Persistence> {
    inner: RwLock<EngineInner<P>>,
}

pub struct EngineInner<P: ::persistence::Persistence> {
    pub store: P,
    pub catalog: Catalog,
}

impl<P: ::persistence::Persistence> Svl<P> {
    pub fn new(store: P) -> Self {
        Self { inner: RwLock::new(EngineInner { store, catalog: Catalog::new() }) }
    }
}

impl<P: persistence::Persistence> crate::Transaction<P> for Svl<P> {
    type Rx = Transaction<P>;
    type Tx = TransactionMut<P>;

    fn begin_read_only(&self) -> crate::Result<Self::Rx> {
        let guard = self.inner.read();
        Ok(Transaction::new(guard))
    }

    fn begin(&self) -> crate::Result<Self::Tx> {
        let guard = self.inner.write();
        Ok(TransactionMut::new(guard))
    }
}
