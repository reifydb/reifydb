// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::catalog_init;
use crate::transaction::svl::lock::RwLock;
use crate::transaction::svl::transaction::{Transaction, TransactionMut};
pub use error::Error;

mod error;
mod lock;
mod transaction;

pub struct Svl<P: ::reifydb_persistence::Persistence> {
    inner: RwLock<SvlInner<P>>,
}

pub struct SvlInner<P: ::reifydb_persistence::Persistence> {
    pub persistence: P,
}

impl<P: ::reifydb_persistence::Persistence> Svl<P> {
    pub fn new(persistence: P) -> Self {
        catalog_init();
        Self { inner: RwLock::new(SvlInner { persistence }) }
    }
}

impl<P: reifydb_persistence::Persistence> crate::Transaction<P> for Svl<P> {
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
