// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::svl::catalog::Catalog;
use crate::svl::transaction::{Transaction, TransactionMut};
pub use error::Error;
use std::sync::{Arc, RwLock};

mod catalog;
mod error;
mod schema;
mod store;
mod transaction;

pub struct Engine<S: storage::StorageEngineMut> {
    inner: Arc<RwLock<EngineInner<S>>>,
}

pub struct EngineInner<S: storage::StorageEngineMut> {
    pub storage: S,
    pub catalog: Catalog,
}

impl<S: storage::StorageEngineMut> Engine<S> {
    pub fn new(storage: S) -> Self {
        Self { inner: Arc::new(RwLock::new(EngineInner { storage, catalog: Catalog::new() })) }
    }
}

impl<'a, S: storage::StorageEngineMut + 'a> crate::TransactionEngine<'a, S> for Engine<S> {
    type Rx = Transaction<'a, S>;

    fn begin_read_only(&'a self) -> crate::Result<Self::Rx> {
        let guard = self.inner.read().unwrap();
        Ok(Transaction::new(guard))
    }
}

impl<'a, S: storage::StorageEngineMut + 'a> crate::TransactionEngineMut<'a, S> for Engine<S> {
    type Tx = TransactionMut<'a, S>;

    fn begin(&'a self) -> crate::Result<Self::Tx> {
        let guard = self.inner.write().unwrap();
        Ok(TransactionMut::new(guard))
    }
}
