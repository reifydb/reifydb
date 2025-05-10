// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::svl::transaction::{Transaction, TransactionMut};
use std::sync::{Arc, RwLock};

mod catalog;
mod schema;
mod transaction;

pub struct Engine {
    inner: Arc<RwLock<EngineInner>>,
}

pub struct EngineInner {}

impl Engine {
    pub fn new() -> Self {
        Self { inner: Arc::new(RwLock::new(EngineInner {})) }
    }
}

impl<'a> crate::Engine<'a> for Engine {
    type Rx = Transaction<'a>;
    type Tx = TransactionMut<'a>;

    fn begin(&'a self) -> crate::Result<Self::Tx> {
        let guard = self.inner.write().unwrap();
        Ok(TransactionMut::new(guard))
    }

    fn begin_read_only(&'a self) -> crate::Result<Self::Rx> {
        let guard = self.inner.read().unwrap();
        Ok(Transaction::new(guard))
    }
}
