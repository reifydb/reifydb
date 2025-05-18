// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use storage::StorageEngine;
use transaction::TransactionEngine;

pub struct Engine<S: StorageEngine, T: TransactionEngine<S>>(Arc<EngineInner<S, T>>);

impl<S, T> Clone for Engine<S, T>
where
    S: StorageEngine,
    T: TransactionEngine<S>,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<S: StorageEngine, T: TransactionEngine<S>> Deref for Engine<S, T> {
    type Target = EngineInner<S, T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct EngineInner<S: StorageEngine, T: TransactionEngine<S>> {
    transaction: T,
    _marker: PhantomData<S>,
}

impl<S: StorageEngine, T: TransactionEngine<S>> Engine<S, T> {
    pub fn new(transaction: T) -> Self {
        Self(Arc::new(EngineInner { transaction, _marker: PhantomData }))
    }
}

impl<S: StorageEngine, T: TransactionEngine<S>> Engine<S, T> {
    pub fn begin(&self) -> crate::Result<T::Tx> {
        Ok(self.transaction.begin().unwrap())
    }

    pub fn begin_read_only(&self) -> crate::Result<T::Rx> {
        Ok(self.transaction.begin_read_only().unwrap())
    }
}
