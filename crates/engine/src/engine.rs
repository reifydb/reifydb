// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use std::marker::PhantomData;
use storage::StorageEngine;
use transaction::TransactionEngine;

pub struct Engine<S: StorageEngine, T: TransactionEngine<S>> {
    transaction: T,
    _marker: PhantomData<S>,
}

impl<S: StorageEngine, T: TransactionEngine<S>> Engine<S, T> {
    pub fn new(transaction: T) -> Self {
        Self { transaction, _marker: PhantomData }
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
