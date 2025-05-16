// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use storage::StorageEngine;
use transaction::TransactionEngine;

pub struct Engine<'a, S: StorageEngine, T: TransactionEngine<'a, S>> {
    transaction: T,
    _marker: std::marker::PhantomData<(&'a (), S)>,
}

impl<'a, S: StorageEngine, T: TransactionEngine<'a, S>> Engine<'a, S, T> {
    pub fn new(transaction: T) -> Self {
        Self { transaction, _marker: Default::default() }
    }
}

impl<'a, S: StorageEngine, T: TransactionEngine<'a, S>> Engine<'a, S, T> {
    pub fn begin(&'a self) -> crate::Result<T::Tx> {
        Ok(self.transaction.begin().unwrap())
    }

    pub fn begin_read_only(&'a self) -> crate::Result<T::Rx> {
        Ok(self.transaction.begin_read_only().unwrap())
    }
}
