// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub struct Engine<'a, S: storage::EngineMut, T: transaction::Engine<'a, S>> {
    transaction: T,
    _marker: std::marker::PhantomData<(&'a (), S)>,
}

impl<'a, S: storage::EngineMut, T: transaction::Engine<'a, S>> Engine<'a, S, T> {
    pub fn new(transaction: T) -> Self {
        Self { transaction, _marker: Default::default() }
    }
}

impl<'a, S: storage::EngineMut, T: transaction::Engine<'a, S>> Engine<'a, S, T> {
    pub fn begin(&'a self) -> crate::Result<T::Tx> {
        Ok(self.transaction.begin().unwrap())
    }

    pub fn begin_read_only(&'a self) -> crate::Result<T::Rx> {
        Ok(self.transaction.begin_read_only().unwrap())
    }
}
