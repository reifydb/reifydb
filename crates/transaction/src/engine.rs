// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Transaction, TransactionMut};

pub trait TransactionEngine<'a, S: storage::StorageEngine>: Sized {
    type Rx: Transaction;

    /// Begins a read-only transaction.
    fn begin_read_only(&'a self) -> crate::Result<Self::Rx>;
}

pub trait TransactionEngineMut<'a, S: storage::StorageEngine>: TransactionEngine<'a, S> {
    type Tx: TransactionMut;

    /// Begins a read-write transaction.
    fn begin(&'a self) -> crate::Result<Self::Tx>;
}
