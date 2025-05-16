// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Rx, Tx};

pub trait TransactionEngine<'a, S: storage::StorageEngine>: Send + Sync {
    type Rx: Rx + 'a;
    type Tx: Tx + 'a;

    /// Begins a read-only transaction.
    fn begin_read_only(&'a self) -> crate::Result<Self::Rx>;

    /// Begins a read-write transaction.
    fn begin(&'a self) -> crate::Result<Self::Tx>;
}
