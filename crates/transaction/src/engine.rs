// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Rx, Tx};

pub trait TransactionEngine<S: store::StoreEngine>: Send + Sync {
    type Rx: Rx;
    type Tx: Tx;

    /// Begins a read-only transaction.
    fn begin_read_only(&self) -> crate::Result<Self::Rx>;

    /// Begins a read-write transaction.
    fn begin(&self) -> crate::Result<Self::Tx>;
}
