// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use crate::mvcc::conflict::BTreeConflict;
use crate::mvcc::pending::BTreePendingWrites;
use crate::mvcc::skipdbcore::{AsSkipCore, SkipCore};
use crate::mvcc::transaction::TransactionManager;

pub use read::TransactionRx;
pub use write::TransactionTx;

mod read;
mod write;

struct Inner {
    tm: TransactionManager<BTreeConflict, BTreePendingWrites>,
    mem_table: SkipCore,
}

impl Inner {
    fn new(name: &str) -> Self {
        let tm = TransactionManager::new(name, 0);
        Self { tm, mem_table: SkipCore::new() }
    }

    fn version(&self) -> u64 {
        self.tm.version()
    }
}

pub struct Optimistic {
    inner: Arc<Inner>,
}

#[doc(hidden)]
impl AsSkipCore for Optimistic {
    #[allow(private_interfaces)]
    fn as_inner(&self) -> &SkipCore {
        &self.inner.mem_table
    }
}

impl Clone for Optimistic {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl Default for Optimistic {
    fn default() -> Self {
        Self::new()
    }
}

impl Optimistic {
    pub fn new() -> Self {
        let inner = Arc::new(Inner::new(core::any::type_name::<Self>()));
        Self { inner }
    }
}

impl Optimistic {
    /// Returns the current read version of the database.
    pub fn version(&self) -> u64 {
        self.inner.version()
    }

    /// Create a read transaction.
    pub fn begin_read_only(&self) -> TransactionRx {
        TransactionRx::new(self.clone())
    }
}

impl Optimistic {
    pub fn begin(&self) -> TransactionTx {
        TransactionTx::new(self.clone())
    }
}

pub enum Transaction {
    Rx(TransactionRx),
    Tx(TransactionTx),
}
