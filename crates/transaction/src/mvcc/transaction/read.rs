// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::Version;
use crate::mvcc::transaction::*;

/// TransactionManagerRx is a read-only transaction manager.
pub struct TransactionManagerRx<C, P> {
    pub(crate) db: TransactionManager<C, P>,
    pub(crate) version: Version,
}

impl<C, P> TransactionManagerRx<C, P> {
    /// Returns the version of this read transaction.
    pub const fn version(&self) -> u64 {
        self.version
    }
}

impl<C, P> Drop for TransactionManagerRx<C, P> {
    fn drop(&mut self) {
        self.db.inner.done_read(self.version);
    }
}
