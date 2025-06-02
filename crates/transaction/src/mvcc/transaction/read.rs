// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::mvcc::transaction::*;
use reifydb_storage::Version;

/// TransactionManagerRx is a read-only transaction manager.
pub struct TransactionManagerRx<C, L, P>
where
    C: Conflict,
    L: LogicalClock,
    P: PendingWrites,
{
    pub(crate) engine: TransactionManager<C, L, P>,
    pub(crate) version: Version,
}

impl<C, L, P> TransactionManagerRx<C, L, P>
where
    C: Conflict,
    L: LogicalClock,
    P: PendingWrites,
{
    /// Returns the version of this read transaction.
    pub fn version(&self) -> Version {
        self.version
    }
}

impl<C, L, P> Drop for TransactionManagerRx<C, L, P>
where
    C: Conflict,
    L: LogicalClock,
    P: PendingWrites,
{
    fn drop(&mut self) {
        self.engine.inner.done_read(self.version);
    }
}
