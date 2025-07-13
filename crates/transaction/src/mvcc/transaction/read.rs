// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::mvcc::transaction::*;
use reifydb_core::Version;
use reifydb_core::clock::LogicalClock;

pub enum TransactionKind {
    Current(Version),
    TimeTravel(Version),
}

/// TransactionManagerRx is a read-only transaction manager.
pub struct TransactionManagerRx<C, L, P>
where
    C: Conflict,
    L: LogicalClock,
    P: PendingWrites,
{
    engine: TransactionManager<C, L, P>,
    transaction: TransactionKind,
}

impl<C, L, P> TransactionManagerRx<C, L, P>
where
    C: Conflict,
    L: LogicalClock,
    P: PendingWrites,
{
    pub fn new_current(engine: TransactionManager<C, L, P>, version: Version) -> Self {
        Self { engine, transaction: TransactionKind::Current(version) }
    }

    pub fn new_time_travel(engine: TransactionManager<C, L, P>, version: Version) -> Self {
        Self { engine, transaction: TransactionKind::TimeTravel(version) }
    }

    pub fn version(&self) -> Version {
        match self.transaction {
            TransactionKind::Current(version) => version,
            TransactionKind::TimeTravel(version) => version,
        }
    }
}

impl<C, L, P> Drop for TransactionManagerRx<C, L, P>
where
    C: Conflict,
    L: LogicalClock,
    P: PendingWrites,
{
    fn drop(&mut self) {
        // time travel transaction have no effect on mvcc
        if let TransactionKind::Current(version) = self.transaction {
            self.engine.inner.done_read(version);
        }
    }
}
