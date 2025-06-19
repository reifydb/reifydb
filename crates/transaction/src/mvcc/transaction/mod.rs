// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use core::mem;
use std::sync::Arc;

pub use crate::mvcc::types::*;
pub use write::*;

use oracle::*;
use reifydb_core::Version;
use reifydb_core::clock::LogicalClock;

pub mod iter;
pub mod iter_rev;

pub mod optimistic;
mod oracle;
pub mod range;
pub mod range_rev;
pub mod read;
pub mod serializable;
mod write;

use crate::mvcc::conflict::Conflict;
use crate::mvcc::error::TransactionError;
use crate::mvcc::pending::PendingWrites;
use crate::mvcc::transaction::read::TransactionManagerRx;

pub struct TransactionManager<C, L, P>
where
    C: Conflict,
    L: LogicalClock,
    P: PendingWrites,
{
    inner: Arc<Oracle<C, L>>,
    _phantom: std::marker::PhantomData<P>,
}

impl<C, L, P> Clone for TransactionManager<C, L, P>
where
    C: Conflict,
    L: LogicalClock,
    P: PendingWrites,
{
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone(), _phantom: std::marker::PhantomData }
    }
}

impl<C, L, P> TransactionManager<C, L, P>
where
    C: Conflict,
    L: LogicalClock,
    P: PendingWrites,
{
    pub fn write(&self) -> Result<TransactionManagerTx<C, L, P>, TransactionError> {
        Ok(TransactionManagerTx {
            oracle: self.inner.clone(),
            version: self.inner.version(),
            size: 0,
            count: 0,
            conflicts: C::new(),
            pending_writes: P::new(),
            duplicates: Vec::new(),
            discarded: false,
            done_read: false,
        })
    }
}

impl<C, L, P> TransactionManager<C, L, P>
where
    C: Conflict,
    L: LogicalClock,
    P: PendingWrites,
{
    pub fn new(name: &str, clock: L) -> Self {
        let version = clock.next();
        Self {
            inner: Arc::new({
                let oracle = Oracle::new(
                    format!("{}.pending_reads", name).into(),
                    format!("{}.txn_timestamps", name).into(),
                    clock,
                );
                oracle.rx.done(version);
                oracle.tx.done(version);
                oracle
            }),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn version(&self) -> Version {
        self.inner.version()
    }
}

impl<C, L, P> TransactionManager<C, L, P>
where
    C: Conflict,
    L: LogicalClock,
    P: PendingWrites,
{
    pub fn discard_hint(&self) -> Version {
        self.inner.discard_at_or_below()
    }

    pub fn read(&self, version: Option<Version>) -> TransactionManagerRx<C, L, P> {
        if let Some(version) = version {
            TransactionManagerRx::new_time_travel(self.clone(), version)
        } else {
            TransactionManagerRx::new_current(self.clone(), self.inner.version())
        }
    }
}
