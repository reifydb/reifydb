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
use reifydb_storage::Version;

pub struct TransactionManager<C, P> {
    inner: Arc<Oracle<C>>,
    _phantom: std::marker::PhantomData<(P)>,
}

impl<C, P> Clone for TransactionManager<C, P> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone(), _phantom: std::marker::PhantomData }
    }
}

impl<C, P> TransactionManager<C, P>
where
    C: Conflict,
    P: PendingWrites,
{
    pub fn write(&self) -> Result<TransactionManagerTx<C, P>, TransactionError> {
        let read_ts = self.inner.read_ts();
        Ok(TransactionManagerTx {
            oracle: self.inner.clone(),
            version: read_ts,
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

impl<C, P> TransactionManager<C, P> {
    pub fn new(name: &str, current_version: Version) -> Self {
        Self {
            inner: Arc::new({
                let next_ts = current_version;
                let orc = Oracle::new(
                    format!("{}.pending_reads", name).into(),
                    format!("{}.txn_timestamps", name).into(),
                    next_ts,
                );
                orc.rx.done(next_ts);
                orc.tx.done(next_ts);
                orc.increment_next_ts();
                orc
            }),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn version(&self) -> u64 {
        self.inner.read_ts()
    }
}

impl<C, P> TransactionManager<C, P> {
    pub fn discard_hint(&self) -> u64 {
        self.inner.discard_at_or_below()
    }

    pub fn read(&self) -> TransactionManagerRx<C, P> {
        TransactionManagerRx { db: self.clone(), version: self.inner.read_ts() }
    }
}
