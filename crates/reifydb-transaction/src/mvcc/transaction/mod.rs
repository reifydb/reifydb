// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

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
pub use command::*;

use oracle::*;
use reifydb_core::Version;
use version::VersionProvider;

pub mod iter;
pub mod iter_rev;

pub mod optimistic;
mod oracle;
pub mod range;
pub mod range_rev;
pub mod query;
pub mod serializable;
mod version;
mod command;

use crate::mvcc::conflict::ConflictManager;
use crate::mvcc::pending::PendingWrites;
use crate::mvcc::transaction::query::TransactionManagerQuery;
pub use oracle::MAX_COMMITTED_TXNS;

pub struct TransactionManager<L, P>
where
    L: VersionProvider,
    P: PendingWrites,
{
    inner: Arc<Oracle<L>>,
    _phantom: std::marker::PhantomData<P>,
}

impl<L, P> Clone for TransactionManager<L, P>
where
    L: VersionProvider,
    P: PendingWrites,
{
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone(), _phantom: std::marker::PhantomData }
    }
}

impl<L, P> TransactionManager<L, P>
where
    L: VersionProvider,
    P: PendingWrites,
{
    pub fn write(&self) -> Result<TransactionManagerCommand<L, P>, reifydb_core::Error> {
        Ok(TransactionManagerCommand {
            oracle: self.inner.clone(),
            version: self.inner.version()?,
            size: 0,
            count: 0,
            conflicts: ConflictManager::new(),
            pending_writes: P::new(),
            duplicates: Vec::new(),
            discarded: false,
            done_query: false,
        })
    }
}

impl<L, P> TransactionManager<L, P>
where
    L: VersionProvider,
    P: PendingWrites,
{
    pub fn new(name: &str, clock: L) -> crate::Result<Self> {
        let version = clock.next()?;
        Ok(Self {
            inner: Arc::new({
                let oracle = Oracle::new(
                    format!("{}.pending_reads", name).into(),
                    format!("{}.txn_timestamps", name).into(),
                    clock,
                );
                oracle.query.done(version);
                oracle.command.done(version);
                oracle
            }),
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn version(&self) -> crate::Result<Version> {
        self.inner.version()
    }
}

impl<L, P> TransactionManager<L, P>
where
    L: VersionProvider,
    P: PendingWrites,
{
    pub fn discard_hint(&self) -> Version {
        self.inner.discard_at_or_below()
    }

    pub fn query(&self, version: Option<Version>) -> crate::Result<TransactionManagerQuery<L, P>> {
        Ok(if let Some(version) = version {
            TransactionManagerQuery::new_time_travel(self.clone(), version)
        } else {
            TransactionManagerQuery::new_current(self.clone(), self.inner.version()?)
        })
    }
}
