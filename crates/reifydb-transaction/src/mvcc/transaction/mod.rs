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

pub use command::*;
use oracle::*;
use reifydb_core::{Version, interface::TransactionId};
use version::VersionProvider;

pub use crate::mvcc::types::*;

pub mod iter;
pub mod iter_rev;

mod command;
pub mod optimistic;
mod oracle;
pub mod query;
pub mod range;
pub mod range_rev;
pub mod serializable;
mod version;

pub use oracle::MAX_COMMITTED_TXNS;

use crate::mvcc::{
	conflict::ConflictManager, pending::PendingWrites,
	transaction::query::TransactionManagerQuery,
};

pub struct TransactionManager<L>
where
	L: VersionProvider,
{
	inner: Arc<Oracle<L>>,
}

impl<L> Clone for TransactionManager<L>
where
	L: VersionProvider,
{
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
		}
	}
}

impl<L> TransactionManager<L>
where
	L: VersionProvider,
{
	pub fn write(
		&self,
	) -> Result<TransactionManagerCommand<L>, reifydb_type::Error> {
		Ok(TransactionManagerCommand {
			id: TransactionId::generate(),
			oracle: self.inner.clone(),
			version: self.inner.version()?,
			size: 0,
			count: 0,
			conflicts: ConflictManager::new(),
			pending_writes: PendingWrites::new(),
			duplicates: Vec::new(),
			discarded: false,
			done_query: false,
		})
	}
}

impl<L> TransactionManager<L>
where
	L: VersionProvider,
{
	pub fn new(clock: L) -> crate::Result<Self> {
		let version = clock.next()?;
		Ok(Self {
			inner: Arc::new({
				let oracle = Oracle::new(clock);
				oracle.query.done(version);
				oracle.command.done(version);
				oracle
			}),
		})
	}

	pub fn version(&self) -> crate::Result<Version> {
		self.inner.version()
	}
}

impl<L> TransactionManager<L>
where
	L: VersionProvider,
{
	pub fn discard_hint(&self) -> Version {
		self.inner.discard_at_or_below()
	}

	pub fn query(
		&self,
		version: Option<Version>,
	) -> crate::Result<TransactionManagerQuery<L>> {
		Ok(if let Some(version) = version {
			TransactionManagerQuery::new_time_travel(
				TransactionId::generate(),
				self.clone(),
				version,
			)
		} else {
			TransactionManagerQuery::new_current(
				TransactionId::generate(),
				self.clone(),
				self.inner.version()?,
			)
		})
	}
}
