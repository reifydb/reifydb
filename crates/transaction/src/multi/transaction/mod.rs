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
use std::{sync::Arc, time::Duration};

pub use command::*;
use oracle::*;
use reifydb_core::{CommitVersion, interface::TransactionId};
use tracing::instrument;
use version::VersionProvider;

pub use crate::multi::types::*;

mod command;
pub mod optimistic;
mod oracle;
pub mod query;
pub mod range;
pub mod range_rev;
pub mod serializable;
mod version;

pub use oracle::MAX_COMMITTED_TXNS;

use crate::multi::{
	AwaitWatermarkError, conflict::ConflictManager, pending::PendingWrites,
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
	#[instrument(level = "debug", skip(self))]
	pub fn write(&self) -> Result<TransactionManagerCommand<L>, reifydb_type::Error> {
		Ok(TransactionManagerCommand {
			id: TransactionId::generate(),
			oracle: self.inner.clone(),
			version: self.inner.version()?,
			read_version: None,
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
	#[instrument(level = "debug", skip(clock))]
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

	#[instrument(level = "trace", skip(self))]
	pub fn version(&self) -> crate::Result<CommitVersion> {
		self.inner.version()
	}
}

impl<L> TransactionManager<L>
where
	L: VersionProvider,
{
	#[instrument(level = "trace", skip(self))]
	pub fn discard_hint(&self) -> CommitVersion {
		self.inner.discard_at_or_below()
	}

	#[instrument(level = "debug", skip(self), fields(as_of_version = ?version))]
	pub fn query(&self, version: Option<CommitVersion>) -> crate::Result<TransactionManagerQuery<L>> {
		Ok(if let Some(version) = version {
			TransactionManagerQuery::new_time_travel(TransactionId::generate(), self.clone(), version)
		} else {
			TransactionManagerQuery::new_current(
				TransactionId::generate(),
				self.clone(),
				self.inner.version()?,
			)
		})
	}

	/// Wait for the command watermark to reach the specified version.
	/// Returns Ok(()) if the watermark reaches the version within the timeout,
	/// or Err(AwaitWatermarkError) if the timeout expires.
	///
	/// This is useful for CDC polling to ensure all in-flight commits have
	/// completed their storage writes before querying for CDC events.
	#[instrument(level = "debug", skip(self))]
	pub fn try_wait_for_watermark(
		&self,
		version: CommitVersion,
		timeout: Duration,
	) -> Result<(), AwaitWatermarkError> {
		if self.inner.command.wait_for_mark_timeout(version, timeout) {
			Ok(())
		} else {
			Err(AwaitWatermarkError {
				version,
				timeout,
			})
		}
	}

	/// Returns the highest version where ALL prior versions have completed.
	/// This is useful for CDC polling to know the safe upper bound for fetching
	/// CDC events - all events up to this version are guaranteed to be in storage.
	#[instrument(level = "trace", skip(self))]
	pub fn done_until(&self) -> CommitVersion {
		self.inner.command.done_until()
	}

	/// Returns (query_done_until, command_done_until) for debugging watermark state.
	pub fn watermarks(&self) -> (CommitVersion, CommitVersion) {
		(self.inner.query.done_until(), self.inner.command.done_until())
	}
}
