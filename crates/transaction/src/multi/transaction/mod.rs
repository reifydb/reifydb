// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use core::mem;
use std::{ops::Deref, sync::Arc, time::Duration};

use reifydb_core::{CommitVersion, EncodedKey, EncodedKeyRange, event::EventBus};
use reifydb_store_transaction::{
	MultiVersionBatch, MultiVersionContains, MultiVersionGet, MultiVersionRange, MultiVersionRangeRev,
	TransactionStore,
};
use reifydb_type::util::hex;
use tracing::instrument;
use version::{StandardVersionProvider, VersionProvider};

pub use crate::multi::types::*;
use crate::{
	TransactionId,
	multi::oracle::*,
	single::{TransactionSingle, TransactionSvl},
};

mod command;
pub mod manager;
mod query;
pub(crate) mod version;

pub use command::CommandTransaction;
pub use query::QueryTransaction;

pub use crate::multi::oracle::MAX_COMMITTED_TXNS;
use crate::multi::{
	AwaitWatermarkError,
	conflict::ConflictManager,
	pending::PendingWrites,
	transaction::manager::{TransactionManagerCommand, TransactionManagerQuery},
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
	#[instrument(name = "transaction::manager::write", level = "debug", skip(self))]
	pub async fn write(&self) -> Result<TransactionManagerCommand<L>, reifydb_type::Error> {
		Ok(TransactionManagerCommand {
			id: TransactionId::generate(),
			oracle: self.inner.clone(),
			version: self.inner.version().await?,
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
	#[instrument(name = "transaction::manager::new", level = "debug", skip(clock))]
	pub async fn new(clock: L) -> crate::Result<Self> {
		let version = clock.next().await?;
		let oracle = Oracle::new(clock).await;
		oracle.query.done(version);
		oracle.command.done(version);
		Ok(Self {
			inner: Arc::new(oracle),
		})
	}

	#[instrument(name = "transaction::manager::version", level = "trace", skip(self))]
	pub async fn version(&self) -> crate::Result<CommitVersion> {
		self.inner.version().await
	}
}

impl<L> TransactionManager<L>
where
	L: VersionProvider,
{
	#[instrument(name = "transaction::manager::discard_hint", level = "trace", skip(self))]
	pub fn discard_hint(&self) -> CommitVersion {
		self.inner.discard_at_or_below()
	}

	#[instrument(name = "transaction::manager::query", level = "debug", skip(self), fields(as_of_version = ?version))]
	pub async fn query(&self, version: Option<CommitVersion>) -> crate::Result<TransactionManagerQuery<L>> {
		Ok(if let Some(version) = version {
			TransactionManagerQuery::new_time_travel(TransactionId::generate(), self.clone(), version)
		} else {
			TransactionManagerQuery::new_current(
				TransactionId::generate(),
				self.clone(),
				self.inner.version().await?,
			)
		})
	}

	/// Wait for the command watermark to reach the specified version.
	/// Returns Ok(()) if the watermark reaches the version within the timeout,
	/// or Err(AwaitWatermarkError) if the timeout expires.
	///
	/// This is useful for CDC polling to ensure all in-flight commits have
	/// completed their storage writes before querying for CDC events.
	#[instrument(name = "transaction::manager::wait_for_watermark", level = "debug", skip(self))]
	pub async fn try_wait_for_watermark(
		&self,
		version: CommitVersion,
		timeout: Duration,
	) -> Result<(), AwaitWatermarkError> {
		if self.inner.command.wait_for_mark_timeout(version, timeout).await {
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
	#[instrument(name = "transaction::manager::done_until", level = "trace", skip(self))]
	pub fn done_until(&self) -> CommitVersion {
		self.inner.command.done_until()
	}

	/// Returns (query_done_until, command_done_until) for debugging watermark state.
	pub fn watermarks(&self) -> (CommitVersion, CommitVersion) {
		(self.inner.query.done_until(), self.inner.command.done_until())
	}
}

// ============================================================================
// Transaction - The main multi-version transaction type
// ============================================================================

pub struct TransactionMulti(Arc<Inner>);

pub struct Inner {
	pub(crate) tm: TransactionManager<StandardVersionProvider>,
	pub(crate) store: TransactionStore,
	pub(crate) event_bus: EventBus,
}

impl Deref for TransactionMulti {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Clone for TransactionMulti {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl Inner {
	async fn new(store: TransactionStore, single: TransactionSingle, event_bus: EventBus) -> crate::Result<Self> {
		let version_provider = StandardVersionProvider::new(single).await?;
		let tm = TransactionManager::new(version_provider).await?;

		Ok(Self {
			tm,
			store,
			event_bus,
		})
	}

	async fn version(&self) -> crate::Result<CommitVersion> {
		self.tm.version().await
	}
}

impl TransactionMulti {
	pub async fn testing() -> Self {
		let store = TransactionStore::testing_memory().await;
		let event_bus = EventBus::new();
		Self::new(
			store.clone(),
			TransactionSingle::SingleVersionLock(TransactionSvl::new(store, event_bus.clone())),
			event_bus,
		)
		.await
		.unwrap()
	}
}

impl TransactionMulti {
	#[instrument(name = "transaction::new", level = "debug", skip(store, single, event_bus))]
	pub async fn new(
		store: TransactionStore,
		single: TransactionSingle,
		event_bus: EventBus,
	) -> crate::Result<Self> {
		Ok(Self(Arc::new(Inner::new(store, single, event_bus).await?)))
	}
}

impl TransactionMulti {
	#[instrument(name = "transaction::version", level = "trace", skip(self))]
	pub async fn version(&self) -> crate::Result<CommitVersion> {
		self.0.version().await
	}

	#[instrument(name = "transaction::begin_query", level = "debug", skip(self))]
	pub async fn begin_query(&self) -> crate::Result<QueryTransaction> {
		QueryTransaction::new(self.clone(), None).await
	}

	/// Begin a query transaction at a specific version.
	///
	/// This is used for parallel query execution where multiple tasks need to
	/// read from the same snapshot (same CommitVersion) for consistency.
	#[instrument(name = "transaction::begin_query_at_version", level = "debug", skip(self), fields(version = %version.0))]
	pub async fn begin_query_at_version(&self, version: CommitVersion) -> crate::Result<QueryTransaction> {
		QueryTransaction::new(self.clone(), Some(version)).await
	}
}

impl TransactionMulti {
	#[instrument(name = "transaction::begin_command", level = "debug", skip(self))]
	pub async fn begin_command(&self) -> crate::Result<CommandTransaction> {
		CommandTransaction::new(self.clone()).await
	}
}

pub enum TransactionType {
	Query(QueryTransaction),
	Command(CommandTransaction),
}

impl TransactionMulti {
	#[instrument(name = "transaction::get", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref()), version = version.0))]
	pub async fn get(
		&self,
		key: &EncodedKey,
		version: CommitVersion,
	) -> Result<Option<Committed>, reifydb_type::Error> {
		Ok(MultiVersionGet::get(&self.store, key, version).await?.map(|sv| sv.into()))
	}

	#[instrument(name = "transaction::contains_key", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref()), version = version.0))]
	pub async fn contains_key(
		&self,
		key: &EncodedKey,
		version: CommitVersion,
	) -> Result<bool, reifydb_type::Error> {
		MultiVersionContains::contains(&self.store, key, version).await
	}

	#[instrument(name = "transaction::range_batch", level = "trace", skip(self), fields(version = version.0, batch_size = batch_size))]
	pub async fn range_batch(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: u64,
	) -> reifydb_type::Result<MultiVersionBatch> {
		MultiVersionRange::range_batch(&self.store, range, version, batch_size).await
	}

	pub async fn range(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
	) -> reifydb_type::Result<MultiVersionBatch> {
		self.range_batch(range, version, 1024).await
	}

	pub async fn range_rev_batch(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: u64,
	) -> reifydb_type::Result<MultiVersionBatch> {
		MultiVersionRangeRev::range_rev_batch(&self.store, range, version, batch_size).await
	}

	pub async fn range_rev(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
	) -> reifydb_type::Result<MultiVersionBatch> {
		self.range_rev_batch(range, version, 1024).await
	}

	/// Get a reference to the underlying transaction store.
	pub fn store(&self) -> &TransactionStore {
		&self.store
	}
}
