// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{ops::Deref, sync::Arc, time::Duration};

use reifydb_core::{
	common::CommitVersion,
	encoded::key::EncodedKey,
	event::EventBus,
	interface::store::{MultiVersionContains, MultiVersionGet},
};
use reifydb_runtime::actor::system::ActorSystem;
use reifydb_store_multi::MultiStore;
use reifydb_type::{Result, util::hex};
use tracing::instrument;
use version::{StandardVersionProvider, VersionProvider};

use crate::{
	TransactionId,
	multi::{oracle::*, types::*},
	single::TransactionSingle,
};

pub mod command;
pub mod manager;
pub mod query;
pub(crate) mod version;

use crate::{
	multi::{
		CommandTransaction, QueryTransaction,
		conflict::ConflictManager,
		pending::PendingWrites,
		transaction::manager::{TransactionManagerCommand, TransactionManagerQuery},
	},
	single::svl::TransactionSvl,
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
	pub fn write(&self) -> Result<TransactionManagerCommand<L>> {
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
	#[instrument(name = "transaction::manager::new", level = "debug", skip(clock, actor_system))]
	pub fn new(clock: L, actor_system: ActorSystem) -> Result<Self> {
		let version = clock.next()?;
		let oracle = Oracle::new(clock, actor_system);
		oracle.query.done(version);
		oracle.command.done(version);
		Ok(Self {
			inner: Arc::new(oracle),
		})
	}

	/// Get the actor system
	pub fn actor_system(&self) -> ActorSystem {
		self.inner.actor_system()
	}

	#[instrument(name = "transaction::manager::version", level = "trace", skip(self))]
	pub fn version(&self) -> Result<CommitVersion> {
		self.inner.version()
	}
}

impl<L> TransactionManager<L>
where
	L: VersionProvider,
{
	#[instrument(name = "transaction::manager::query", level = "debug", skip(self), fields(as_of_version = ?version))]
	pub fn query(&self, version: Option<CommitVersion>) -> Result<TransactionManagerQuery<L>> {
		let safe_version = self.inner.version()?;

		Ok(if let Some(version) = version {
			assert!(version <= safe_version);
			TransactionManagerQuery::new_time_travel(TransactionId::generate(), self.clone(), version)
		} else {
			TransactionManagerQuery::new_current(TransactionId::generate(), self.clone(), safe_version)
		})
	}

	/// Returns the highest version where ALL prior versions have completed.
	/// This is useful for CDC polling to know the safe upper bound for fetching
	/// CDC events - all events up to this version are guaranteed to be in storage.
	#[instrument(name = "transaction::manager::done_until", level = "trace", skip(self))]
	pub fn done_until(&self) -> CommitVersion {
		self.inner.command.done_until()
	}

	/// Wait for the watermark to reach the given version with a timeout.
	/// Returns true if the watermark reached the target, false if timeout occurred.
	#[instrument(name = "transaction::manager::wait_for_mark_timeout", level = "trace", skip(self))]
	pub fn wait_for_mark_timeout(&self, version: CommitVersion, timeout: Duration) -> bool {
		self.inner.command.wait_for_mark_timeout(version, timeout)
	}
}

// ============================================================================
// Transaction - The main multi-version transaction type
// ============================================================================

pub struct TransactionMulti(Arc<Inner>);

pub struct Inner {
	pub(crate) tm: TransactionManager<StandardVersionProvider>,
	pub(crate) store: MultiStore,
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
	fn new(
		store: MultiStore,
		single: TransactionSingle,
		event_bus: EventBus,
		actor_system: ActorSystem,
	) -> Result<Self> {
		let version_provider = StandardVersionProvider::new(single)?;
		let tm = TransactionManager::new(version_provider, actor_system)?;

		Ok(Self {
			tm,
			store,
			event_bus,
		})
	}

	fn version(&self) -> Result<CommitVersion> {
		self.tm.version()
	}

	fn actor_system(&self) -> ActorSystem {
		self.tm.actor_system()
	}
}

impl TransactionMulti {
	pub fn testing() -> Self {
		use reifydb_runtime::actor::system::ActorSystemConfig;
		let multi_store = reifydb_store_multi::MultiStore::testing_memory();
		let single_store = reifydb_store_single::SingleStore::testing_memory();
		let event_bus = EventBus::new();
		let actor_system = ActorSystem::new(ActorSystemConfig::default());
		Self::new(
			multi_store,
			TransactionSingle::SingleVersionLock(TransactionSvl::new(single_store, event_bus.clone())),
			event_bus,
			actor_system,
		)
		.unwrap()
	}
}

impl TransactionMulti {
	#[instrument(name = "transaction::new", level = "debug", skip(store, single, event_bus, actor_system))]
	pub fn new(
		store: MultiStore,
		single: TransactionSingle,
		event_bus: EventBus,
		actor_system: ActorSystem,
	) -> Result<Self> {
		Ok(Self(Arc::new(Inner::new(store, single, event_bus, actor_system)?)))
	}

	/// Get the actor system
	pub fn actor_system(&self) -> ActorSystem {
		self.0.actor_system()
	}
}

impl TransactionMulti {
	#[instrument(name = "transaction::version", level = "trace", skip(self))]
	pub fn version(&self) -> Result<CommitVersion> {
		self.0.version()
	}

	#[instrument(name = "transaction::begin_query", level = "debug", skip(self))]
	pub fn begin_query(&self) -> Result<QueryTransaction> {
		QueryTransaction::new(self.clone(), None)
	}

	/// Begin a query transaction at a specific version.
	///
	/// This is used for parallel query execution where multiple tasks need to
	/// read from the same snapshot (same CommitVersion) for consistency.
	#[instrument(name = "transaction::begin_query_at_version", level = "debug", skip(self), fields(version = %version.0))]
	pub fn begin_query_at_version(&self, version: CommitVersion) -> Result<QueryTransaction> {
		QueryTransaction::new(self.clone(), Some(version))
	}
}

impl TransactionMulti {
	#[instrument(name = "transaction::begin_command", level = "debug", skip(self))]
	pub fn begin_command(&self) -> Result<CommandTransaction> {
		CommandTransaction::new(self.clone())
	}
}

pub enum TransactionType {
	Query(QueryTransaction),
	Command(CommandTransaction),
}

impl TransactionMulti {
	#[instrument(name = "transaction::get", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref()), version = version.0))]
	pub fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<Committed>> {
		Ok(MultiVersionGet::get(&self.store, key, version)?.map(|sv| sv.into()))
	}

	#[instrument(name = "transaction::contains_key", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref()), version = version.0))]
	pub fn contains_key(&self, key: &EncodedKey, version: CommitVersion) -> Result<bool> {
		MultiVersionContains::contains(&self.store, key, version)
	}

	/// Get a reference to the underlying transaction store.
	pub fn store(&self) -> &MultiStore {
		&self.store
	}
}
