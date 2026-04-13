// SPDX-License-Identifier: Apache-2.0
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
	interface::{
		catalog::config::{ConfigKey, GetConfig},
		store::{MultiVersionContains, MultiVersionGet},
	},
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::{
		clock::{Clock, MockClock},
		rng::Rng,
	},
	pool::{PoolConfig, Pools},
};
use reifydb_store_multi::MultiStore;
use reifydb_type::{Result, util::hex, value::Value};
use tracing::instrument;
use version::{StandardVersionProvider, VersionProvider};

pub(crate) use crate::multi::oracle::{CreateCommitResult, Oracle};
use crate::{TransactionId, multi::types::*, single::SingleTransaction};

pub mod manager;
pub mod read;
pub mod replica;
pub(crate) mod version;
pub mod write;

use reifydb_store_single::SingleStore;

use crate::multi::{
	MultiReadTransaction, MultiReplicaTransaction, MultiWriteTransaction,
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
	pub fn write(&self) -> Result<TransactionManagerCommand<L>> {
		Ok(TransactionManagerCommand {
			id: TransactionId::generate(self.inner.metrics_clock(), self.inner.rng()),
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
	#[instrument(
		name = "transaction::manager::new",
		level = "debug",
		skip(clock, actor_system, metrics_clock, rng, config)
	)]
	pub fn new(
		clock: L,
		actor_system: ActorSystem,
		metrics_clock: Clock,
		rng: Rng,
		config: Arc<dyn GetConfig>,
	) -> Result<Self> {
		let version = clock.next()?;
		let oracle = Oracle::new(clock, actor_system, metrics_clock, rng, config);
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

	/// Get the shared configuration.
	pub fn config(&self) -> Arc<dyn GetConfig> {
		self.inner.config()
	}

	/// Clear the conflict detection window after bootstrap.
	pub fn bootstrapping_completed(&self) {
		self.inner.bootstrapping_completed();
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
			TransactionManagerQuery::new_time_travel(
				TransactionId::generate(self.inner.metrics_clock(), self.inner.rng()),
				self.clone(),
				version,
			)
		} else {
			TransactionManagerQuery::new_current(
				TransactionId::generate(self.inner.metrics_clock(), self.inner.rng()),
				self.clone(),
				safe_version,
			)
		})
	}

	/// Register a version with the command watermark before storage write.
	/// Used by the replica applier to participate in the watermark system.
	pub fn begin_commit(&self, version: CommitVersion) {
		self.inner.command.begin(version);
	}

	/// Mark a commit version as done in the command watermark.
	/// Used by the replica applier after storage write completes.
	pub fn done_commit(&self, version: CommitVersion) {
		self.inner.done_commit(version);
	}

	/// Advance the version provider's clock to at least the given version.
	/// Used by the replica applier so that `clock.current()` returns
	/// the latest replicated version for subsequent query transactions.
	pub fn advance_clock_to(&self, version: CommitVersion) {
		self.inner.clock.advance_to(version);
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

	/// Advance the version state for replica replication.
	///
	/// This advances the watermark, the version provider counter, and the query
	/// watermark so that queries can see replicated data. Must only be called
	/// from the replica applier in sequential version order.
	pub fn advance_version_for_replica(&self, version: CommitVersion) {
		self.inner.advance_version_for_replica(version);
		self.inner.command.advance_to(version);
		self.inner.query.advance_to(version);
	}
}

pub struct MultiTransaction(Arc<Inner>);

pub struct Inner {
	pub(crate) tm: TransactionManager<StandardVersionProvider>,
	pub(crate) store: MultiStore,
	pub(crate) event_bus: EventBus,
}

impl Deref for MultiTransaction {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Clone for MultiTransaction {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl Inner {
	fn new(
		store: MultiStore,
		single: SingleTransaction,
		event_bus: EventBus,
		actor_system: ActorSystem,
		metrics_clock: Clock,
		rng: Rng,
		config: Arc<dyn GetConfig>,
	) -> Result<Self> {
		let version_provider = StandardVersionProvider::new(single)?;
		let tm = TransactionManager::new(version_provider, actor_system, metrics_clock, rng, config)?;

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

	fn bootstrapping_completed(&self) {
		self.tm.bootstrapping_completed();
	}
}

impl MultiTransaction {
	pub fn testing() -> Self {
		let multi_store = MultiStore::testing_memory();
		let single_store = SingleStore::testing_memory();
		let pools = Pools::new(PoolConfig::default());
		let actor_system = ActorSystem::new(pools, Clock::Real);
		let event_bus = EventBus::new(&actor_system);

		struct DummyConfig;
		impl GetConfig for DummyConfig {
			fn get_config(&self, key: ConfigKey) -> Value {
				key.default_value()
			}
			fn get_config_at(&self, key: ConfigKey, _version: CommitVersion) -> Value {
				key.default_value()
			}
		}
		let config = Arc::new(DummyConfig);

		Self::new(
			multi_store,
			SingleTransaction::new(single_store, event_bus.clone()),
			event_bus,
			actor_system,
			Clock::Mock(MockClock::from_millis(1000)),
			Rng::seeded(42),
			config,
		)
		.expect("failed to create testing MultiTransaction")
	}
}

impl MultiTransaction {
	#[instrument(
		name = "transaction::new",
		level = "debug",
		skip(store, single, event_bus, actor_system, metrics_clock, rng, config)
	)]
	pub fn new(
		store: MultiStore,
		single: SingleTransaction,
		event_bus: EventBus,
		actor_system: ActorSystem,
		metrics_clock: Clock,
		rng: Rng,
		config: Arc<dyn GetConfig>,
	) -> Result<Self> {
		Ok(Self(Arc::new(Inner::new(store, single, event_bus, actor_system, metrics_clock, rng, config)?)))
	}

	/// Get the actor system
	pub fn actor_system(&self) -> ActorSystem {
		self.0.actor_system()
	}

	/// Get the shared configuration from the oracle.
	pub fn config(&self) -> Arc<dyn GetConfig> {
		self.0.tm.config()
	}

	/// Clear the conflict detection window after bootstrap.
	pub fn bootstrapping_completed(&self) {
		self.0.bootstrapping_completed();
	}
}

impl MultiTransaction {
	#[instrument(name = "transaction::version", level = "trace", skip(self))]
	pub fn version(&self) -> Result<CommitVersion> {
		self.0.version()
	}

	#[instrument(name = "transaction::begin_query", level = "debug", skip(self))]
	pub fn begin_query(&self) -> Result<MultiReadTransaction> {
		MultiReadTransaction::new(self.clone(), None)
	}

	/// Begin a query transaction at a specific version.
	///
	/// This is used for parallel query execution where multiple tasks need to
	/// read from the same snapshot (same CommitVersion) for consistency.
	#[instrument(name = "transaction::begin_query_at_version", level = "debug", skip(self), fields(version = %version.0))]
	pub fn begin_query_at_version(&self, version: CommitVersion) -> Result<MultiReadTransaction> {
		MultiReadTransaction::new(self.clone(), Some(version))
	}
}

impl MultiTransaction {
	#[instrument(name = "transaction::begin_command", level = "debug", skip(self))]
	pub fn begin_command(&self) -> Result<MultiWriteTransaction> {
		MultiWriteTransaction::new(self.clone())
	}

	/// Begin a replica write transaction at the primary's exact version.
	///
	/// The returned transaction commits at the given version, bypassing
	/// oracle conflict detection and version allocation.
	#[instrument(name = "transaction::begin_replica", level = "debug", skip(self), fields(version = %version.0))]
	pub fn begin_replica(&self, version: CommitVersion) -> Result<MultiReplicaTransaction> {
		MultiReplicaTransaction::new(self.clone(), version)
	}
}

pub enum TransactionType {
	Query(MultiReadTransaction),
	Command(Box<MultiWriteTransaction>),
}

impl MultiTransaction {
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
