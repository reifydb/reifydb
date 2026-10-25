// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{collections::VecDeque, ops::Deref, sync::Arc};

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	event::EventBus,
	interface::{catalog::config::GetConfig, store::WalCheckpoint},
	key::any::TaggedKey,
	testing::ProfileConfig,
};
use reifydb_runtime::{
	actor::system::{ActorSpawner, ActorSystem},
	context::{
		clock::{Clock, MockClock},
		rng::Rng,
	},
	version_epoch::VersionEpoch,
};
use reifydb_store_commit::VersionedGetResult;
use reifydb_store_multi::MultiStore;
use reifydb_value::{Result, value::duration::Duration};
use reifydb_wal::lsn::Lsn;
use tracing::{instrument, warn};
use version::{StandardVersionProvider, VersionProvider};

pub(crate) use crate::multi::oracle::Oracle;
use crate::{
	TransactionId,
	error::TransactionError,
	multi::types::*,
	wal::{CheckpointedStore, CommitWal, Recovery, StoreCheckpoint, memory_wal, replay},
};

pub mod manager;
pub mod read;
pub(crate) mod version;
pub mod write;

use crate::multi::{
	MultiReadTransaction, MultiWriteTransaction,
	lease::{VersionLeaseGuard, VersionLeases},
	transaction::manager::TransactionManagerQuery,
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
	#[allow(clippy::too_many_arguments)]
	#[instrument(
		name = "transaction::manager::new",
		level = "debug",
		skip(versions, spawner, store, clock, version_epoch, rng, config, wal, lsns)
	)]
	pub fn new(
		versions: L,
		spawner: ActorSpawner,
		store: MultiStore,
		clock: Clock,
		version_epoch: VersionEpoch,
		rng: Rng,
		config: Arc<dyn GetConfig>,
		wal: CommitWal,
		lsns: VecDeque<(CommitVersion, Lsn)>,
	) -> Result<Self>
	where
		L: 'static,
	{
		let version = versions.next()?;
		let oracle = Oracle::new(versions, spawner, store, clock, version_epoch, rng, config, wal, lsns);
		oracle.query.advance_to(version);
		oracle.command.advance_to(version);
		Ok(Self {
			inner: Arc::new(oracle),
		})
	}

	pub fn spawner(&self) -> ActorSpawner {
		self.inner.spawner()
	}

	pub fn config(&self) -> Arc<dyn GetConfig> {
		self.inner.config()
	}

	pub(crate) fn oracle(&self) -> &Arc<Oracle<L>> {
		&self.inner
	}

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
		Ok(if let Some(version) = version {
			let safe_version = self.inner.version()?;
			if version > safe_version {
				return Err(TransactionError::SnapshotVersionEvicted {
					version,
					cutoff: safe_version,
				}
				.into());
			}
			TransactionManagerQuery::new_time_travel(
				TransactionId::generate(self.inner.clock(), self.inner.rng()),
				self.clone(),
				version,
			)
		} else {
			let safe_version = self.inner.query.register_in_flight_with(|| self.inner.version())?;
			TransactionManagerQuery::new_current(
				TransactionId::generate(self.inner.clock(), self.inner.rng()),
				self.clone(),
				safe_version,
			)
		})
	}

	pub fn begin_commit(&self, version: CommitVersion) {
		self.inner.command.register_in_flight(version);
	}

	pub fn done_commit(&self, version: CommitVersion) {
		self.inner.done_commit(version);
	}

	#[instrument(name = "transaction::manager::done_until", level = "trace", skip(self))]
	pub fn done_until(&self) -> CommitVersion {
		self.inner.command.done_until()
	}

	#[instrument(name = "transaction::manager::query_done_until", level = "trace", skip(self))]
	pub fn query_done_until(&self) -> CommitVersion {
		self.inner.query.done_until()
	}

	#[instrument(name = "transaction::manager::wait_for_mark_timeout", level = "trace", skip(self))]
	pub fn wait_for_mark_timeout(&self, version: CommitVersion, timeout: Duration) -> bool {
		self.inner.command.wait_for_mark_timeout(version, timeout)
	}

	pub fn notify_on_mark(&self, version: CommitVersion, callback: Box<dyn FnOnce() + Send>) {
		self.inner.command.notify_on_mark(version, callback);
	}

	pub fn advance_version_to(&self, version: CommitVersion) {
		self.inner.versions.advance_to(version);
		self.inner.command.advance_to(version);
		self.inner.query.advance_to(version);
	}

	pub fn leases(&self) -> Arc<VersionLeases> {
		self.inner.leases.clone()
	}

	pub fn acquire_version_lease(&self, version: CommitVersion) -> Result<VersionLeaseGuard> {
		self.inner.leases.try_acquire_at(version, &self.inner.query)
	}

	pub fn acquire_cutoff_lease(&self) -> Result<VersionLeaseGuard> {
		self.inner.leases.try_acquire_at_cutoff(&self.inner.query)
	}

	pub fn acquire_current_snapshot_lease(&self) -> Result<(CommitVersion, VersionLeaseGuard)> {
		let oracle = self.inner.clone();
		let (guard, version) = oracle.leases.try_acquire_with(|| {
			let version = oracle.version()?;
			let qdu = oracle.query.done_until();
			Ok((version, qdu, version))
		})?;
		Ok((version, guard))
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
	#[allow(clippy::too_many_arguments)]
	fn new(
		store: MultiStore,
		event_bus: EventBus,
		spawner: ActorSpawner,
		clock: Clock,
		version_epoch: VersionEpoch,
		rng: Rng,
		config: Arc<dyn GetConfig>,
		wal: CommitWal,
		recovery: Recovery,
	) -> Result<Self> {
		let version_provider = StandardVersionProvider::new(recovery.last);
		let tm = TransactionManager::new(
			version_provider,
			spawner,
			store.clone(),
			clock,
			version_epoch,
			rng,
			config,
			wal,
			recovery.lsns,
		)?;

		Ok(Self {
			tm,
			store,
			event_bus,
		})
	}

	fn version(&self) -> Result<CommitVersion> {
		self.tm.version()
	}

	fn spawner(&self) -> ActorSpawner {
		self.tm.spawner()
	}

	fn bootstrapping_completed(&self) {
		self.tm.bootstrapping_completed();
	}
}

impl MultiTransaction {
	pub fn oracle_window_count(&self) -> usize {
		self.tm.oracle().window_count()
	}

	pub fn testing() -> Self {
		let multi_store = MultiStore::testing_memory();
		let actor_system = ActorSystem::testing(Clock::Real);
		let spawner = actor_system.spawner();
		let event_bus = EventBus::new(&spawner);

		let config = Arc::new(ProfileConfig);
		let clock = Clock::Mock(MockClock::from_millis(1000));
		let wal = memory_wal(clock.clone()).expect("failed to create testing commit log");
		let recovery = replay(&wal, multi_store.commit(), CommitVersion(0), CommitVersion(0));

		Self::new(
			multi_store,
			event_bus,
			spawner,
			clock,
			VersionEpoch::new(),
			Rng::seeded(42),
			config,
			wal,
			recovery,
		)
		.expect("failed to create testing MultiTransaction")
	}
}

impl MultiTransaction {
	#[instrument(
		name = "transaction::new",
		level = "debug",
		skip(store, event_bus, spawner, clock, version_epoch, rng, config, wal, recovery)
	)]
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		store: MultiStore,
		event_bus: EventBus,
		spawner: ActorSpawner,
		clock: Clock,
		version_epoch: VersionEpoch,
		rng: Rng,
		config: Arc<dyn GetConfig>,
		wal: CommitWal,
		recovery: Recovery,
	) -> Result<Self> {
		Ok(Self(Arc::new(Inner::new(
			store,
			event_bus,
			spawner,
			clock,
			version_epoch,
			rng,
			config,
			wal,
			recovery,
		)?)))
	}

	pub fn spawner(&self) -> ActorSpawner {
		self.0.spawner()
	}

	pub fn config(&self) -> Arc<dyn GetConfig> {
		self.0.tm.config()
	}

	pub fn advance_version_to(&self, version: CommitVersion) {
		self.0.tm.advance_version_to(version);
	}

	pub fn freeze_commits(&self) -> Result<CommitVersion> {
		self.0.tm.oracle().freeze_commits()
	}

	pub fn bootstrapping_completed(&self) {
		self.0.bootstrapping_completed();
	}

	pub fn wal(&self) -> &CommitWal {
		&self.0.tm.oracle().wal
	}

	pub fn store_checkpoint(&self, store: CheckpointedStore) -> Result<Arc<dyn WalCheckpoint>> {
		let oracle = self.0.tm.oracle();
		let floor = oracle.wal.floor(store.floor_name()).unwrap_or_else(|err| {
			panic!("commit log floor {} could not be taken: {err}", store.floor_name())
		});
		Ok(Arc::new(StoreCheckpoint::new(Arc::downgrade(oracle.shared()), floor)))
	}

	#[instrument(name = "transaction::query_done_until", level = "trace", skip(self))]
	pub fn query_done_until(&self) -> CommitVersion {
		self.0.tm.query_done_until()
	}
}

impl MultiTransaction {
	#[instrument(name = "transaction::version", level = "trace", skip(self))]
	pub fn version(&self) -> Result<CommitVersion> {
		self.0.version()
	}

	#[instrument(name = "transaction::begin_query", level = "trace", skip(self))]
	pub fn begin_query(&self) -> Result<MultiReadTransaction> {
		MultiReadTransaction::new(self.clone(), None)
	}

	#[instrument(name = "transaction::begin_query_at_version", level = "trace", skip(self, lease), fields(version = %lease.version().0))]
	pub fn begin_query_at_version(&self, lease: &VersionLeaseGuard) -> Result<MultiReadTransaction> {
		MultiReadTransaction::new_with_lease(self.clone(), lease.clone())
	}

	pub fn acquire_version_lease(&self, version: CommitVersion) -> Result<VersionLeaseGuard> {
		self.0.tm.acquire_version_lease(version)
	}

	pub fn acquire_cutoff_lease(&self) -> Result<VersionLeaseGuard> {
		self.0.tm.acquire_cutoff_lease()
	}

	pub fn acquire_current_snapshot_lease(&self) -> Result<(CommitVersion, VersionLeaseGuard)> {
		self.0.tm.acquire_current_snapshot_lease()
	}

	pub fn leases(&self) -> Arc<VersionLeases> {
		self.0.tm.leases()
	}
}

impl MultiTransaction {
	#[instrument(name = "transaction::begin_command", level = "debug", skip(self))]
	pub fn begin_command(&self) -> Result<MultiWriteTransaction> {
		MultiWriteTransaction::new(self.clone())
	}
}

impl MultiTransaction {
	#[instrument(name = "transaction::get", level = "trace", skip(self, keys), fields(keys = keys.len(), version = version.0))]
	pub fn get(&self, keys: &[TaggedKey], version: CommitVersion) -> Result<Vec<Option<Committed>>> {
		let encoded: Vec<EncodedKey> = keys.iter().map(TaggedKey::encode).collect();
		let found = self.store.get_versioned(&encoded, version)?;
		Ok(keys.iter()
			.zip(&encoded)
			.map(|(key, encoded)| match found.get(encoded) {
				Some(VersionedGetResult::Value {
					value,
					version,
				}) => Some(Committed {
					key: key.clone(),
					bytes: EncodedBytes(value.clone()),
					version: *version,
				}),
				_ => None,
			})
			.collect())
	}

	pub fn store(&self) -> &MultiStore {
		&self.store
	}
}
