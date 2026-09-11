// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod census;
mod checkpoint;
pub mod occupancy;
mod pager;
pub mod state;
#[cfg(test)]
mod tests;

#[cfg(test)]
use std::sync::OnceLock;
use std::{ops::Deref, sync::Arc};

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_core::default;
use reifydb_core::{common::CommitVersion, lifecycle::watermark::CheckpointFloor, metrics::collect::MetricsCollector};
use reifydb_filter::adaptive::FilterMetrics;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_filter::{
	actor::{FilterActor, FilterMessage},
	config::FilterConfig,
};
use reifydb_runtime::{
	actor::{
		mailbox::ActorRef,
		system::{ActorSpawner, ActorSystem},
	},
	context::clock::Clock,
	shutdown::Shutdown,
};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_store::metrics::PageCacheMetrics;

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::{
	actor::{
		range_evict::RangeEvictActor, resident_evict::ResidentEvictActor, resident_flush::ResidentFlushActor,
	},
	config::OperatorPersistentConfig,
	persistent::{Enumerate, filter::OperatorStateKeySource},
	range::OperatorRangeConfig,
	resident::FILTER_KEYS,
};
use crate::{
	actor::{
		Waker,
		resident_flush::{FlushMessage, flush_now, flush_pending},
	},
	config::OperatorStoreConfig,
	persistent::{Persistent, PersistentTier},
	range::{
		OperatorRangeTier, RangeSink,
		tiers::{RangeKeyspaceMetrics, RangeTiers},
	},
	resident::Resident,
	store::{census::OperatorCensus, occupancy::KeyspaceOccupancy},
};

#[repr(u8)]
#[derive(Clone)]
pub enum OperatorStore {
	Standard(StandardOperatorStore) = 0,
}

/// Entered between the two tier reads of a checkpoint merge, so a test can land a whole flush at the
/// one instant an entry is leaving the buffer and has not yet landed in the persistent tier.
#[cfg(test)]
type CheckpointInterlock = Box<dyn Fn(&StandardOperatorStore) + Send + Sync>;

#[derive(Clone)]
pub struct StandardOperatorStore(Arc<StandardOperatorStoreInner>);

pub struct StandardOperatorStoreInner {
	pub(crate) resident: Resident,
	pub(crate) occupancy: KeyspaceOccupancy,
	pub(crate) census: OperatorCensus,
	pub(crate) persistent: PersistentTier,
	pub(crate) range: OperatorRangeTier,
	pub(crate) flush: Option<ActorRef<FlushMessage>>,
	#[allow(dead_code)]
	pub(crate) spawner: ActorSpawner,
	#[cfg(test)]
	pub(crate) checkpoint_interlock: OnceLock<CheckpointInterlock>,
}

impl Deref for StandardOperatorStore {
	type Target = StandardOperatorStoreInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl StandardOperatorStore {
	pub fn new(config: OperatorStoreConfig) -> Self {
		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		let flush_interval = config.resident.flush_interval;
		let resident = config.resident.storage;
		let spawner = config.spawner;
		let range = config
			.persistent
			.is_some()
			.then(|| config.range.map(Into::into).and_then(RangeTiers::new))
			.flatten();

		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		let (persistent, flush) = {
			if let Some(persistent) = config.persistent.as_ref() {
				resident.attach_sinks(
					persistent.storage.clone(),
					OperatorRangeTier::standard(range.clone()),
				);
			}
			let flush = config
				.persistent
				.as_ref()
				.map(|_| ResidentFlushActor::spawn(&spawner, resident.clone(), flush_interval));
			if config.persistent.is_some() {
				resident.attach_evictor(Waker::Spawned(ResidentEvictActor::spawn(
					&spawner,
					resident.clone(),
				)));
			}
			if let Some(range) = range.as_ref() {
				RangeEvictActor::spawn(
					&spawner,
					range.clone(),
					default::store::OPERATOR_RANGE_RELIEF_INTERVAL,
				);
			}
			(
				config.persistent
					.map(|persistent| persistent.storage)
					.unwrap_or(PersistentTier::Absent),
				flush,
			)
		};

		#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
		let (persistent, flush): (PersistentTier, Option<ActorRef<FlushMessage>>) = (
			config.persistent.map(|persistent| persistent.storage).unwrap_or(PersistentTier::Absent),
			None,
		);

		let range = OperatorRangeTier::standard(if persistent.is_absent() {
			None
		} else {
			range
		});
		if let Some(flush) = flush.as_ref() {
			resident.attach_flusher(Waker::Spawned(flush.clone()));
		}
		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		if !persistent.is_absent() && persistent.census().is_ok_and(|census| !census.is_empty()) {
			let actor = FilterActor::spawn(&spawner);
			let _ = actor.send(FilterMessage::Register {
				filter: resident.filter(),
				source: Box::new(OperatorStateKeySource::new(persistent.clone())),
				config: FilterConfig {
					min_size_keys: FILTER_KEYS,
					..FilterConfig::default()
				},
			});
		}

		Self(Arc::new(StandardOperatorStoreInner {
			resident,
			occupancy: KeyspaceOccupancy::new(),
			census: OperatorCensus::seeded(&persistent),
			persistent,
			range,
			flush,
			spawner,
			#[cfg(test)]
			checkpoint_interlock: OnceLock::new(),
		}))
	}

	#[cfg(test)]
	pub(crate) fn attach_checkpoint_interlock(&self, interlock: CheckpointInterlock) {
		let _ = self.checkpoint_interlock.set(interlock);
	}

	#[cfg(test)]
	pub(crate) fn checkpoint_interlock(&self) {
		if let Some(interlock) = self.checkpoint_interlock.get() {
			interlock(self);
		}
	}

	#[cfg(not(test))]
	pub(crate) fn checkpoint_interlock(&self) {}

	pub fn resident(&self) -> &Resident {
		&self.resident
	}

	pub fn occupancy(&self) -> &KeyspaceOccupancy {
		&self.occupancy
	}

	pub fn persistent(&self) -> &PersistentTier {
		&self.persistent
	}

	pub fn flush_pending_blocking(&self) -> bool {
		match &self.flush {
			Some(actor) => flush_pending(actor),
			None => true,
		}
	}

	pub fn range(&self) -> &OperatorRangeTier {
		&self.range
	}

	pub fn range_keyspace_metrics(&self) -> Vec<RangeKeyspaceMetrics> {
		self.range.keyspace_metrics()
	}

	pub fn filter_metrics(&self) -> FilterMetrics {
		self.resident.filter_metrics()
	}

	pub fn persistent_page_cache_metrics(&self) -> Option<PageCacheMetrics> {
		self.persistent.page_cache_metrics()
	}

	pub fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>> {
		let mut collectors = Persistent::metrics_collectors(&self.persistent);
		if let Some(tiers) = self.range.tiers() {
			collectors.push(Arc::new(tiers.clone()));
		}
		collectors
	}
}

impl Shutdown for StandardOperatorStore {
	fn shutdown(&self) {
		if self.persistent.is_absent() {
			return;
		}
		flush_now(&self.resident);
		self.persistent.shutdown();
	}
}

impl OperatorStore {
	pub fn standard(config: OperatorStoreConfig) -> Self {
		Self::Standard(StandardOperatorStore::new(config))
	}

	pub fn testing_memory() -> Self {
		let clock = Clock::testing();
		let actor_system = ActorSystem::testing(clock.clone());
		let spawner = actor_system.spawner();
		Self::standard(OperatorStoreConfig::memory(spawner, clock))
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn testing_memory_with_persistent_sqlite() -> (Self, SqliteTempPathGuard) {
		let clock = Clock::testing();
		let actor_system = ActorSystem::testing(clock.clone());
		let spawner = actor_system.spawner();
		let (persistent, guard) = OperatorPersistentConfig::sqlite_in_memory();
		(
			Self::standard(OperatorStoreConfig {
				range: Some(OperatorRangeConfig::testing()),
				..OperatorStoreConfig::sqlite(persistent, spawner, clock)
			}),
			guard,
		)
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(config: SqliteConfig, spawner: ActorSpawner, clock: Clock) -> Self {
		Self::standard(OperatorStoreConfig {
			range: Some(OperatorRangeConfig::testing()),
			..OperatorStoreConfig::sqlite(OperatorPersistentConfig::sqlite(config), spawner, clock)
		})
	}

	pub fn resident(&self) -> &Resident {
		match self {
			Self::Standard(store) => store.resident(),
		}
	}

	pub fn occupancy(&self) -> &KeyspaceOccupancy {
		match self {
			Self::Standard(store) => store.occupancy(),
		}
	}

	pub fn persistent(&self) -> &PersistentTier {
		match self {
			Self::Standard(store) => store.persistent(),
		}
	}

	pub fn flush_pending_blocking(&self) -> bool {
		match self {
			Self::Standard(store) => store.flush_pending_blocking(),
		}
	}

	pub fn range(&self) -> &OperatorRangeTier {
		match self {
			Self::Standard(store) => store.range(),
		}
	}

	pub fn range_keyspace_metrics(&self) -> Vec<RangeKeyspaceMetrics> {
		match self {
			Self::Standard(store) => store.range_keyspace_metrics(),
		}
	}

	pub fn persistent_page_cache_metrics(&self) -> Option<PageCacheMetrics> {
		match self {
			Self::Standard(store) => store.persistent_page_cache_metrics(),
		}
	}

	pub fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>> {
		match self {
			Self::Standard(store) => store.metrics_collectors(),
		}
	}
}

impl CheckpointFloor for OperatorStore {
	fn floor(&self) -> Option<CommitVersion> {
		self.checkpoint_floor().ok().flatten()
	}
}

impl Shutdown for OperatorStore {
	fn shutdown(&self) {
		match self {
			Self::Standard(store) => store.shutdown(),
		}
	}
}
