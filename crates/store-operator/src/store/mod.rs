// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod anchor;
mod census;
mod checkpoint;
mod state;

use std::{ops::Deref, sync::Arc};

use reifydb_core::{common::CommitVersion, lifecycle::watermark::CheckpointFloor, metrics::collect::MetricsCollector};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_filter::{actor::FilterActor, config::FilterConfig};
use reifydb_filter::{actor::FilterMessage, adaptive::FilterMetrics};
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

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::{
	config::OperatorPersistentConfig, filter::source::OperatorStateKeySource, flush::OperatorFlushActor,
	sqlite::SqliteOperatorStorage,
};
use crate::{
	config::OperatorStoreConfig,
	flush::{FlushMessage, flush_now, flush_pending},
	tier::{
		commit::OperatorCommitBuffer,
		persistent::{OperatorPageCacheMetrics, OperatorPersistentTier},
		point::{OperatorPointKeyspaceMetrics, OperatorPointShardMetrics, OperatorPointTier},
		range::{OperatorRangeKeyspaceMetrics, OperatorRangeShardMetrics, OperatorRangeTier},
	},
};

#[repr(u8)]
#[derive(Clone)]
pub enum OperatorStore {
	Standard(StandardOperatorStore) = 0,
}

#[derive(Clone)]
pub struct StandardOperatorStore(Arc<StandardOperatorStoreInner>);

pub struct StandardOperatorStoreInner {
	pub(crate) commit: OperatorCommitBuffer,
	pub(crate) persistent: Option<OperatorPersistentTier>,
	pub(crate) point: Option<OperatorPointTier>,
	pub(crate) range: Option<OperatorRangeTier>,
	pub(crate) flush: Option<ActorRef<FlushMessage>>,
	pub(crate) filter: Option<ActorRef<FilterMessage>>,
	#[allow(dead_code)]
	pub(crate) spawner: ActorSpawner,
}

impl Deref for StandardOperatorStore {
	type Target = StandardOperatorStoreInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl StandardOperatorStore {
	pub fn new(config: OperatorStoreConfig) -> Self {
		let commit = config.commit.storage;
		let spawner = config.spawner;
		let point =
			config.persistent.is_some().then(|| config.point.and_then(OperatorPointTier::new)).flatten();
		let range =
			config.persistent.is_some().then(|| config.range.and_then(OperatorRangeTier::new)).flatten();

		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		let (persistent, flush, filter) = {
			let flush = config.persistent.as_ref().map(|persistent| {
				OperatorFlushActor::spawn(
					&spawner,
					commit.clone(),
					persistent.storage.clone(),
					point.clone(),
					range.clone(),
					persistent.flush_interval,
				)
			});
			let filter = config.persistent.as_ref().map(|persistent| {
				let storage = persistent.storage.sqlite_storage().clone();
				let actor = FilterActor::spawn(&spawner);
				actor.send(FilterMessage::Register {
					filter: storage.filter().handle(),
					source: Box::new(OperatorStateKeySource::new(storage)),
					config: FilterConfig::default(),
				})
				.expect("operator state filter source could not be registered");
				actor
			});
			(config.persistent.map(|persistent| persistent.storage), flush, filter)
		};

		#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
		let (persistent, flush, filter): (
			Option<OperatorPersistentTier>,
			Option<ActorRef<FlushMessage>>,
			Option<ActorRef<FilterMessage>>,
		) = match config.persistent {
			Some(persistent) => match persistent.storage {},
			None => (None, None, None),
		};

		let point = persistent.as_ref().and(point);
		let range = persistent.as_ref().and(range);
		if let Some(flush) = flush.as_ref() {
			commit.attach_flusher(flush.clone());
		}

		Self(Arc::new(StandardOperatorStoreInner {
			commit,
			persistent,
			point,
			range,
			flush,
			filter,
			spawner,
		}))
	}

	pub fn commit(&self) -> &OperatorCommitBuffer {
		&self.commit
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn persistent(&self) -> Option<&SqliteOperatorStorage> {
		self.persistent.as_ref().map(OperatorPersistentTier::sqlite_storage)
	}

	pub fn flush_pending_blocking(&self) -> bool {
		match &self.flush {
			Some(actor) => flush_pending(actor),
			None => true,
		}
	}

	pub fn point(&self) -> Option<&OperatorPointTier> {
		self.point.as_ref()
	}

	pub fn range(&self) -> Option<&OperatorRangeTier> {
		self.range.as_ref()
	}

	pub fn point_shard_metrics(&self) -> Vec<OperatorPointShardMetrics> {
		self.point.as_ref().map(OperatorPointTier::shard_metrics).unwrap_or_default()
	}

	pub fn point_keyspace_metrics(&self) -> Vec<OperatorPointKeyspaceMetrics> {
		self.point.as_ref().map(OperatorPointTier::keyspace_metrics).unwrap_or_default()
	}

	pub fn range_shard_metrics(&self) -> Vec<OperatorRangeShardMetrics> {
		self.range.as_ref().map(OperatorRangeTier::shard_metrics).unwrap_or_default()
	}

	pub fn range_keyspace_metrics(&self) -> Vec<OperatorRangeKeyspaceMetrics> {
		self.range.as_ref().map(OperatorRangeTier::keyspace_metrics).unwrap_or_default()
	}

	pub fn persistent_page_cache_metrics(&self) -> Option<OperatorPageCacheMetrics> {
		self.persistent.as_ref().map(OperatorPersistentTier::page_cache_metrics)
	}

	pub fn persistent_filter_metrics(&self) -> Option<FilterMetrics> {
		self.persistent.as_ref().map(|tier| tier.filter().metrics())
	}

	pub fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>> {
		let mut collectors =
			self.persistent.as_ref().map(OperatorPersistentTier::metrics_collectors).unwrap_or_default();
		if let Some(point) = &self.point {
			collectors.push(Arc::new(point.clone()));
		}
		if let Some(range) = &self.range {
			collectors.push(Arc::new(range.clone()));
		}
		collectors
	}
}

impl Shutdown for StandardOperatorStore {
	fn shutdown(&self) {
		if let Some(filter) = self.filter.as_ref() {
			let _ = filter.send(FilterMessage::Shutdown);
		}
		let Some(persistent) = self.persistent.as_ref() else {
			return;
		};
		flush_now(&self.commit, persistent, self.point.as_ref(), self.range.as_ref());
		persistent.shutdown();
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
		(Self::standard(OperatorStoreConfig::sqlite(persistent, spawner, clock)), guard)
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn sqlite(config: SqliteConfig, spawner: ActorSpawner, clock: Clock) -> Self {
		Self::standard(OperatorStoreConfig::sqlite(OperatorPersistentConfig::sqlite(config), spawner, clock))
	}

	pub fn commit(&self) -> &OperatorCommitBuffer {
		match self {
			Self::Standard(store) => store.commit(),
		}
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn persistent(&self) -> Option<&SqliteOperatorStorage> {
		match self {
			Self::Standard(store) => store.persistent(),
		}
	}

	pub fn flush_pending_blocking(&self) -> bool {
		match self {
			Self::Standard(store) => store.flush_pending_blocking(),
		}
	}

	pub fn point(&self) -> Option<&OperatorPointTier> {
		match self {
			Self::Standard(store) => store.point(),
		}
	}

	pub fn range(&self) -> Option<&OperatorRangeTier> {
		match self {
			Self::Standard(store) => store.range(),
		}
	}

	pub fn point_shard_metrics(&self) -> Vec<OperatorPointShardMetrics> {
		match self {
			Self::Standard(store) => store.point_shard_metrics(),
		}
	}

	pub fn point_keyspace_metrics(&self) -> Vec<OperatorPointKeyspaceMetrics> {
		match self {
			Self::Standard(store) => store.point_keyspace_metrics(),
		}
	}

	pub fn range_shard_metrics(&self) -> Vec<OperatorRangeShardMetrics> {
		match self {
			Self::Standard(store) => store.range_shard_metrics(),
		}
	}

	pub fn range_keyspace_metrics(&self) -> Vec<OperatorRangeKeyspaceMetrics> {
		match self {
			Self::Standard(store) => store.range_keyspace_metrics(),
		}
	}

	pub fn persistent_page_cache_metrics(&self) -> Option<OperatorPageCacheMetrics> {
		match self {
			Self::Standard(store) => store.persistent_page_cache_metrics(),
		}
	}

	pub fn persistent_filter_metrics(&self) -> Option<FilterMetrics> {
		match self {
			Self::Standard(store) => store.persistent_filter_metrics(),
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
		self.checkpoint_floor()
	}
}

impl Shutdown for OperatorStore {
	fn shutdown(&self) {
		match self {
			Self::Standard(store) => store.shutdown(),
		}
	}
}
