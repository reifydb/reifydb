// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	ops::Deref,
	sync::{Arc, OnceLock},
};

use reifydb_codec::key::encoded::EncodedKey;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_core::metrics::sample::MetricsSample;
use reifydb_core::{
	common::CommitVersion, event::EventBus, lifecycle::watermark::EvictionWatermark,
	metrics::collect::MetricsCollector,
};
use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock, shutdown::Shutdown, sync::rwlock::RwLock};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::SqliteTempPathGuard;
use reifydb_value::util::cowvec::CowVec;
use tracing::instrument;

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::config::PersistentConfig;
use crate::{
	CommitBufferConfig,
	config::MultiStoreConfig,
	flush::{ObjectPersistence, engine::FlushEngine},
	tier::{
		commit::buffer::MultiCommitBufferTier,
		persistent::MultiPersistentTier,
		read::{MultiReadBufferTier, ReadBufferConfig, ReadBufferShardMetrics},
	},
};

pub mod multi;
pub mod router;

use crate::Result;

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
struct SqlitePageCacheCollector {
	persistent: MultiPersistentTier,
}

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
impl MetricsCollector for SqlitePageCacheCollector {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		let metrics = self.persistent.page_cache_metrics();
		out.push(MetricsSample::bytes("sqlite::multi", "page_cache_used_bytes", metrics.used));
		out.push(MetricsSample::counter("sqlite::multi", "page_cache_hit_count", metrics.hits.as_u64()));
		out.push(MetricsSample::counter("sqlite::multi", "page_cache_miss_count", metrics.misses.as_u64()));
		out.push(MetricsSample::count(
			"sqlite::multi",
			"page_cache_sampled_connections",
			metrics.connections_sampled.as_u64(),
		));
	}
}

#[derive(Clone)]
pub struct StandardMultiStore(Arc<StandardMultiStoreInner>);

pub struct StandardMultiStoreInner {
	pub(crate) commit: MultiCommitBufferTier,
	pub(crate) persistent: Option<MultiPersistentTier>,
	pub(crate) read: Option<MultiReadBufferTier>,

	#[allow(dead_code)]
	pub(crate) flush_engine: Option<Arc<FlushEngine>>,
	#[allow(dead_code)]
	pub(crate) row_settings_provider: Arc<OnceLock<Arc<dyn ObjectPersistence>>>,
	#[allow(dead_code)]
	pub(crate) eviction_watermark: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>>,

	pub(crate) event_bus: EventBus,
}

impl StandardMultiStore {
	#[instrument(name = "store::multi::new", level = "debug", skip(config), fields(
		has_persistent = config.persistent.is_some(),
	))]
	pub fn new(config: MultiStoreConfig) -> Result<Self> {
		let commit = config.commit.storage;

		let row_settings_provider: Arc<OnceLock<Arc<dyn ObjectPersistence>>> = Arc::new(OnceLock::new());

		let eviction_watermark: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>> = Arc::new(RwLock::new(None));

		let read = config
			.persistent
			.is_some()
			.then(|| config.read.and_then(MultiReadBufferTier::new))
			.flatten();

		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		let (persistent, flush_engine) = {
			let persistent_config = config.persistent.clone();
			let persistent = persistent_config.as_ref().map(|c| c.storage.clone());
			let flush_engine = match (persistent.as_ref(), persistent_config.as_ref()) {
				(Some(persistent_storage), Some(persistent_cfg)) => Some(Arc::new(FlushEngine::new(
					commit.clone(),
					persistent_storage.clone(),
					persistent_cfg.flush_interval,
					row_settings_provider.clone(),
					eviction_watermark.clone(),
					read.clone(),
					config.clock.clone(),
					config.event_bus.clone(),
				))),
				_ => None,
			};
			(persistent, flush_engine)
		};

		#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
		let (persistent, flush_engine): (Option<MultiPersistentTier>, Option<Arc<FlushEngine>>) = {
			let _ = config.persistent;
			(None, None)
		};

		let read = persistent.as_ref().and(read);

		Ok(Self(Arc::new(StandardMultiStoreInner {
			commit,
			persistent,
			read,
			flush_engine,
			row_settings_provider,
			eviction_watermark,
			event_bus: config.event_bus,
		})))
	}

	pub fn flush_engine(&self) -> Option<Arc<FlushEngine>> {
		self.flush_engine.clone()
	}

	pub fn configure_wal_autocheckpoint(&self, frames: u32) {
		if let Some(persistent) = &self.persistent {
			persistent.set_checkpoint_threshold(frames);
		}
	}

	pub fn insert_read_key(&self, key: EncodedKey, version: CommitVersion, value: Option<CowVec<u8>>) {
		if let Some(read) = &self.read {
			read.insert(key, version, value);
		}
	}

	pub fn invalidate_read_key(&self, key: &EncodedKey) {
		if let Some(read) = &self.read {
			read.invalidate(key);
		}
	}

	pub fn clear_read(&self) {
		if let Some(read) = &self.read {
			read.clear();
		}
	}

	pub fn set_row_settings_provider(&self, provider: Arc<dyn ObjectPersistence>) {
		let _ = self.row_settings_provider.set(provider);
	}

	pub fn set_eviction_watermark(&self, watermark: Arc<dyn EvictionWatermark>) {
		*self.eviction_watermark.write() = Some(watermark);
	}

	pub fn clear_eviction_watermark(&self) {
		*self.eviction_watermark.write() = None;
	}

	pub fn commit(&self) -> &MultiCommitBufferTier {
		&self.commit
	}

	pub fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}

	pub fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>> {
		let mut collectors: Vec<Arc<dyn MetricsCollector>> = Vec::new();
		if let Some(read) = &self.read {
			collectors.push(Arc::new(read.clone()));
		}
		collectors.push(Arc::new(self.commit.clone()));
		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		if let Some(persistent) = &self.persistent {
			collectors.push(Arc::new(SqlitePageCacheCollector {
				persistent: persistent.clone(),
			}));
		}
		collectors
	}

	pub fn persistent(&self) -> Option<&MultiPersistentTier> {
		self.persistent.as_ref()
	}

	pub fn read_buffer_shard_metrics(&self) -> Vec<ReadBufferShardMetrics> {
		self.read.as_ref().map(|read| read.shard_metrics()).unwrap_or_default()
	}

	pub fn flush_pending_blocking(&self) {
		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		if let Some(engine) = self.flush_engine.as_ref() {
			self.event_bus.wait_for_completion();
			engine.flush_pending();
		}
	}

	pub fn flush_all_blocking(&self) {
		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		if let Some(engine) = self.flush_engine.as_ref() {
			self.event_bus.wait_for_completion();
			engine.flush_all();
		}
	}
}

impl Deref for StandardMultiStore {
	type Target = StandardMultiStoreInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Shutdown for StandardMultiStore {
	fn shutdown(&self) {
		if let Some(persistent) = self.persistent.as_ref() {
			persistent.shutdown();
		}
	}
}

impl StandardMultiStore {
	pub fn testing_memory() -> Self {
		let clock = Clock::testing();
		let actor_system = ActorSystem::testing(clock.clone());
		let spawner = actor_system.spawner();
		let event_bus = EventBus::new(&spawner);
		Self::new(MultiStoreConfig {
			commit: CommitBufferConfig {
				storage: MultiCommitBufferTier::memory(),
			},
			persistent: None,
			read: None,
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus,
			spawner,
			clock,
		})
		.unwrap()
	}

	pub fn testing_memory_with_eventbus(event_bus: EventBus) -> Self {
		let clock = Clock::testing();
		let actor_system = ActorSystem::testing(clock.clone());
		let spawner = actor_system.spawner();
		Self::new(MultiStoreConfig {
			commit: CommitBufferConfig {
				storage: MultiCommitBufferTier::memory(),
			},
			persistent: None,
			read: None,
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus,
			spawner,
			clock,
		})
		.unwrap()
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn testing_memory_with_persistent_sqlite() -> (Self, SqliteTempPathGuard) {
		Self::testing_memory_with_persistent_sqlite_read(ReadBufferConfig::default())
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn testing_memory_with_persistent_sqlite_read(read: ReadBufferConfig) -> (Self, SqliteTempPathGuard) {
		let clock = Clock::testing();
		let actor_system = ActorSystem::testing(clock.clone());
		let spawner = actor_system.spawner();
		let event_bus = EventBus::new(&spawner);
		let (persistent, guard) = PersistentConfig::sqlite_in_memory();
		let store = Self::new(MultiStoreConfig {
			commit: CommitBufferConfig {
				storage: MultiCommitBufferTier::memory(),
			},
			persistent: Some(persistent),
			read: Some(read),
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus,
			spawner,
			clock,
		})
		.unwrap();
		(store, guard)
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn testing_memory_with_persistent_sqlite_with_eventbus(event_bus: EventBus) -> (Self, SqliteTempPathGuard) {
		let clock = Clock::testing();
		let actor_system = ActorSystem::testing(clock.clone());
		let spawner = actor_system.spawner();
		let (persistent, guard) = PersistentConfig::sqlite_in_memory();
		let store = Self::new(MultiStoreConfig {
			commit: CommitBufferConfig {
				storage: MultiCommitBufferTier::memory(),
			},
			persistent: Some(persistent),
			read: Some(ReadBufferConfig::default()),
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus,
			spawner,
			clock,
		})
		.unwrap();
		(store, guard)
	}
}
