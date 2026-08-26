// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	ops::Deref,
	sync::{
		Arc, OnceLock,
		atomic::{AtomicU64, Ordering},
	},
};

use reifydb_codec::key::encoded::EncodedKey;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_core::metrics::sample::MetricsSample;
use reifydb_core::{
	common::CommitVersion, event::EventBus, lifecycle::watermark::EvictionWatermark,
	metrics::collect::MetricsCollector,
};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_filter::{actor::FilterActor, config::FilterConfig};
use reifydb_filter::{actor::FilterMessage, adaptive::FilterMetrics};
use reifydb_runtime::{
	actor::{mailbox::ActorRef, system::ActorSystem},
	context::clock::Clock,
	shutdown::Shutdown,
	sync::rwlock::RwLock,
};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_sqlite::SqliteTempPathGuard;
use reifydb_store::metrics::PageCacheMetrics;
use reifydb_value::{count::Count, reifydb_assertions, util::cowvec::CowVec};
use tracing::instrument;

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::filter::source::MultiCurrentKeySource;
use crate::{
	CommitBufferConfig,
	config::MultiStoreConfig,
	flush::{ObjectPersistence, engine::FlushEngine},
	tier::{
		commit::buffer::{MultiCommitBufferTier, MultiCommitMetrics},
		persistent::MultiPersistentTier,
		range::{MultiRangeConfig, MultiRangeTier},
		read::{MultiReadBufferTier, ReadBufferShardMetrics},
	},
};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::{config::PersistentConfig, tier::read::ReadBufferConfig};

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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MultiPersistentProbeMetrics {
	pub persistent_probes: Count,
	pub persistent_absent: Count,
}

#[derive(Clone)]
pub struct StandardMultiStore(Arc<StandardMultiStoreInner>);

pub struct StandardMultiStoreInner {
	pub(crate) commit: MultiCommitBufferTier,
	pub(crate) persistent: Option<MultiPersistentTier>,
	pub(crate) read: Option<MultiReadBufferTier>,
	pub(crate) range: Option<MultiRangeTier>,

	#[allow(dead_code)]
	pub(crate) flush_engine: Option<Arc<FlushEngine>>,
	#[allow(dead_code)]
	pub(crate) row_settings_provider: Arc<OnceLock<Arc<dyn ObjectPersistence>>>,
	#[allow(dead_code)]
	pub(crate) eviction_watermark: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>>,

	pub(crate) event_bus: EventBus,

	pub(crate) persistent_probes: AtomicU64,
	pub(crate) persistent_absent: AtomicU64,

	pub(crate) filter: Option<ActorRef<FilterMessage>>,
}

impl StandardMultiStore {
	#[instrument(name = "store::multi::new", level = "debug", skip(config), fields(
		has_persistent = config.persistent.is_some(),
	))]
	pub fn new(config: MultiStoreConfig) -> Result<Self> {
		let commit = config.commit.storage;

		let row_settings_provider: Arc<OnceLock<Arc<dyn ObjectPersistence>>> = Arc::new(OnceLock::new());

		let eviction_watermark: Arc<RwLock<Option<Arc<dyn EvictionWatermark>>>> = Arc::new(RwLock::new(None));

		let read =
			config.persistent.is_some().then(|| config.read.and_then(MultiReadBufferTier::new)).flatten();

		let range = config
			.persistent
			.is_some()
			.then(|| {
				config.read.and_then(|read| {
					MultiRangeTier::new(MultiRangeConfig {
						resident_bytes: read.resident_bytes,
						shards: read.shards,
						..MultiRangeConfig::default()
					})
				})
			})
			.flatten();

		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		let (persistent, flush_engine, filter) = {
			let persistent_config = config.persistent.clone();
			let persistent = persistent_config.as_ref().map(|c| c.storage.clone());
			let filter = persistent.as_ref().map(|tier| {
				let storage = tier.sqlite_storage().clone();
				let actor = FilterActor::spawn(&config.spawner);
				actor.send(FilterMessage::Register {
					filter: storage.filter().handle(),
					source: Box::new(MultiCurrentKeySource::new(storage)),
					config: FilterConfig::default(),
				})
				.expect("multi current filter source could not be registered");
				actor
			});
			let flush_engine = match (persistent.as_ref(), persistent_config.as_ref()) {
				(Some(persistent_storage), Some(_)) => Some(Arc::new(
					FlushEngine::new(
						commit.clone(),
						persistent_storage.clone(),
						row_settings_provider.clone(),
						eviction_watermark.clone(),
						read.clone(),
						config.clock.clone(),
						config.event_bus.clone(),
					)
					.with_range(range.clone()),
				)),
				_ => None,
			};
			(persistent, flush_engine, filter)
		};

		#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
		let (persistent, flush_engine, filter): (
			Option<MultiPersistentTier>,
			Option<Arc<FlushEngine>>,
			Option<ActorRef<FilterMessage>>,
		) = {
			let _ = config.persistent;
			(None, None, None)
		};

		let read = persistent.as_ref().and(read);
		let range = persistent.as_ref().and(range);

		Ok(Self(Arc::new(StandardMultiStoreInner {
			commit,
			persistent,
			read,
			range,
			flush_engine,
			row_settings_provider,
			eviction_watermark,
			event_bus: config.event_bus,
			persistent_probes: AtomicU64::new(0),
			persistent_absent: AtomicU64::new(0),
			filter,
		})))
	}

	pub fn flush_engine(&self) -> Option<Arc<FlushEngine>> {
		self.flush_engine.clone()
	}

	pub fn insert_read_key(&self, key: EncodedKey, version: CommitVersion, value: Option<CowVec<u8>>) {
		if let Some(range) = &self.range {
			range.insert(key.clone(), version, value.clone());
		}
		if let Some(read) = &self.read {
			read.insert(key, version, value);
		}
	}

	pub fn invalidate_read_key(&self, key: &EncodedKey) {
		if let Some(range) = &self.range {
			range.invalidate(key);
		}
		if let Some(read) = &self.read {
			read.invalidate(key);
		}
	}

	pub fn clear_read(&self) {
		if let Some(range) = &self.range {
			range.clear();
		}
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
		let mut out = self.read.as_ref().map(|read| read.shard_metrics()).unwrap_or_default();
		let Some(range) = &self.range else {
			return out;
		};
		let shards = range.shard_metrics();
		let serves = range.serve_metrics();
		let complete = range.complete_partitions();
		reifydb_assertions! {
			assert_eq!(
				(out.len(), out.len(), out.len()),
				(shards.len(), serves.len(), complete.len()),
				"both read tiers must shard alike, or a range counter is dropped and every materialize on the shards past the shorter tier reports zero"
			);
		}
		for (((target, source), serve), complete) in out.iter_mut().zip(shards).zip(serves).zip(complete) {
			target.state.complete_pages += complete;
			target.reads.point_hits += source.counters.point_hits;
			target.reads.point_misses += source.counters.point_misses;
			target.reads.range_served += source.counters.hits;
			target.reads.range_gaps += source.counters.misses;
			target.coverage.materializes += source.counters.materializes;
			target.coverage.materializes_refused += source.counters.materializes_refused;
			target.coverage.served += serve.served;
			target.coverage.rows += serve.rows;
			target.coverage.head_advances += serve.head_advances;
			target.pages.pages_evicted += source.counters.evictions;
		}
		out
	}

	pub fn commit_metrics(&self) -> MultiCommitMetrics {
		self.commit.metrics()
	}

	pub fn persistent_page_cache_metrics(&self) -> Option<PageCacheMetrics> {
		self.persistent.as_ref().map(MultiPersistentTier::page_cache_metrics)
	}

	pub fn persistent_filter_metrics(&self) -> Option<FilterMetrics> {
		self.persistent.as_ref().map(|tier| tier.filter().metrics())
	}

	pub fn persistent_probe_metrics(&self) -> Option<MultiPersistentProbeMetrics> {
		self.persistent.as_ref().map(|_| MultiPersistentProbeMetrics {
			persistent_probes: Count::new(self.persistent_probes.load(Ordering::Relaxed)),
			persistent_absent: Count::new(self.persistent_absent.load(Ordering::Relaxed)),
		})
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
		if let Some(filter) = self.filter.as_ref() {
			let _ = filter.send(FilterMessage::Shutdown);
		}
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
