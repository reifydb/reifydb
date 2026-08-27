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
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use reifydb_store::tier::commit::CommitTier;
use reifydb_value::{count::Count, util::cowvec::CowVec};
use tracing::instrument;

#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::filter::source::MultiCurrentKeySource;
use crate::{
	CommitBufferConfig,
	config::MultiStoreConfig,
	flush::ObjectPersistence,
	tier::{
		commit::buffer::{MultiCommitBufferTier, MultiCommitMetrics},
		persistent::MultiPersistentTier,
		point::{MultiPointShardMetrics, MultiPointTier},
		range::{MultiRangeShardMetrics, MultiRangeTier},
	},
};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::{
	config::PersistentConfig,
	tier::{
		commit::domain::{MultiCommitTier, MultiState, commit_config},
		point::MultiPointConfig,
		range::MultiRangeConfig,
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
	pub(crate) point: Option<MultiPointTier>,
	pub(crate) range: Option<MultiRangeTier>,

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	#[allow(dead_code)]
	pub(crate) commit_tier: Option<MultiCommitTier>,
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

		let point = config.persistent.is_some().then(|| config.point.and_then(MultiPointTier::new)).flatten();

		let range = config.persistent.is_some().then(|| config.range.and_then(MultiRangeTier::new)).flatten();

		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		let (persistent, commit_tier, filter) = {
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
			let commit_tier = match (persistent.as_ref(), persistent_config.as_ref()) {
				(Some(persistent_storage), Some(_)) => CommitTier::new(commit_config(), |_budget| {
					MultiState::new(
						commit.clone(),
						persistent_storage.clone(),
						row_settings_provider.clone(),
						eviction_watermark.clone(),
						config.event_bus.clone(),
					)
					.with_point(point.clone())
					.with_range(range.clone())
				}),
				_ => None,
			};
			(persistent, commit_tier, filter)
		};

		#[cfg(not(all(feature = "sqlite", not(target_arch = "wasm32"))))]
		let (persistent, filter): (Option<MultiPersistentTier>, Option<ActorRef<FilterMessage>>) = {
			let _ = config.persistent;
			(None, None)
		};

		let point = persistent.as_ref().and(point);
		let range = persistent.as_ref().and(range);

		Ok(Self(Arc::new(StandardMultiStoreInner {
			commit,
			persistent,
			point,
			range,
			#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
			commit_tier,
			row_settings_provider,
			eviction_watermark,
			event_bus: config.event_bus,
			persistent_probes: AtomicU64::new(0),
			persistent_absent: AtomicU64::new(0),
			filter,
		})))
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn commit_tier(&self) -> Option<MultiCommitTier> {
		self.commit_tier.clone()
	}

	pub fn insert_read_key(&self, key: EncodedKey, version: CommitVersion, value: Option<CowVec<u8>>) {
		if let Some(range) = &self.range {
			range.insert(key.clone(), version, value.clone());
		}
		if let Some(point) = &self.point {
			point.insert(key, version, value);
		}
	}

	pub fn invalidate_read_key(&self, key: &EncodedKey) {
		if let Some(range) = &self.range {
			range.invalidate(key);
		}
		if let Some(point) = &self.point {
			point.invalidate(key);
		}
	}

	pub fn clear_read(&self) {
		if let Some(range) = &self.range {
			range.clear();
		}
		if let Some(point) = &self.point {
			point.clear();
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

	pub fn point_shard_metrics(&self) -> Vec<MultiPointShardMetrics> {
		self.point.as_ref().map(|point| point.shard_metrics()).unwrap_or_default()
	}

	pub fn range_shard_metrics(&self) -> Vec<MultiRangeShardMetrics> {
		self.range.as_ref().map(|range| range.full_shard_metrics()).unwrap_or_default()
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
		if let Some(tier) = self.commit_tier.as_ref() {
			self.event_bus.wait_for_completion();
			tier.flush_pending();
		}
	}

	pub fn flush_all_blocking(&self) {
		#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
		if let Some(tier) = self.commit_tier.as_ref() {
			self.event_bus.wait_for_completion();
			tier.flush_all();
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
			point: None,
			range: None,
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
			point: None,
			range: None,
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
		Self::testing_memory_with_persistent_sqlite_tiers(
			MultiPointConfig::testing(),
			MultiRangeConfig::testing(),
		)
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	pub fn testing_memory_with_persistent_sqlite_tiers(
		point: MultiPointConfig,
		range: MultiRangeConfig,
	) -> (Self, SqliteTempPathGuard) {
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
			point: Some(point),
			range: Some(range),
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
			point: Some(MultiPointConfig::testing()),
			range: Some(MultiRangeConfig::testing()),
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
