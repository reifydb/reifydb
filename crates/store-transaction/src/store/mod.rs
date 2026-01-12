// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{ops::Deref, sync::Arc, time::Duration};

use parking_lot::Mutex;
use tracing::instrument;
use reifydb_core::CommitVersion;
use reifydb_core::event::EventBus;
use crate::{
	HotConfig,
	cold::ColdStorage,
	config::TransactionStoreConfig,
	hot::HotStorage,
	stats::{StorageTracker, StorageTrackerConfig, StatsWorker, StatsWorkerConfig},
	warm::WarmStorage,
};

mod cdc;
mod drop;
pub mod worker;
mod multi;
pub mod router;
mod single;
pub mod version;

pub use worker::{DropWorker, DropWorkerConfig, DropStatsCallback};

#[derive(Clone)]
pub struct StandardTransactionStore(Arc<StandardTransactionStoreInner>);

pub struct StandardTransactionStoreInner {
	pub(crate) hot: Option<HotStorage>,
	pub(crate) warm: Option<WarmStorage>,
	pub(crate) cold: Option<ColdStorage>,
	pub(crate) stats_tracker: StorageTracker,
	/// Background stats worker.
	pub(crate) stats_worker: Arc<StatsWorker>,
	/// Background drop worker.
	pub(crate) drop_worker: Arc<Mutex<DropWorker>>,
}

impl StandardTransactionStore {
	#[instrument(name = "store::new", level = "info", skip(config), fields(
		has_hot = config.hot.is_some(),
		has_warm = config.warm.is_some(),
		has_cold = config.cold.is_some(),
	))]
	pub fn new(config: TransactionStoreConfig) -> crate::Result<Self> {
		let hot = config.hot.map(|c| c.storage);
		// TODO: warm and cold are placeholders for now
		let warm = None;
		let cold = None;
		let _ = config.warm;
		let _ = config.cold;

		let tracker_config = StorageTrackerConfig {
			checkpoint_interval: config.stats.checkpoint_interval,
		};

		// Create a new stats tracker
		let stats_tracker = StorageTracker::new(tracker_config);

		// Create background workers (requires hot tier)
		let storage = hot.as_ref().expect("hot tier is required");

		// Stats worker
		let stats_config = StatsWorkerConfig {
			channel_capacity: config.stats.worker_channel_capacity,
			checkpoint_interval: config.stats.checkpoint_interval,
		};
		let stats_worker = Arc::new(StatsWorker::new(
			stats_config,
			stats_tracker.clone(),
			storage.clone(),
			config.event_bus,
		));

		// Drop worker with stats callback
		let drop_config = DropWorkerConfig::default();
		let drop_worker = DropWorker::new(drop_config, storage.clone(), StatsWorkerCallback {
			worker: stats_worker.clone(),
			tracker: stats_tracker.clone(),
		});

		Ok(Self(Arc::new(StandardTransactionStoreInner {
			hot,
			warm,
			cold,
			stats_tracker,
			stats_worker,
			drop_worker: Arc::new(Mutex::new(drop_worker)),
		})))
	}

	/// Get access to the storage tracker.
	pub fn stats_tracker(&self) -> &StorageTracker {
		&self.stats_tracker
	}
}

impl Deref for StandardTransactionStore {
	type Target = StandardTransactionStoreInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl StandardTransactionStore {
	pub fn testing_memory() -> Self {
		Self::new(TransactionStoreConfig {
			hot: Some(HotConfig {
				storage: HotStorage::memory(),
				retention_period: Duration::from_millis(100),
			}),
			warm: None,
			cold: None,
			retention: Default::default(),
			merge_config: Default::default(),
			stats: Default::default(),
			event_bus: EventBus::new(),
		})
		.unwrap()
	}
}

/// Callback for drop worker to record stats via the stats worker.
pub(crate) struct StatsWorkerCallback {
	pub(crate) worker: Arc<StatsWorker>,
	#[allow(dead_code)]
	pub(crate) tracker: StorageTracker,
}

impl DropStatsCallback for StatsWorkerCallback {
	fn record_drop(
		&self,
		tier: crate::stats::Tier,
		key: &[u8],
		versioned_key_bytes: u64,
		value_bytes: u64,
		version: CommitVersion,
	) {
		self.worker.record_drop(tier, key, versioned_key_bytes, value_bytes, version);
	}
}
