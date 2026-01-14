// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{ops::Deref, sync::Arc, time::Duration};

use parking_lot::Mutex;
use tracing::instrument;
use reifydb_core::{CommitVersion, EncodedKey};
use reifydb_core::event::{EventBus, StorageDrop, StorageStatsRecordedEvent};
use reifydb_core::runtime::ComputePool;
use crate::{
	HotConfig,
	cold::ColdStorage,
	config::TransactionStoreConfig,
	hot::HotStorage,
	warm::WarmStorage,
};

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
	/// Event bus for emitting storage stats events.
	pub(crate) event_bus: EventBus,
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

		// Create background workers (requires hot tier)
		let storage = hot.as_ref().expect("hot tier is required");
		let event_bus = config.event_bus;

		// Drop worker with event bus callback
		let drop_config = DropWorkerConfig::default();
		let drop_worker = DropWorker::new(drop_config, storage.clone(), EventBusStatsCallback {
			event_bus: event_bus.clone(),
		});

		Ok(Self(Arc::new(StandardTransactionStoreInner {
			hot,
			warm,
			cold,
			event_bus,
			drop_worker: Arc::new(Mutex::new(drop_worker)),
		})))
	}

	/// Get access to the hot storage tier.
	///
	/// Returns `None` if the hot tier is not configured.
	pub fn hot(&self) -> Option<&HotStorage> {
		self.hot.as_ref()
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
		Self::testing_memory_with_eventbus(EventBus::new())
	}

	/// Create a test store with a custom EventBus.
	///
	/// Use this when you need the store to emit events on a specific EventBus,
	/// e.g., for testing metrics integration.
	pub fn testing_memory_with_eventbus(event_bus: EventBus) -> Self {
		Self::new(TransactionStoreConfig {
			hot: Some(HotConfig {
				storage: HotStorage::memory(ComputePool::new(1,1)),
				retention_period: Duration::from_millis(100),
			}),
			warm: None,
			cold: None,
			retention: Default::default(),
			merge_config: Default::default(),
			stats: Default::default(),
			event_bus,
		})
		.unwrap()
	}
}

/// Callback for drop worker to emit storage stats events via the event bus.
pub(crate) struct EventBusStatsCallback {
	pub(crate) event_bus: EventBus,
}

impl DropStatsCallback for EventBusStatsCallback {
	fn record_drop(
		&self,
		key: EncodedKey,
		_versioned_key_bytes: u64,
		value_bytes: u64,
		version: CommitVersion,
	) {
		self.event_bus.emit(StorageStatsRecordedEvent {
			writes: Vec::new(),
			deletes: Vec::new(),
			drops: vec![StorageDrop {
				key,
				value_bytes,
			}],
			version,
		});
	}
}
