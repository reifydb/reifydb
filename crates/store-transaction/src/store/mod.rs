// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{ops::Deref, sync::Arc, time::Duration};

use tracing::instrument;

use crate::{
	HotConfig,
	cold::ColdStorage,
	config::TransactionStoreConfig,
	hot::HotStorage,
	stats::{StorageTracker, StorageTrackerConfig},
	warm::WarmStorage,
};

mod cdc;
mod drop;
mod multi;
pub mod router;
mod single;
pub mod version;

#[derive(Clone)]
pub struct StandardTransactionStore(Arc<StandardTransactionStoreInner>);

pub struct StandardTransactionStoreInner {
	pub(crate) hot: Option<HotStorage>,
	pub(crate) warm: Option<WarmStorage>,
	pub(crate) cold: Option<ColdStorage>,
	pub(crate) stats_tracker: StorageTracker,
}

impl StandardTransactionStore {
	#[instrument(name = "store::new", level = "info", skip(config), fields(
		has_hot = config.hot.is_some(),
		has_warm = config.warm.is_some(),
		has_cold = config.cold.is_some()
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

		// Create a new stats tracker. Use `new_async` or `restore_async`
		// to restore from storage if needed.
		let stats_tracker = StorageTracker::new(tracker_config);

		Ok(Self(Arc::new(StandardTransactionStoreInner {
			hot,
			warm,
			cold,
			stats_tracker,
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
		})
		.unwrap()
	}
}
