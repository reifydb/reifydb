// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod multi;
mod single;

use std::{
	sync::{Arc, Mutex},
	time::Duration,
};

use reifydb_core::{
	CommitVersion,
	interface::{MultiVersionValues, RowStore},
};

use crate::{backend::Backend, config::RowStoreConfig};

#[derive(Clone)]
pub struct StandardRowStore {
	pub(crate) hot: Option<Backend>,
	pub(crate) warm: Option<Backend>,
	pub(crate) cold: Option<Backend>,
	merge_state: Arc<Mutex<MergeState>>,
}

#[derive(Default)]
struct MergeState {
	_hot_evicted_version: CommitVersion,  // Last version evicted from hot to warm
	_warm_evicted_version: CommitVersion, // Last version evicted from warm to cold
	_last_merge_version: CommitVersion,
}

impl StandardRowStore {
	pub fn new(config: RowStoreConfig) -> crate::Result<Self> {
		Ok(Self {
			hot: config.hot.map(|c| c.backend),
			warm: config.warm.map(|c| c.backend),
			cold: config.cold.as_ref().map(|c| c.backend.clone()),
			merge_state: Arc::new(Mutex::new(MergeState::default())),
		})
	}
}

impl RowStore for StandardRowStore {
	fn last_merge_version(&self) -> CommitVersion {
		let state = self.merge_state.lock().unwrap();
		state._last_merge_version
	}

	fn pending_row_count(&self) -> usize {
		let mut count = 0;
		if let Some(hot) = &self.hot {
			count += hot.count();
		}
		if let Some(warm) = &self.warm {
			count += warm.count();
		}
		// Don't count cold as it's not pending merge
		count
	}

	fn should_merge(&self) -> bool {
		todo!()
	}

	fn get_merge_batch(&self, _limit: usize) -> crate::Result<Vec<MultiVersionValues>> {
		todo!("Implement get_merge_batch from all tiers")
	}

	fn mark_merged_and_evict(&self, _up_to_version: CommitVersion) -> crate::Result<usize> {
		todo!("Implement mark_merged_and_evict across backends")
	}

	fn verify_safe_to_evict(&self, _up_to_version: CommitVersion) -> crate::Result<bool> {
		todo!("Implement verify_safe_to_evict")
	}

	fn retention_period(&self) -> Duration {
		todo!()
	}
}
