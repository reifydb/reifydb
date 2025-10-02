// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	sync::{Arc, Mutex},
	time::{Duration, SystemTime},
};

use reifydb_core::{
	CommitVersion,
	interface::{ColumnStatistics, ColumnStore},
	value::column::{ColumnData, CompressedColumn},
};
use reifydb_type::Result;

use crate::{backend::Backend, config::ColumnStoreConfig, statistics::merge};

#[derive(Clone)]
pub struct StandardColumnStore {
	pub(crate) hot: Option<Backend>,
	pub(crate) warm: Option<Backend>,
	pub(crate) cold: Option<Backend>,
	config: ColumnStoreConfig,
	tier_state: Arc<Mutex<TierState>>,
}

struct TierState {
	_hot_evicted_version: CommitVersion,  // Last version evicted from hot to warm
	_warm_evicted_version: CommitVersion, // Last version evicted from warm to cold
	_last_eviction_time: SystemTime,
}

impl Default for TierState {
	fn default() -> Self {
		Self {
			_hot_evicted_version: 0,
			_warm_evicted_version: 0,
			_last_eviction_time: SystemTime::now(),
		}
	}
}

impl StandardColumnStore {
	pub fn new(config: ColumnStoreConfig) -> Result<Self> {
		Ok(Self {
			hot: config.hot.as_ref().map(|c| c.backend.clone()),
			warm: config.warm.as_ref().map(|c| c.backend.clone()),
			cold: config.cold.as_ref().map(|c| c.backend.clone()),
			config,
			tier_state: Arc::new(Mutex::new(TierState::default())),
		})
	}

	/// Determine which tier to write to based on age and tier availability
	fn select_write_tier(&self, _version: CommitVersion) -> Option<&Backend> {
		// For now, always write to hot tier if available, fallback to warm, then cold
		self.hot.as_ref().or(self.warm.as_ref()).or(self.cold.as_ref())
	}

	/// Search across tiers for data, starting with hot (most recent)
	fn search_tiers<T, F>(&self, mut search_fn: F) -> Option<T>
	where
		F: FnMut(&Backend) -> Option<T>,
	{
		// Search hot tier first
		if let Some(hot) = &self.hot {
			if let Some(result) = search_fn(hot) {
				return Some(result);
			}
		}

		// Search warm tier
		if let Some(warm) = &self.warm {
			if let Some(result) = search_fn(warm) {
				return Some(result);
			}
		}

		// Search cold tier
		if let Some(cold) = &self.cold {
			if let Some(result) = search_fn(cold) {
				return Some(result);
			}
		}

		None
	}

	/// Collect statistics from all tiers and merge them
	fn collect_statistics(&self, column_index: usize) -> Option<ColumnStatistics> {
		let mut all_stats = Vec::new();

		if let Some(hot) = &self.hot {
			if let Some(stats) = hot.statistics(column_index) {
				all_stats.push(stats);
			}
		}

		if let Some(warm) = &self.warm {
			if let Some(stats) = warm.statistics(column_index) {
				all_stats.push(stats);
			}
		}

		if let Some(cold) = &self.cold {
			if let Some(stats) = cold.statistics(column_index) {
				all_stats.push(stats);
			}
		}

		merge(&all_stats)
	}

	/// Check if data should be evicted based on retention policies
	pub fn should_evict(&self) -> bool {
		let state = self.tier_state.lock().unwrap();
		let now = SystemTime::now();

		// Simple time-based eviction check
		if let Ok(duration) = now.duration_since(state._last_eviction_time) {
			duration > Duration::from_secs(300) // Check every 5 minutes
		} else {
			false
		}
	}

	/// Evict data from hot to warm, and warm to cold based on retention policies
	pub fn evict_data(&self) -> Result<usize> {
		// TODO: Implement actual eviction logic
		// This would involve:
		// 1. Identifying data older than retention periods
		// 2. Moving data from hot to warm tier
		// 3. Moving data from warm to cold tier
		// 4. Updating tier state

		Ok(0) // Return number of evicted partitions
	}
}

impl ColumnStore for StandardColumnStore {
	fn insert(&self, version: CommitVersion, columns: Vec<CompressedColumn>) -> Result<()> {
		if let Some(backend) = self.select_write_tier(version) {
			backend.insert(version, columns)
		} else {
			reifydb_type::err!(reifydb_type::diagnostic::internal_with_context(
				"No available backend for column storage",
				file!(),
				line!(),
				column!(),
				module_path!(),
				module_path!()
			))
		}
	}

	fn scan(&self, version: CommitVersion, column_indices: &[usize]) -> Result<Vec<ColumnData>> {
		Ok(self.search_tiers(|backend| backend.scan(version, column_indices).ok()).unwrap_or_else(|| vec![]))
	}

	fn statistics(&self, column_index: usize) -> Option<ColumnStatistics> {
		self.collect_statistics(column_index)
	}

	fn partition_count(&self) -> usize {
		let mut count = 0;

		if let Some(hot) = &self.hot {
			count += hot.partition_count();
		}

		if let Some(warm) = &self.warm {
			count += warm.partition_count();
		}

		if let Some(cold) = &self.cold {
			count += cold.partition_count();
		}

		count
	}

	fn compressed_size(&self) -> usize {
		let mut size = 0;

		if let Some(hot) = &self.hot {
			size += hot.compressed_size();
		}

		if let Some(warm) = &self.warm {
			size += warm.compressed_size();
		}

		if let Some(cold) = &self.cold {
			size += cold.compressed_size();
		}

		size
	}

	fn uncompressed_size(&self) -> usize {
		let mut size = 0;

		if let Some(hot) = &self.hot {
			size += hot.uncompressed_size();
		}

		if let Some(warm) = &self.warm {
			size += warm.uncompressed_size();
		}

		if let Some(cold) = &self.cold {
			size += cold.uncompressed_size();
		}

		size
	}
}
