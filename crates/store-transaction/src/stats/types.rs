// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Core types for storage statistics.

use std::ops::AddAssign;

use reifydb_core::interface::{FlowNodeId, SourceId};

use super::accumulator::StorageStatsDelta;

/// Identifies which storage tier data resides in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tier {
	Hot,
	Warm,
	Cold,
}

/// Storage statistics for a single object or aggregate.
///
/// Tracks both "current" (latest MVCC version) and "historical" (older versions)
/// separately to understand storage overhead from versioning.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageStats {
	/// Total bytes used by keys for latest versions
	pub current_key_bytes: u64,
	/// Total bytes used by values for latest versions
	pub current_value_bytes: u64,
	/// Total bytes used by keys for older MVCC versions
	pub historical_key_bytes: u64,
	/// Total bytes used by values for older MVCC versions
	pub historical_value_bytes: u64,
	/// Number of current (latest version) entries
	pub current_count: u64,
	/// Number of historical (older version) entries
	pub historical_count: u64,
	/// Total CDC key bytes attributed to this object
	pub cdc_key_bytes: u64,
	/// Total CDC value bytes attributed to this object
	pub cdc_value_bytes: u64,
	/// Number of CDC entries attributed to this object
	pub cdc_count: u64,
}

impl StorageStats {
	/// Create new empty stats.
	pub fn new() -> Self {
		Self::default()
	}

	/// Total bytes across current and historical data.
	pub fn total_bytes(&self) -> u64 {
		self.current_key_bytes
			+ self.current_value_bytes
			+ self.historical_key_bytes
			+ self.historical_value_bytes
	}

	/// Total bytes for current (latest) data only.
	pub fn current_bytes(&self) -> u64 {
		self.current_key_bytes + self.current_value_bytes
	}

	/// Total bytes for historical (older) data only.
	pub fn historical_bytes(&self) -> u64 {
		self.historical_key_bytes + self.historical_value_bytes
	}

	/// Total CDC bytes for this object.
	pub fn cdc_total_bytes(&self) -> u64 {
		self.cdc_key_bytes + self.cdc_value_bytes
	}

	/// Total entry count across current and historical.
	pub fn total_count(&self) -> u64 {
		self.current_count + self.historical_count
	}

	/// Apply a delta to this stats object atomically.
	///
	/// Used by StatsAccumulator to apply collected changes in one operation.
	/// Uses saturating arithmetic to prevent underflow.
	pub fn apply_delta(&mut self, delta: &StorageStatsDelta) {
		self.current_count = if delta.current_count_delta >= 0 {
			self.current_count.saturating_add(delta.current_count_delta as u64)
		} else {
			self.current_count.saturating_sub((-delta.current_count_delta) as u64)
		};

		self.current_key_bytes = if delta.current_key_bytes_delta >= 0 {
			self.current_key_bytes.saturating_add(delta.current_key_bytes_delta as u64)
		} else {
			self.current_key_bytes.saturating_sub((-delta.current_key_bytes_delta) as u64)
		};

		self.current_value_bytes = if delta.current_value_bytes_delta >= 0 {
			self.current_value_bytes.saturating_add(delta.current_value_bytes_delta as u64)
		} else {
			self.current_value_bytes.saturating_sub((-delta.current_value_bytes_delta) as u64)
		};

		self.historical_count = if delta.historical_count_delta >= 0 {
			self.historical_count.saturating_add(delta.historical_count_delta as u64)
		} else {
			self.historical_count.saturating_sub((-delta.historical_count_delta) as u64)
		};

		self.historical_key_bytes = if delta.historical_key_bytes_delta >= 0 {
			self.historical_key_bytes.saturating_add(delta.historical_key_bytes_delta as u64)
		} else {
			self.historical_key_bytes.saturating_sub((-delta.historical_key_bytes_delta) as u64)
		};

		self.historical_value_bytes = if delta.historical_value_bytes_delta >= 0 {
			self.historical_value_bytes.saturating_add(delta.historical_value_bytes_delta as u64)
		} else {
			self.historical_value_bytes.saturating_sub((-delta.historical_value_bytes_delta) as u64)
		};

		self.cdc_count = if delta.cdc_count_delta >= 0 {
			self.cdc_count.saturating_add(delta.cdc_count_delta as u64)
		} else {
			self.cdc_count.saturating_sub((-delta.cdc_count_delta) as u64)
		};

		self.cdc_key_bytes = if delta.cdc_key_bytes_delta >= 0 {
			self.cdc_key_bytes.saturating_add(delta.cdc_key_bytes_delta as u64)
		} else {
			self.cdc_key_bytes.saturating_sub((-delta.cdc_key_bytes_delta) as u64)
		};

		self.cdc_value_bytes = if delta.cdc_value_bytes_delta >= 0 {
			self.cdc_value_bytes.saturating_add(delta.cdc_value_bytes_delta as u64)
		} else {
			self.cdc_value_bytes.saturating_sub((-delta.cdc_value_bytes_delta) as u64)
		};
	}
}

impl AddAssign for StorageStats {
	fn add_assign(&mut self, rhs: Self) {
		self.current_key_bytes += rhs.current_key_bytes;
		self.current_value_bytes += rhs.current_value_bytes;
		self.historical_key_bytes += rhs.historical_key_bytes;
		self.historical_value_bytes += rhs.historical_value_bytes;
		self.current_count += rhs.current_count;
		self.historical_count += rhs.historical_count;
		self.cdc_key_bytes += rhs.cdc_key_bytes;
		self.cdc_value_bytes += rhs.cdc_value_bytes;
		self.cdc_count += rhs.cdc_count;
	}
}

/// Storage statistics broken down by tier.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TierStats {
	pub hot: StorageStats,
	pub warm: StorageStats,
	pub cold: StorageStats,
}

impl TierStats {
	/// Create new empty tier stats.
	pub fn new() -> Self {
		Self::default()
	}

	/// Get stats for a specific tier.
	pub fn get(&self, tier: Tier) -> &StorageStats {
		match tier {
			Tier::Hot => &self.hot,
			Tier::Warm => &self.warm,
			Tier::Cold => &self.cold,
		}
	}

	/// Get mutable stats for a specific tier.
	pub fn get_mut(&mut self, tier: Tier) -> &mut StorageStats {
		match tier {
			Tier::Hot => &mut self.hot,
			Tier::Warm => &mut self.warm,
			Tier::Cold => &mut self.cold,
		}
	}

	/// Total bytes across all tiers.
	pub fn total_bytes(&self) -> u64 {
		self.hot.total_bytes() + self.warm.total_bytes() + self.cold.total_bytes()
	}

	/// Total current bytes across all tiers.
	pub fn current_bytes(&self) -> u64 {
		self.hot.current_bytes() + self.warm.current_bytes() + self.cold.current_bytes()
	}

	/// Total historical bytes across all tiers.
	pub fn historical_bytes(&self) -> u64 {
		self.hot.historical_bytes() + self.warm.historical_bytes() + self.cold.historical_bytes()
	}
}

impl AddAssign for TierStats {
	fn add_assign(&mut self, rhs: Self) {
		self.hot += rhs.hot;
		self.warm += rhs.warm;
		self.cold += rhs.cold;
	}
}

/// Identifier for tracking per-object storage statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectId {
	/// Table, view, or flow source
	Source(SourceId),
	/// Flow operator node
	FlowNode(FlowNodeId),
	/// System metadata (sequences, versions, etc.)
	System,
}

#[cfg(test)]
mod tests {
	use super::{super::accumulator::StorageStatsDelta, *};

	#[test]
	fn test_apply_delta_insert() {
		let mut stats = StorageStats::new();
		let mut delta = StorageStatsDelta::default();
		delta.add_insert(10, 100);

		stats.apply_delta(&delta);

		assert_eq!(stats.current_key_bytes, 10);
		assert_eq!(stats.current_value_bytes, 100);
		assert_eq!(stats.current_count, 1);
		assert_eq!(stats.historical_key_bytes, 0);
		assert_eq!(stats.historical_count, 0);
		assert_eq!(stats.total_bytes(), 110);
	}

	#[test]
	fn test_apply_delta_update() {
		let mut stats = StorageStats::new();
		stats.current_key_bytes = 10;
		stats.current_value_bytes = 100;
		stats.current_count = 1;

		let mut delta = StorageStatsDelta::default();
		delta.add_update(10, 150, 10, 100);

		stats.apply_delta(&delta);

		// Current should have new value
		assert_eq!(stats.current_key_bytes, 10);
		assert_eq!(stats.current_value_bytes, 150);
		assert_eq!(stats.current_count, 1);

		// Historical should have old value
		assert_eq!(stats.historical_key_bytes, 10);
		assert_eq!(stats.historical_value_bytes, 100);
		assert_eq!(stats.historical_count, 1);

		assert_eq!(stats.total_bytes(), 270); // 10+150 + 10+100
	}

	#[test]
	fn test_apply_delta_delete() {
		let mut stats = StorageStats::new();
		stats.current_key_bytes = 10;
		stats.current_value_bytes = 100;
		stats.current_count = 1;

		let mut delta = StorageStatsDelta::default();
		delta.add_delete(10, 10, 100);

		stats.apply_delta(&delta);

		// Current should be empty
		assert_eq!(stats.current_key_bytes, 0);
		assert_eq!(stats.current_value_bytes, 0);
		assert_eq!(stats.current_count, 0);

		// Historical should have old value + tombstone key
		assert_eq!(stats.historical_key_bytes, 20); // old key + tombstone key
		assert_eq!(stats.historical_value_bytes, 100);
		assert_eq!(stats.historical_count, 2); // old entry + tombstone
	}

	#[test]
	fn test_apply_delta_drop() {
		let mut stats = StorageStats::new();
		stats.historical_key_bytes = 10;
		stats.historical_value_bytes = 100;
		stats.historical_count = 1;

		let mut delta = StorageStatsDelta::default();
		delta.add_drop(10, 100);

		stats.apply_delta(&delta);

		assert_eq!(stats.historical_key_bytes, 0);
		assert_eq!(stats.historical_value_bytes, 0);
		assert_eq!(stats.historical_count, 0);
	}

	#[test]
	fn test_apply_delta_cdc() {
		let mut stats = StorageStats::new();
		let mut delta = StorageStatsDelta::default();
		delta.add_cdc(100, 500, 5);

		stats.apply_delta(&delta);

		assert_eq!(stats.cdc_key_bytes, 100);
		assert_eq!(stats.cdc_value_bytes, 500);
		assert_eq!(stats.cdc_count, 5);
		// CDC shouldn't affect current or historical
		assert_eq!(stats.current_count, 0);
		assert_eq!(stats.historical_count, 0);
	}

	#[test]
	fn test_tier_stats() {
		let mut tier_stats = TierStats::new();

		let mut delta = StorageStatsDelta::default();
		delta.add_insert(10, 100);
		tier_stats.get_mut(Tier::Hot).apply_delta(&delta);

		let mut delta = StorageStatsDelta::default();
		delta.add_insert(20, 200);
		tier_stats.get_mut(Tier::Warm).apply_delta(&delta);

		let mut delta = StorageStatsDelta::default();
		delta.add_insert(30, 300);
		tier_stats.get_mut(Tier::Cold).apply_delta(&delta);

		assert_eq!(tier_stats.hot.total_bytes(), 110);
		assert_eq!(tier_stats.warm.total_bytes(), 220);
		assert_eq!(tier_stats.cold.total_bytes(), 330);
		assert_eq!(tier_stats.total_bytes(), 660);
	}
}
