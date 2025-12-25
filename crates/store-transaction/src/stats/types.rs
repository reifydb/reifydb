// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Core types for storage statistics.

use std::ops::AddAssign;

use reifydb_core::interface::{FlowNodeId, PrimitiveId};

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

	/// Record CDC bytes for a change attributed to this object.
	pub fn record_cdc(&mut self, key_bytes: u64, value_bytes: u64, count: u64) {
		self.cdc_key_bytes += key_bytes;
		self.cdc_value_bytes += value_bytes;
		self.cdc_count += count;
	}

	/// Record a new entry (insert of a key that didn't exist).
	pub fn record_insert(&mut self, key_bytes: u64, value_bytes: u64) {
		self.current_key_bytes += key_bytes;
		self.current_value_bytes += value_bytes;
		self.current_count += 1;
	}

	/// Record an update (new version of existing key).
	///
	/// The old version moves from current to historical.
	pub fn record_update(
		&mut self,
		new_key_bytes: u64,
		new_value_bytes: u64,
		old_key_bytes: u64,
		old_value_bytes: u64,
	) {
		// Move old version from current to historical
		self.current_key_bytes = self.current_key_bytes.saturating_sub(old_key_bytes);
		self.current_value_bytes = self.current_value_bytes.saturating_sub(old_value_bytes);
		self.current_count = self.current_count.saturating_sub(1);

		self.historical_key_bytes += old_key_bytes;
		self.historical_value_bytes += old_value_bytes;
		self.historical_count += 1;

		// Add new version to current
		self.current_key_bytes += new_key_bytes;
		self.current_value_bytes += new_value_bytes;
		self.current_count += 1;
	}

	/// Record a delete (tombstone for existing key).
	///
	/// The old version moves to historical, tombstone key added to historical.
	pub fn record_delete(&mut self, tombstone_key_bytes: u64, old_key_bytes: u64, old_value_bytes: u64) {
		// Move old version from current to historical
		self.current_key_bytes = self.current_key_bytes.saturating_sub(old_key_bytes);
		self.current_value_bytes = self.current_value_bytes.saturating_sub(old_value_bytes);
		self.current_count = self.current_count.saturating_sub(1);

		self.historical_key_bytes += old_key_bytes;
		self.historical_value_bytes += old_value_bytes;
		self.historical_count += 1;

		// Tombstone goes to historical (key only, no value)
		self.historical_key_bytes += tombstone_key_bytes;
		self.historical_count += 1;
	}

	/// Record a drop (physical removal of a historical version entry).
	///
	/// Unlike delete, drop doesn't create tombstones - it physically removes
	/// entries from storage. Used for MVCC cleanup of old versions.
	pub fn record_drop(&mut self, key_bytes: u64, value_bytes: u64) {
		self.historical_key_bytes = self.historical_key_bytes.saturating_sub(key_bytes);
		self.historical_value_bytes = self.historical_value_bytes.saturating_sub(value_bytes);
		self.historical_count = self.historical_count.saturating_sub(1);
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
	Source(PrimitiveId),
	/// Flow operator node
	FlowNode(FlowNodeId),
	/// System metadata (sequences, versions, etc.)
	System,
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_storage_stats_insert() {
		let mut stats = StorageStats::new();
		stats.record_insert(10, 100);

		assert_eq!(stats.current_key_bytes, 10);
		assert_eq!(stats.current_value_bytes, 100);
		assert_eq!(stats.current_count, 1);
		assert_eq!(stats.historical_key_bytes, 0);
		assert_eq!(stats.historical_count, 0);
		assert_eq!(stats.total_bytes(), 110);
	}

	#[test]
	fn test_storage_stats_update() {
		let mut stats = StorageStats::new();
		stats.record_insert(10, 100);
		stats.record_update(10, 150, 10, 100);

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
	fn test_storage_stats_delete() {
		let mut stats = StorageStats::new();
		stats.record_insert(10, 100);
		stats.record_delete(10, 10, 100);

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
	fn test_tier_stats() {
		let mut tier_stats = TierStats::new();
		tier_stats.get_mut(Tier::Hot).record_insert(10, 100);
		tier_stats.get_mut(Tier::Warm).record_insert(20, 200);
		tier_stats.get_mut(Tier::Cold).record_insert(30, 300);

		assert_eq!(tier_stats.hot.total_bytes(), 110);
		assert_eq!(tier_stats.warm.total_bytes(), 220);
		assert_eq!(tier_stats.cold.total_bytes(), 330);
		assert_eq!(tier_stats.total_bytes(), 660);
	}
}
