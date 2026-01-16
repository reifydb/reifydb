// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Multi-version (MVCC) storage statistics types, reader, and writer.
//!
//! This module contains everything related to multi-version storage metrics:
//! - `Tier` - storage tier (Hot, Warm, Cold)
//! - `MultiStorageStats` - MVCC statistics for a single object
//! - `TieredStorageStats` - statistics broken down by tier
//! - `MultiStorageOperation` - operation type for storage metrics processing
//! - `StorageStatsWriter` - single writer for storage statistics
//! - `StorageStatsReader` - read-only access to storage statistics

/// Size of the MVCC version suffix in bytes.
///
/// Each versioned key in storage has format: `[escaped_key][terminator][version]`
/// where terminator is 2 bytes and version is 8 bytes (big-endian u64).
const MVCC_VERSION_SIZE: usize = 10;

use std::ops::AddAssign;

use reifydb_core::{
	encoded::{encoded::EncodedValues, key::EncodedKey},
	interface::store::SingleVersionStore,
};
use reifydb_type::{Result, util::cowvec::CowVec};

use crate::{
	MetricId,
	encoding::{
		decode_storage_stats, decode_storage_stats_key, encode_storage_stats, encode_storage_stats_key,
		storage_stats_key_prefix,
	},
	parser::parse_id,
};

/// Identifies which storage tier data resides in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tier {
	Hot,
	Warm,
	Cold,
}

/// MVCC storage statistics for a single object or aggregate.
///
/// Tracks both "current" (latest MVCC version) and "historical" (older versions)
/// separately to understand storage overhead from versioning.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MultiStorageStats {
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
}

impl MultiStorageStats {
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

	/// Total entry count across current and historical.
	pub fn total_count(&self) -> u64 {
		self.current_count + self.historical_count
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

impl AddAssign for MultiStorageStats {
	fn add_assign(&mut self, rhs: Self) {
		self.current_key_bytes += rhs.current_key_bytes;
		self.current_value_bytes += rhs.current_value_bytes;
		self.historical_key_bytes += rhs.historical_key_bytes;
		self.historical_value_bytes += rhs.historical_value_bytes;
		self.current_count += rhs.current_count;
		self.historical_count += rhs.historical_count;
	}
}

/// Storage statistics broken down by tier.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TieredStorageStats {
	pub hot: MultiStorageStats,
	pub warm: MultiStorageStats,
	pub cold: MultiStorageStats,
}

impl TieredStorageStats {
	/// Create new empty tier stats.
	pub fn new() -> Self {
		Self::default()
	}

	/// Get stats for a specific tier.
	pub fn get(&self, tier: Tier) -> &MultiStorageStats {
		match tier {
			Tier::Hot => &self.hot,
			Tier::Warm => &self.warm,
			Tier::Cold => &self.cold,
		}
	}

	/// Get mutable stats for a specific tier.
	pub fn get_mut(&mut self, tier: Tier) -> &mut MultiStorageStats {
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

impl AddAssign for TieredStorageStats {
	fn add_assign(&mut self, rhs: Self) {
		self.hot += rhs.hot;
		self.warm += rhs.warm;
		self.cold += rhs.cold;
	}
}

/// Multi-version storage operation for metrics processing.
///
/// Represents a single storage operation to be recorded in statistics.
#[derive(Debug, Clone)]
pub enum MultiStorageOperation {
	/// Write operation (insert or update).
	Write {
		tier: Tier,
		key: EncodedKey,
		/// Size of the value being written
		value_bytes: u64,
	},
	/// Delete operation.
	Delete {
		tier: Tier,
		key: EncodedKey,
		/// Size of the value being deleted (for metrics tracking)
		value_bytes: u64,
	},
	/// Drop operation (MVCC cleanup).
	Drop {
		tier: Tier,
		key: EncodedKey,
		/// Size of the value being dropped
		value_bytes: u64,
	},
}

/// Writer for MVCC storage statistics (single writer only).
///
/// This should only be used by the MetricsWorker to maintain single-writer semantics.
pub struct StorageStatsWriter<S> {
	storage: S,
}

impl<S: SingleVersionStore> StorageStatsWriter<S> {
	/// Create a new writer.
	pub fn new(storage: S) -> Self {
		Self {
			storage,
		}
	}

	/// Record a write operation (insert or update).
	///
	/// If `pre_value_bytes` is provided, this is an update (previous version exists).
	/// Otherwise it's an insert. The key is always the same for updates.
	pub fn record_write(
		&mut self,
		tier: Tier,
		key: &[u8],
		value_bytes: u64,
		pre_value_bytes: Option<u64>,
	) -> Result<()> {
		let id = parse_id(key);
		// Account for MVCC version suffix in stored key size
		let key_bytes = (key.len() + MVCC_VERSION_SIZE) as u64;

		self.update(tier, id, |stats| {
			if let Some(old_val) = pre_value_bytes {
				stats.record_update(key_bytes, value_bytes, key_bytes, old_val);
			} else {
				stats.record_insert(key_bytes, value_bytes);
			}
		})
	}

	/// Record a delete operation.
	///
	/// If `pre_value_bytes` is provided, the old entry is moved to historical.
	/// Otherwise only a tombstone is recorded. The key is always the same.
	pub fn record_delete(&mut self, tier: Tier, key: &[u8], pre_value_bytes: Option<u64>) -> Result<()> {
		let id = parse_id(key);
		// Account for MVCC version suffix in stored key size
		let key_bytes = (key.len() + MVCC_VERSION_SIZE) as u64;

		self.update(tier, id, |stats| {
			if let Some(old_val) = pre_value_bytes {
				stats.record_delete(key_bytes, key_bytes, old_val);
			} else {
				// No pre info - just record the tombstone
				stats.historical_key_bytes += key_bytes;
				stats.historical_count += 1;
			}
		})
	}

	/// Record a drop operation (MVCC cleanup).
	pub fn record_drop(&mut self, tier: Tier, key: &[u8], value_bytes: u64) -> Result<()> {
		let id = parse_id(key);
		// Account for MVCC version suffix in stored key size
		let key_bytes = (key.len() + MVCC_VERSION_SIZE) as u64;

		self.update(tier, id, |stats| {
			stats.record_drop(key_bytes, value_bytes);
		})
	}

	/// Apply a mutation to the stats for a given (tier, id) pair.
	fn update<F>(&mut self, tier: Tier, id: MetricId, f: F) -> Result<()>
	where
		F: FnOnce(&mut MultiStorageStats),
	{
		let storage_key = EncodedKey::new(encode_storage_stats_key(tier, id));

		// Read current (or default)
		let mut stats = self
			.storage
			.get(&storage_key)?
			.and_then(|v| decode_storage_stats(v.values.as_slice()))
			.unwrap_or_default();

		// Modify
		f(&mut stats);

		// Write back
		self.storage.set(&storage_key, EncodedValues(CowVec::new(encode_storage_stats(&stats))))
	}
}

/// Reader for MVCC storage statistics (read-only).
#[derive(Clone)]
pub struct StorageStatsReader<S> {
	storage: S,
}

impl<S: SingleVersionStore> StorageStatsReader<S> {
	/// Create a new reader.
	pub fn new(storage: S) -> Self {
		Self {
			storage,
		}
	}

	/// Get stats for a specific (tier, id) pair.
	pub fn get(&self, tier: Tier, id: MetricId) -> Result<Option<MultiStorageStats>> {
		let key = EncodedKey::new(encode_storage_stats_key(tier, id));
		Ok(self.storage.get(&key)?.and_then(|v| decode_storage_stats(v.values.as_slice())))
	}

	/// Scan all storage stats entries.
	pub fn scan_all(&self) -> Result<Vec<((Tier, MetricId), MultiStorageStats)>> {
		let prefix = EncodedKey::new(storage_stats_key_prefix());
		let batch = self.storage.prefix(&prefix)?;

		let mut results = Vec::new();
		for item in batch.items {
			if let Some((tier, id)) = decode_storage_stats_key(item.key.as_slice()) {
				if let Some(stats) = decode_storage_stats(item.values.as_slice()) {
					results.push(((tier, id), stats));
				}
			}
		}

		Ok(results)
	}

	/// Scan all storage stats for a specific tier.
	pub fn scan_tier(&self, tier: Tier) -> Result<Vec<(MetricId, MultiStorageStats)>> {
		self.scan_all().map(|all| {
			all.into_iter()
				.filter_map(|((t, obj), stats)| {
					if t == tier {
						Some((obj, stats))
					} else {
						None
					}
				})
				.collect()
		})
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_storage_stats_insert() {
		let mut stats = MultiStorageStats::new();
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
		let mut stats = MultiStorageStats::new();
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
		let mut stats = MultiStorageStats::new();
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
		let mut tier_stats = TieredStorageStats::new();
		tier_stats.get_mut(Tier::Hot).record_insert(10, 100);
		tier_stats.get_mut(Tier::Warm).record_insert(20, 200);
		tier_stats.get_mut(Tier::Cold).record_insert(30, 300);

		assert_eq!(tier_stats.hot.total_bytes(), 110);
		assert_eq!(tier_stats.warm.total_bytes(), 220);
		assert_eq!(tier_stats.cold.total_bytes(), 330);
		assert_eq!(tier_stats.total_bytes(), 660);
	}
}
