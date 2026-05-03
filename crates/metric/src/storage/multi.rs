// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

/// Size of the MVCC version suffix in bytes.
///
/// Each versioned key in storage has format: `[escaped_key][terminator][version]`
/// where terminator is 2 bytes and version is 8 bytes (big-endian u64).
const MVCC_VERSION_SIZE: usize = 10;

use std::ops::AddAssign;

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::store::{SingleVersionStore, Tier},
};
use reifydb_type::{Result, util::cowvec::CowVec};

use crate::{
	MetricId,
	storage::{
		encoding::{
			decode_storage_stats, decode_storage_stats_key, encode_storage_stats, encode_storage_stats_key,
			storage_stats_key_prefix,
		},
		parser::parse_id,
	},
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MultiStorageStats {
	pub current_key_bytes: u64,

	pub current_value_bytes: u64,

	pub historical_key_bytes: u64,

	pub historical_value_bytes: u64,

	pub current_count: u64,

	pub historical_count: u64,
}

impl MultiStorageStats {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn total_bytes(&self) -> u64 {
		self.current_key_bytes
			+ self.current_value_bytes
			+ self.historical_key_bytes
			+ self.historical_value_bytes
	}

	pub fn current_bytes(&self) -> u64 {
		self.current_key_bytes + self.current_value_bytes
	}

	pub fn historical_bytes(&self) -> u64 {
		self.historical_key_bytes + self.historical_value_bytes
	}

	pub fn total_count(&self) -> u64 {
		self.current_count + self.historical_count
	}

	pub fn record_insert(&mut self, key_bytes: u64, value_bytes: u64) {
		self.current_key_bytes += key_bytes;
		self.current_value_bytes += value_bytes;
		self.current_count += 1;
	}

	pub fn record_update(
		&mut self,
		post_key_bytes: u64,
		post_value_bytes: u64,
		pre_key_bytes: u64,
		pre_value_bytes: u64,
	) {
		self.current_key_bytes = self.current_key_bytes.saturating_sub(pre_key_bytes);
		self.current_value_bytes = self.current_value_bytes.saturating_sub(pre_value_bytes);
		self.current_count = self.current_count.saturating_sub(1);

		self.historical_key_bytes += pre_key_bytes;
		self.historical_value_bytes += pre_value_bytes;
		self.historical_count += 1;

		self.current_key_bytes += post_key_bytes;
		self.current_value_bytes += post_value_bytes;
		self.current_count += 1;
	}

	pub fn record_delete(&mut self, tombstone_key_bytes: u64, pre_key_bytes: u64, pre_value_bytes: u64) {
		self.current_key_bytes = self.current_key_bytes.saturating_sub(pre_key_bytes);
		self.current_value_bytes = self.current_value_bytes.saturating_sub(pre_value_bytes);
		self.current_count = self.current_count.saturating_sub(1);

		self.historical_key_bytes += pre_key_bytes;
		self.historical_value_bytes += pre_value_bytes;
		self.historical_count += 1;

		self.historical_key_bytes += tombstone_key_bytes;
		self.historical_count += 1;
	}

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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TieredStorageStats {
	pub buffer: MultiStorageStats,
	pub persistent: MultiStorageStats,
}

impl TieredStorageStats {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn get(&self, tier: Tier) -> &MultiStorageStats {
		match tier {
			Tier::Buffer => &self.buffer,
			Tier::Persistent => &self.persistent,
		}
	}

	pub fn get_mut(&mut self, tier: Tier) -> &mut MultiStorageStats {
		match tier {
			Tier::Buffer => &mut self.buffer,
			Tier::Persistent => &mut self.persistent,
		}
	}

	pub fn total_bytes(&self) -> u64 {
		self.buffer.total_bytes() + self.persistent.total_bytes()
	}

	pub fn current_bytes(&self) -> u64 {
		self.buffer.current_bytes() + self.persistent.current_bytes()
	}

	pub fn historical_bytes(&self) -> u64 {
		self.buffer.historical_bytes() + self.persistent.historical_bytes()
	}
}

impl AddAssign for TieredStorageStats {
	fn add_assign(&mut self, rhs: Self) {
		self.buffer += rhs.buffer;
		self.persistent += rhs.persistent;
	}
}

pub struct StorageStatsWriter<S> {
	storage: S,
}

impl<S: SingleVersionStore> StorageStatsWriter<S> {
	pub fn new(storage: S) -> Self {
		Self {
			storage,
		}
	}

	pub fn record_write(
		&mut self,
		tier: Tier,
		key: &[u8],
		value_bytes: u64,
		pre_value_bytes: Option<u64>,
	) -> Result<()> {
		let id = parse_id(key);

		let key_bytes = (key.len() + MVCC_VERSION_SIZE) as u64;

		self.update(tier, id, |stats| {
			if let Some(pre_val) = pre_value_bytes {
				stats.record_update(key_bytes, value_bytes, key_bytes, pre_val);
			} else {
				stats.record_insert(key_bytes, value_bytes);
			}
		})
	}

	pub fn record_delete(&mut self, tier: Tier, key: &[u8], pre_value_bytes: Option<u64>) -> Result<()> {
		let id = parse_id(key);

		let key_bytes = (key.len() + MVCC_VERSION_SIZE) as u64;

		self.update(tier, id, |stats| {
			if let Some(pre_val) = pre_value_bytes {
				stats.record_delete(key_bytes, key_bytes, pre_val);
			} else {
				stats.historical_key_bytes += key_bytes;
				stats.historical_count += 1;
			}
		})
	}

	pub fn record_drop(&mut self, tier: Tier, key: &[u8], value_bytes: u64) -> Result<()> {
		let id = parse_id(key);

		let key_bytes = (key.len() + MVCC_VERSION_SIZE) as u64;

		self.update(tier, id, |stats| {
			stats.record_drop(key_bytes, value_bytes);
		})
	}

	fn update<F>(&mut self, tier: Tier, id: MetricId, f: F) -> Result<()>
	where
		F: FnOnce(&mut MultiStorageStats),
	{
		let storage_key = EncodedKey::new(encode_storage_stats_key(tier, id));

		let mut stats = self
			.storage
			.get(&storage_key)?
			.and_then(|v| decode_storage_stats(v.row.as_slice()))
			.unwrap_or_default();

		f(&mut stats);

		self.storage.set(&storage_key, EncodedRow(CowVec::new(encode_storage_stats(&stats))))
	}
}

#[derive(Clone)]
pub struct StorageStatsReader<S> {
	storage: S,
}

impl<S: SingleVersionStore> StorageStatsReader<S> {
	pub fn new(storage: S) -> Self {
		Self {
			storage,
		}
	}

	pub fn get(&self, tier: Tier, id: MetricId) -> Result<Option<MultiStorageStats>> {
		let key = EncodedKey::new(encode_storage_stats_key(tier, id));
		Ok(self.storage.get(&key)?.and_then(|v| decode_storage_stats(v.row.as_slice())))
	}

	pub fn scan_all(&self) -> Result<Vec<((Tier, MetricId), MultiStorageStats)>> {
		let prefix = EncodedKey::new(storage_stats_key_prefix());
		let batch = self.storage.prefix(&prefix)?;

		let mut results = Vec::new();
		for item in batch.items {
			if let Some((tier, id)) = decode_storage_stats_key(item.key.as_slice())
				&& let Some(stats) = decode_storage_stats(item.row.as_slice())
			{
				results.push(((tier, id), stats));
			}
		}

		Ok(results)
	}

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
		tier_stats.get_mut(Tier::Buffer).record_insert(10, 100);
		tier_stats.get_mut(Tier::Persistent).record_insert(20, 200);

		assert_eq!(tier_stats.buffer.total_bytes(), 110);
		assert_eq!(tier_stats.persistent.total_bytes(), 220);
		assert_eq!(tier_stats.total_bytes(), 330);
	}
}
