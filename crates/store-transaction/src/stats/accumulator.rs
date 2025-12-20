// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Stats accumulator for atomic transaction-level stats updates.
//!
//! The accumulator collects all stats changes during a transaction without
//! applying them immediately. At the end of the transaction, all changes are
//! applied atomically in a single operation, ensuring no intermediate state
//! is visible to stats queries.

use std::collections::HashMap;

use reifydb_core::key::KeyKind;

use super::types::ObjectId;
use crate::Tier;

/// Delta representing changes to storage stats.
/// Uses signed integers to support both increments and decrements.
#[derive(Debug, Clone, Default)]
pub struct StorageStatsDelta {
	pub current_count_delta: i64,
	pub current_key_bytes_delta: i64,
	pub current_value_bytes_delta: i64,
	pub historical_count_delta: i64,
	pub historical_key_bytes_delta: i64,
	pub historical_value_bytes_delta: i64,
	pub cdc_count_delta: i64,
	pub cdc_key_bytes_delta: i64,
	pub cdc_value_bytes_delta: i64,
}

impl StorageStatsDelta {
	/// Add an insert operation to this delta.
	pub fn add_insert(&mut self, key_bytes: u64, value_bytes: u64) {
		self.current_count_delta += 1;
		self.current_key_bytes_delta += key_bytes as i64;
		self.current_value_bytes_delta += value_bytes as i64;
	}

	/// Add an update operation to this delta.
	pub fn add_update(
		&mut self,
		new_key_bytes: u64,
		new_value_bytes: u64,
		old_key_bytes: u64,
		old_value_bytes: u64,
	) {
		// Move old version from current to historical
		self.current_count_delta -= 1;
		self.current_key_bytes_delta -= old_key_bytes as i64;
		self.current_value_bytes_delta -= old_value_bytes as i64;

		self.historical_count_delta += 1;
		self.historical_key_bytes_delta += old_key_bytes as i64;
		self.historical_value_bytes_delta += old_value_bytes as i64;

		// Add new version to current
		self.current_count_delta += 1;
		self.current_key_bytes_delta += new_key_bytes as i64;
		self.current_value_bytes_delta += new_value_bytes as i64;
	}

	/// Add a delete operation to this delta.
	pub fn add_delete(&mut self, tombstone_key_bytes: u64, old_key_bytes: u64, old_value_bytes: u64) {
		// Move old version from current to historical
		self.current_count_delta -= 1;
		self.current_key_bytes_delta -= old_key_bytes as i64;
		self.current_value_bytes_delta -= old_value_bytes as i64;

		self.historical_count_delta += 1;
		self.historical_key_bytes_delta += old_key_bytes as i64;
		self.historical_value_bytes_delta += old_value_bytes as i64;

		// Tombstone goes to historical (key only, no value)
		self.historical_count_delta += 1;
		self.historical_key_bytes_delta += tombstone_key_bytes as i64;
	}

	/// Add a drop operation to this delta.
	pub fn add_drop(&mut self, key_bytes: u64, value_bytes: u64) {
		self.historical_count_delta -= 1;
		self.historical_key_bytes_delta -= key_bytes as i64;
		self.historical_value_bytes_delta -= value_bytes as i64;
	}

	/// Add a CDC (Change Data Capture) operation to this delta.
	pub fn add_cdc(&mut self, key_bytes: u64, value_bytes: u64, count: u64) {
		self.cdc_count_delta += count as i64;
		self.cdc_key_bytes_delta += key_bytes as i64;
		self.cdc_value_bytes_delta += value_bytes as i64;
	}
}

/// Accumulator for collecting stats changes during a transaction.
///
/// Mirrors the three-tier structure of StorageTracker (by_tier, by_type, by_object)
/// but accumulates deltas locally without any locking. All changes are applied
/// atomically at the end of the transaction.
#[derive(Debug, Default)]
pub struct StatsAccumulator {
	pub by_tier: HashMap<Tier, StorageStatsDelta>,
	pub by_type: HashMap<(Tier, KeyKind), StorageStatsDelta>,
	pub by_object: HashMap<(Tier, ObjectId), StorageStatsDelta>,
}

impl StatsAccumulator {
	/// Create a new empty accumulator.
	pub fn new() -> Self {
		Self::default()
	}

	/// Record a write operation (insert or update).
	pub fn record_write(
		&mut self,
		tier: Tier,
		kind: Option<KeyKind>,
		object_id: Option<ObjectId>,
		key_bytes: u64,
		value_bytes: u64,
		pre_version: Option<(u64, u64)>,
	) {
		// Update by_tier
		let tier_delta = self.by_tier.entry(tier).or_default();
		if let Some((pre_key_bytes, pre_value_bytes)) = pre_version {
			tier_delta.add_update(key_bytes, value_bytes, pre_key_bytes, pre_value_bytes);
		} else {
			tier_delta.add_insert(key_bytes, value_bytes);
		}

		// Update by_type
		if let Some(k) = kind {
			let type_delta = self.by_type.entry((tier, k)).or_default();
			if let Some((pre_key_bytes, pre_value_bytes)) = pre_version {
				type_delta.add_update(key_bytes, value_bytes, pre_key_bytes, pre_value_bytes);
			} else {
				type_delta.add_insert(key_bytes, value_bytes);
			}
		}

		// Update by_object
		if let Some(oid) = object_id {
			let object_delta = self.by_object.entry((tier, oid)).or_default();
			if let Some((pre_key_bytes, pre_value_bytes)) = pre_version {
				object_delta.add_update(key_bytes, value_bytes, pre_key_bytes, pre_value_bytes);
			} else {
				object_delta.add_insert(key_bytes, value_bytes);
			}
		}
	}

	/// Record a delete operation (tombstone).
	pub fn record_delete(
		&mut self,
		tier: Tier,
		kind: Option<KeyKind>,
		object_id: Option<ObjectId>,
		tombstone_key_bytes: u64,
		pre_version: Option<(u64, u64)>,
	) {
		// If there was no previous version, nothing to track
		let Some((pre_key_bytes, pre_value_bytes)) = pre_version else {
			return;
		};

		// Update by_tier
		let tier_delta = self.by_tier.entry(tier).or_default();
		tier_delta.add_delete(tombstone_key_bytes, pre_key_bytes, pre_value_bytes);

		// Update by_type
		if let Some(k) = kind {
			let type_delta = self.by_type.entry((tier, k)).or_default();
			type_delta.add_delete(tombstone_key_bytes, pre_key_bytes, pre_value_bytes);
		}

		// Update by_object
		if let Some(oid) = object_id {
			let object_delta = self.by_object.entry((tier, oid)).or_default();
			object_delta.add_delete(tombstone_key_bytes, pre_key_bytes, pre_value_bytes);
		}
	}

	/// Record a drop operation (removal of historical versions).
	///
	/// Subtracts the total bytes and count directly to avoid integer division rounding errors.
	pub fn record_drop(
		&mut self,
		tier: Tier,
		kind: Option<KeyKind>,
		object_id: Option<ObjectId>,
		total_key_bytes: u64,
		total_value_bytes: u64,
		count: u64,
	) {
		// Update by_tier - subtract totals directly
		let tier_delta = self.by_tier.entry(tier).or_default();
		tier_delta.historical_count_delta -= count as i64;
		tier_delta.historical_key_bytes_delta -= total_key_bytes as i64;
		tier_delta.historical_value_bytes_delta -= total_value_bytes as i64;

		// Update by_type
		if let Some(k) = kind {
			let type_delta = self.by_type.entry((tier, k)).or_default();
			type_delta.historical_count_delta -= count as i64;
			type_delta.historical_key_bytes_delta -= total_key_bytes as i64;
			type_delta.historical_value_bytes_delta -= total_value_bytes as i64;
		}

		// Update by_object
		if let Some(oid) = object_id {
			let object_delta = self.by_object.entry((tier, oid)).or_default();
			object_delta.historical_count_delta -= count as i64;
			object_delta.historical_key_bytes_delta -= total_key_bytes as i64;
			object_delta.historical_value_bytes_delta -= total_value_bytes as i64;
		}
	}

	/// Record CDC (Change Data Capture) bytes for a change.
	pub fn record_cdc(
		&mut self,
		tier: Tier,
		kind: Option<KeyKind>,
		object_id: Option<ObjectId>,
		key_bytes: u64,
		value_bytes: u64,
		count: u64,
	) {
		// Update by_tier
		let tier_delta = self.by_tier.entry(tier).or_default();
		tier_delta.add_cdc(key_bytes, value_bytes, count);

		// Update by_type
		if let Some(k) = kind {
			let type_delta = self.by_type.entry((tier, k)).or_default();
			type_delta.add_cdc(key_bytes, value_bytes, count);
		}

		// Update by_object
		if let Some(oid) = object_id {
			let object_delta = self.by_object.entry((tier, oid)).or_default();
			object_delta.add_cdc(key_bytes, value_bytes, count);
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{interface::SourceId, key::KeyKind};

	use super::*;

	#[test]
	fn test_storage_stats_delta_insert() {
		let mut delta = StorageStatsDelta::default();
		delta.add_insert(10, 100);

		assert_eq!(delta.current_count_delta, 1);
		assert_eq!(delta.current_key_bytes_delta, 10);
		assert_eq!(delta.current_value_bytes_delta, 100);
		assert_eq!(delta.historical_count_delta, 0);
		assert_eq!(delta.historical_key_bytes_delta, 0);
		assert_eq!(delta.historical_value_bytes_delta, 0);
	}

	#[test]
	fn test_storage_stats_delta_update() {
		let mut delta = StorageStatsDelta::default();
		delta.add_update(10, 150, 10, 100);

		// Should move old version to historical and add new to current
		// Net effect: current stays same count, but value increases
		assert_eq!(delta.current_count_delta, 0); // -1 (old) + 1 (new)
		assert_eq!(delta.current_key_bytes_delta, 0); // -10 (old) + 10 (new)
		assert_eq!(delta.current_value_bytes_delta, 50); // -100 (old) + 150 (new)
		assert_eq!(delta.historical_count_delta, 1);
		assert_eq!(delta.historical_key_bytes_delta, 10);
		assert_eq!(delta.historical_value_bytes_delta, 100);
	}

	#[test]
	fn test_storage_stats_delta_delete() {
		let mut delta = StorageStatsDelta::default();
		delta.add_delete(10, 10, 100);

		// Should move old to historical, add tombstone to historical
		assert_eq!(delta.current_count_delta, -1);
		assert_eq!(delta.current_key_bytes_delta, -10);
		assert_eq!(delta.current_value_bytes_delta, -100);
		assert_eq!(delta.historical_count_delta, 2); // old + tombstone
		assert_eq!(delta.historical_key_bytes_delta, 20); // old key + tombstone key
		assert_eq!(delta.historical_value_bytes_delta, 100); // old value only
	}

	#[test]
	fn test_storage_stats_delta_drop() {
		let mut delta = StorageStatsDelta::default();
		delta.add_drop(10, 100);

		assert_eq!(delta.current_count_delta, 0);
		assert_eq!(delta.current_key_bytes_delta, 0);
		assert_eq!(delta.current_value_bytes_delta, 0);
		assert_eq!(delta.historical_count_delta, -1);
		assert_eq!(delta.historical_key_bytes_delta, -10);
		assert_eq!(delta.historical_value_bytes_delta, -100);
	}

	#[test]
	fn test_accumulator_by_tier() {
		let mut acc = StatsAccumulator::new();

		// Record writes to different tiers
		acc.record_write(Tier::Hot, None, None, 10, 100, None);
		acc.record_write(Tier::Warm, None, None, 20, 200, None);

		assert_eq!(acc.by_tier.len(), 2);
		assert_eq!(acc.by_tier[&Tier::Hot].current_count_delta, 1);
		assert_eq!(acc.by_tier[&Tier::Hot].current_key_bytes_delta, 10);
		assert_eq!(acc.by_tier[&Tier::Warm].current_count_delta, 1);
		assert_eq!(acc.by_tier[&Tier::Warm].current_key_bytes_delta, 20);
	}

	#[test]
	fn test_accumulator_by_type() {
		let mut acc = StatsAccumulator::new();

		// Record writes of different key types
		acc.record_write(Tier::Hot, Some(KeyKind::Row), None, 10, 100, None);
		acc.record_write(Tier::Hot, Some(KeyKind::Index), None, 20, 200, None);

		assert_eq!(acc.by_type.len(), 2);
		assert_eq!(acc.by_type[&(Tier::Hot, KeyKind::Row)].current_count_delta, 1);
		assert_eq!(acc.by_type[&(Tier::Hot, KeyKind::Index)].current_count_delta, 1);
	}

	#[test]
	fn test_accumulator_by_object() {
		let mut acc = StatsAccumulator::new();

		let source1 = ObjectId::Source(SourceId::table(1));
		let source2 = ObjectId::Source(SourceId::table(2));

		// Record writes to different objects
		acc.record_write(Tier::Hot, None, Some(source1), 10, 100, None);
		acc.record_write(Tier::Hot, None, Some(source2), 20, 200, None);

		assert_eq!(acc.by_object.len(), 2);
		assert_eq!(acc.by_object[&(Tier::Hot, source1)].current_count_delta, 1);
		assert_eq!(acc.by_object[&(Tier::Hot, source2)].current_count_delta, 1);
	}

	#[test]
	fn test_accumulator_multiple_operations_accumulate() {
		let mut acc = StatsAccumulator::new();

		// Multiple inserts to same tier should accumulate
		acc.record_write(Tier::Hot, None, None, 10, 100, None);
		acc.record_write(Tier::Hot, None, None, 20, 200, None);
		acc.record_write(Tier::Hot, None, None, 30, 300, None);

		let tier_delta = &acc.by_tier[&Tier::Hot];
		assert_eq!(tier_delta.current_count_delta, 3);
		assert_eq!(tier_delta.current_key_bytes_delta, 60);
		assert_eq!(tier_delta.current_value_bytes_delta, 600);
	}

	#[test]
	fn test_accumulator_insert_then_update() {
		let mut acc = StatsAccumulator::new();

		// Insert
		acc.record_write(Tier::Hot, None, None, 10, 100, None);
		// Update
		acc.record_write(Tier::Hot, None, None, 10, 150, Some((10, 100)));

		let tier_delta = &acc.by_tier[&Tier::Hot];
		// Insert: +1 current
		// Update: -1 current (old), +1 historical (old), +1 current (new)
		// Net: +1 current, +1 historical
		assert_eq!(tier_delta.current_count_delta, 1);
		assert_eq!(tier_delta.current_key_bytes_delta, 10);
		assert_eq!(tier_delta.current_value_bytes_delta, 150);
		assert_eq!(tier_delta.historical_count_delta, 1);
		assert_eq!(tier_delta.historical_key_bytes_delta, 10);
		assert_eq!(tier_delta.historical_value_bytes_delta, 100);
	}

	#[test]
	fn test_accumulator_delete_without_pre_version() {
		let mut acc = StatsAccumulator::new();

		// Delete without pre_version should be no-op
		acc.record_delete(Tier::Hot, None, None, 10, None);

		// Should have no entries
		assert_eq!(acc.by_tier.len(), 0);
	}

	#[test]
	fn test_accumulator_drop_multiple_entries() {
		let mut acc = StatsAccumulator::new();

		// Drop 3 entries with total size 30/300
		acc.record_drop(Tier::Hot, None, None, 30, 300, 3);

		let tier_delta = &acc.by_tier[&Tier::Hot];
		// Should have 3 drops of 10/100 each
		assert_eq!(tier_delta.historical_count_delta, -3);
		assert_eq!(tier_delta.historical_key_bytes_delta, -30);
		assert_eq!(tier_delta.historical_value_bytes_delta, -300);
	}

	#[test]
	fn test_accumulator_all_dimensions() {
		let mut acc = StatsAccumulator::new();

		let source = ObjectId::Source(SourceId::table(42));

		// Record write with all dimensions
		acc.record_write(Tier::Hot, Some(KeyKind::Row), Some(source), 10, 100, None);

		// Should track in all three dimensions
		assert_eq!(acc.by_tier.len(), 1);
		assert_eq!(acc.by_type.len(), 1);
		assert_eq!(acc.by_object.len(), 1);

		assert_eq!(acc.by_tier[&Tier::Hot].current_count_delta, 1);
		assert_eq!(acc.by_type[&(Tier::Hot, KeyKind::Row)].current_count_delta, 1);
		assert_eq!(acc.by_object[&(Tier::Hot, source)].current_count_delta, 1);
	}

	#[test]
	fn test_storage_stats_delta_cdc() {
		let mut delta = StorageStatsDelta::default();
		delta.add_cdc(100, 500, 5);

		assert_eq!(delta.cdc_count_delta, 5);
		assert_eq!(delta.cdc_key_bytes_delta, 100);
		assert_eq!(delta.cdc_value_bytes_delta, 500);
		// CDC shouldn't affect current or historical
		assert_eq!(delta.current_count_delta, 0);
		assert_eq!(delta.historical_count_delta, 0);
	}

	#[test]
	fn test_accumulator_record_cdc() {
		let mut acc = StatsAccumulator::new();

		acc.record_cdc(Tier::Hot, None, None, 100, 500, 5);

		let tier_delta = &acc.by_tier[&Tier::Hot];
		assert_eq!(tier_delta.cdc_count_delta, 5);
		assert_eq!(tier_delta.cdc_key_bytes_delta, 100);
		assert_eq!(tier_delta.cdc_value_bytes_delta, 500);
	}

	#[test]
	fn test_accumulator_record_cdc_with_type_and_object() {
		let mut acc = StatsAccumulator::new();

		let source = ObjectId::Source(SourceId::table(42));

		acc.record_cdc(Tier::Hot, Some(KeyKind::Row), Some(source), 100, 500, 5);

		// Should track in all three dimensions
		assert_eq!(acc.by_tier.len(), 1);
		assert_eq!(acc.by_type.len(), 1);
		assert_eq!(acc.by_object.len(), 1);

		assert_eq!(acc.by_tier[&Tier::Hot].cdc_count_delta, 5);
		assert_eq!(acc.by_type[&(Tier::Hot, KeyKind::Row)].cdc_count_delta, 5);
		assert_eq!(acc.by_object[&(Tier::Hot, source)].cdc_count_delta, 5);
	}

	#[test]
	fn test_accumulator_mixed_operations_with_cdc() {
		let mut acc = StatsAccumulator::new();

		// Write operation
		acc.record_write(Tier::Hot, None, None, 10, 100, None);
		// CDC operation
		acc.record_cdc(Tier::Hot, None, None, 50, 200, 2);

		let tier_delta = &acc.by_tier[&Tier::Hot];
		// Write stats
		assert_eq!(tier_delta.current_count_delta, 1);
		assert_eq!(tier_delta.current_key_bytes_delta, 10);
		assert_eq!(tier_delta.current_value_bytes_delta, 100);
		// CDC stats
		assert_eq!(tier_delta.cdc_count_delta, 2);
		assert_eq!(tier_delta.cdc_key_bytes_delta, 50);
		assert_eq!(tier_delta.cdc_value_bytes_delta, 200);
	}
}
