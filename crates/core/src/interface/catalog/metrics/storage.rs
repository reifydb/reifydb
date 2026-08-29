// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::AddAssign;

pub const MVCC_VERSION_SIZE: usize = 10;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MultiStorageMetrics {
	pub estimated_current_key_bytes: u64,

	pub estimated_current_value_bytes: u64,

	pub estimated_historical_key_bytes: u64,

	pub estimated_historical_value_bytes: u64,

	pub estimated_current_count: u64,

	pub estimated_historical_count: u64,
}

impl MultiStorageMetrics {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn estimated_total_bytes(&self) -> u64 {
		self.estimated_current_key_bytes
			+ self.estimated_current_value_bytes
			+ self.estimated_historical_key_bytes
			+ self.estimated_historical_value_bytes
	}

	pub fn estimated_current_bytes(&self) -> u64 {
		self.estimated_current_key_bytes + self.estimated_current_value_bytes
	}

	pub fn estimated_historical_bytes(&self) -> u64 {
		self.estimated_historical_key_bytes + self.estimated_historical_value_bytes
	}

	pub fn estimated_total_count(&self) -> u64 {
		self.estimated_current_count + self.estimated_historical_count
	}

	pub fn record_insert(&mut self, key_bytes: u64, value_bytes: u64) {
		self.estimated_current_key_bytes += key_bytes;
		self.estimated_current_value_bytes += value_bytes;
		self.estimated_current_count += 1;
	}

	pub fn record_update(
		&mut self,
		post_key_bytes: u64,
		post_value_bytes: u64,
		pre_key_bytes: u64,
		pre_value_bytes: u64,
	) {
		self.estimated_current_key_bytes = self.estimated_current_key_bytes.saturating_sub(pre_key_bytes);
		self.estimated_current_value_bytes = self.estimated_current_value_bytes.saturating_sub(pre_value_bytes);
		self.estimated_current_count = self.estimated_current_count.saturating_sub(1);

		self.estimated_historical_key_bytes += pre_key_bytes;
		self.estimated_historical_value_bytes += pre_value_bytes;
		self.estimated_historical_count += 1;

		self.estimated_current_key_bytes += post_key_bytes;
		self.estimated_current_value_bytes += post_value_bytes;
		self.estimated_current_count += 1;
	}

	pub fn record_delete(&mut self, tombstone_key_bytes: u64, pre_key_bytes: u64, pre_value_bytes: u64) {
		self.estimated_current_key_bytes = self.estimated_current_key_bytes.saturating_sub(pre_key_bytes);
		self.estimated_current_value_bytes = self.estimated_current_value_bytes.saturating_sub(pre_value_bytes);
		self.estimated_current_count = self.estimated_current_count.saturating_sub(1);

		self.estimated_historical_key_bytes += pre_key_bytes;
		self.estimated_historical_value_bytes += pre_value_bytes;
		self.estimated_historical_count += 1;

		self.estimated_current_key_bytes += tombstone_key_bytes;
		self.estimated_current_count += 1;
	}

	pub fn record_compaction(&mut self, key_bytes: u64, value_bytes: u64) {
		self.estimated_historical_key_bytes = self.estimated_historical_key_bytes.saturating_sub(key_bytes);
		self.estimated_historical_value_bytes =
			self.estimated_historical_value_bytes.saturating_sub(value_bytes);
		self.estimated_historical_count = self.estimated_historical_count.saturating_sub(1);
	}

	pub fn record_eviction(&mut self, key_bytes: u64, value_bytes: u64, current: bool) {
		if current {
			self.estimated_current_key_bytes = self.estimated_current_key_bytes.saturating_sub(key_bytes);
			self.estimated_current_value_bytes =
				self.estimated_current_value_bytes.saturating_sub(value_bytes);
			self.estimated_current_count = self.estimated_current_count.saturating_sub(1);
		} else {
			self.record_compaction(key_bytes, value_bytes);
		}
	}
}

impl AddAssign for MultiStorageMetrics {
	fn add_assign(&mut self, rhs: Self) {
		self.estimated_current_key_bytes += rhs.estimated_current_key_bytes;
		self.estimated_current_value_bytes += rhs.estimated_current_value_bytes;
		self.estimated_historical_key_bytes += rhs.estimated_historical_key_bytes;
		self.estimated_historical_value_bytes += rhs.estimated_historical_value_bytes;
		self.estimated_current_count += rhs.estimated_current_count;
		self.estimated_historical_count += rhs.estimated_historical_count;
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_storage_stats_insert() {
		let mut stats = MultiStorageMetrics::new();
		stats.record_insert(10, 100);

		assert_eq!(stats.estimated_current_key_bytes, 10);
		assert_eq!(stats.estimated_current_value_bytes, 100);
		assert_eq!(stats.estimated_current_count, 1);
		assert_eq!(stats.estimated_historical_key_bytes, 0);
		assert_eq!(stats.estimated_historical_count, 0);
		assert_eq!(stats.estimated_total_bytes(), 110);
	}

	#[test]
	fn test_storage_stats_update() {
		let mut stats = MultiStorageMetrics::new();
		stats.record_insert(10, 100);
		stats.record_update(10, 150, 10, 100);

		assert_eq!(stats.estimated_current_key_bytes, 10);
		assert_eq!(stats.estimated_current_value_bytes, 150);
		assert_eq!(stats.estimated_current_count, 1);

		assert_eq!(stats.estimated_historical_key_bytes, 10);
		assert_eq!(stats.estimated_historical_value_bytes, 100);
		assert_eq!(stats.estimated_historical_count, 1);

		assert_eq!(stats.estimated_total_bytes(), 270); // 10+150 + 10+100
	}

	#[test]
	fn a_delete_leaves_the_tombstone_on_the_current_side_where_the_store_puts_it() {
		// The store writes a delete as a none-valued current entry, moving only the pre-image to
		// historical. Charging the tombstone to historical instead invents an entry the store
		// never held, so no removal can cancel it, while under-counting current by one.
		let mut stats = MultiStorageMetrics::new();
		stats.record_insert(10, 100);
		stats.record_delete(10, 10, 100);

		assert_eq!(stats.estimated_current_count, 1, "the tombstone is the key's current version");
		assert_eq!(stats.estimated_current_key_bytes, 10, "the tombstone still costs its key");
		assert_eq!(stats.estimated_current_value_bytes, 0, "a tombstone carries no value");

		assert_eq!(stats.estimated_historical_count, 1, "only the pre-image moved to historical");
		assert_eq!(stats.estimated_historical_key_bytes, 10);
		assert_eq!(stats.estimated_historical_value_bytes, 100);
	}

	#[test]
	fn sweeping_a_deleted_key_returns_both_counters_to_zero() {
		// Eviction can only report what the store actually removed, so a counter inflated at
		// write time by an entry the store never held can never come back down. Sweeping
		// everything the store holds for a key must zero both sides.
		let mut stats = MultiStorageMetrics::new();
		stats.record_insert(10, 100);
		stats.record_delete(10, 10, 100);

		stats.record_eviction(10, 0, true);
		stats.record_eviction(10, 100, false);

		assert_eq!(stats.estimated_current_count, 0, "the tombstone was swept");
		assert_eq!(stats.estimated_historical_count, 0, "the pre-image was swept");
		assert_eq!(stats.estimated_total_count(), 0, "a fully swept key must leave no residue in either tier");
	}

	#[test]
	fn an_eviction_returns_the_bytes_the_write_that_created_them_charged() {
		// Every counter here is one-way unless something subtracts, and record_compaction is
		// driven by an event the flush sweep never emits. Without an eviction subtractor a
		// buffer swept empty still reports every version it ever held.
		let mut stats = MultiStorageMetrics::new();
		stats.record_insert(10, 100);
		stats.record_update(10, 150, 10, 100);

		assert_eq!(stats.estimated_current_count, 1, "precondition: one live version");
		assert_eq!(stats.estimated_historical_count, 1, "precondition: one superseded version");

		stats.record_eviction(10, 100, false);
		assert_eq!(stats.estimated_historical_count, 0, "the superseded version left the tier");
		assert_eq!(stats.estimated_historical_key_bytes, 0);
		assert_eq!(stats.estimated_historical_value_bytes, 0);
		assert_eq!(stats.estimated_current_count, 1, "evicting history must not disturb the live version");

		stats.record_eviction(10, 150, true);
		assert_eq!(stats.estimated_current_count, 0, "the live version left the tier too");
		assert_eq!(stats.estimated_current_key_bytes, 0);
		assert_eq!(stats.estimated_current_value_bytes, 0);
		assert_eq!(stats.estimated_total_bytes(), 0, "a fully swept tier reports no bytes");
	}

	#[test]
	fn evicting_more_than_was_recorded_clamps_instead_of_wrapping() {
		// These are u64 counters: a double-delivered sweep event would underflow to ~1.8e19 and
		// render every storage metric meaningless rather than merely slightly wrong.
		let mut stats = MultiStorageMetrics::new();
		stats.record_insert(10, 100);

		stats.record_eviction(10, 100, true);
		stats.record_eviction(10, 100, true);

		assert_eq!(stats.estimated_current_count, 0);
		assert_eq!(stats.estimated_current_key_bytes, 0);
		assert_eq!(stats.estimated_current_value_bytes, 0);
	}
}
