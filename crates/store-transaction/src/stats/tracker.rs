// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Storage tracker for real-time storage statistics.

use std::{
	collections::HashMap,
	ops::Bound,
	sync::{Arc, RwLock},
	time::{Duration, Instant},
};

use reifydb_core::key::{Key, KeyKind};
use reifydb_type::Result;

use super::{
	accumulator::StatsAccumulator,
	persistence::{
		decode_object_stats_key, decode_stats, decode_type_stats_key, encode_object_stats_key, encode_stats,
		encode_type_stats_key, object_stats_key_prefix, type_stats_key_prefix,
	},
	types::{ObjectId, StorageStats, Tier},
};
use crate::{
	backend::{PrimitiveStorage, primitive::TableId},
	stats::parser::extract_object_id,
};

/// Configuration for storage tracking.
#[derive(Debug, Clone)]
pub struct StorageTrackerConfig {
	/// Time between checkpoint persists.
	pub checkpoint_interval: Duration,
}

impl Default for StorageTrackerConfig {
	fn default() -> Self {
		Self {
			checkpoint_interval: Duration::from_secs(10),
		}
	}
}

/// Information about a previous version of a key.
#[derive(Debug, Clone)]
pub struct PreVersionInfo {
	/// Size of the key in bytes
	pub key_bytes: u64,
	/// Size of the value in bytes
	pub value_bytes: u64,
}

/// Real-time storage statistics tracker.
///
/// Maintains in-memory counters that are updated on every write operation.
/// Thread-safe via RwLock for concurrent access.
#[derive(Debug, Clone)]
pub struct StorageTracker {
	pub(super) inner: Arc<RwLock<StorageTrackerInner>>,
}

#[derive(Debug)]
pub(super) struct StorageTrackerInner {
	/// Per-tier, per-KeyKind aggregated stats
	pub(super) by_type: HashMap<(Tier, KeyKind), StorageStats>,
	/// Per-tier, per-object stats
	pub(super) by_object: HashMap<(Tier, ObjectId), StorageStats>,
	/// Per-tier totals (includes all keys, even those without recognized KeyKind)
	pub(super) by_tier: HashMap<Tier, StorageStats>,
	/// Configuration
	pub(super) config: StorageTrackerConfig,
	/// Last checkpoint time
	pub(super) last_checkpoint: Instant,
}

impl StorageTracker {
	/// Create a new storage tracker with the given configuration.
	pub fn new(config: StorageTrackerConfig) -> Self {
		Self {
			inner: Arc::new(RwLock::new(StorageTrackerInner {
				by_type: HashMap::new(),
				by_object: HashMap::new(),
				by_tier: HashMap::new(),
				config,
				last_checkpoint: Instant::now(),
			})),
		}
	}

	/// Create a new tracker with default configuration.
	pub fn with_defaults() -> Self {
		Self::new(StorageTrackerConfig::default())
	}

	/// Apply all deltas from a StatsAccumulator atomically.
	///
	/// Takes a write lock once and applies all collected deltas in a single atomic operation.
	/// This ensures no intermediate state is visible to stats queries.
	pub fn apply_deltas(&self, accumulator: &StatsAccumulator) {
		let mut inner = self.inner.write().unwrap();

		// Apply by_tier deltas
		for (tier, delta) in &accumulator.by_tier {
			let stats = inner.by_tier.entry(*tier).or_insert_with(StorageStats::new);
			stats.apply_delta(delta);
		}

		// Apply by_type deltas
		for ((tier, kind), delta) in &accumulator.by_type {
			let stats = inner.by_type.entry((*tier, *kind)).or_insert_with(StorageStats::new);
			stats.apply_delta(delta);
		}

		// Apply by_object deltas
		for ((tier, object_id), delta) in &accumulator.by_object {
			let stats = inner.by_object.entry((*tier, *object_id)).or_insert_with(StorageStats::new);
			stats.apply_delta(delta);
		}
	}

	/// Record data migration between tiers.
	///
	/// When data moves from one tier to another (e.g., hot -> warm),
	/// this updates the stats for both tiers.
	pub fn record_tier_migration(
		&self,
		from_tier: Tier,
		to_tier: Tier,
		key: &[u8],
		value_bytes: u64,
		is_current: bool,
	) {
		let key_bytes = key.len() as u64;

		let kind = Key::kind(key);
		let object_id = kind.map(|k| extract_object_id(key, k));

		let mut inner = self.inner.write().unwrap();

		// Update per-tier totals (always, regardless of KeyKind)
		{
			// Subtract from source tier
			if let Some(stats) = inner.by_tier.get_mut(&from_tier) {
				if is_current {
					stats.current_key_bytes = stats.current_key_bytes.saturating_sub(key_bytes);
					stats.current_value_bytes =
						stats.current_value_bytes.saturating_sub(value_bytes);
					stats.current_count = stats.current_count.saturating_sub(1);
				} else {
					stats.historical_key_bytes =
						stats.historical_key_bytes.saturating_sub(key_bytes);
					stats.historical_value_bytes =
						stats.historical_value_bytes.saturating_sub(value_bytes);
					stats.historical_count = stats.historical_count.saturating_sub(1);
				}
			}

			// Add to destination tier
			let stats = inner.by_tier.entry(to_tier).or_insert_with(StorageStats::new);
			if is_current {
				stats.current_key_bytes += key_bytes;
				stats.current_value_bytes += value_bytes;
				stats.current_count += 1;
			} else {
				stats.historical_key_bytes += key_bytes;
				stats.historical_value_bytes += value_bytes;
				stats.historical_count += 1;
			}
		}

		// Update per-type stats
		if let Some(kind) = kind {
			// Subtract from source tier
			if let Some(stats) = inner.by_type.get_mut(&(from_tier, kind)) {
				if is_current {
					stats.current_key_bytes = stats.current_key_bytes.saturating_sub(key_bytes);
					stats.current_value_bytes =
						stats.current_value_bytes.saturating_sub(value_bytes);
					stats.current_count = stats.current_count.saturating_sub(1);
				} else {
					stats.historical_key_bytes =
						stats.historical_key_bytes.saturating_sub(key_bytes);
					stats.historical_value_bytes =
						stats.historical_value_bytes.saturating_sub(value_bytes);
					stats.historical_count = stats.historical_count.saturating_sub(1);
				}
			}

			// Add to destination tier
			let stats = inner.by_type.entry((to_tier, kind)).or_insert_with(StorageStats::new);
			if is_current {
				stats.current_key_bytes += key_bytes;
				stats.current_value_bytes += value_bytes;
				stats.current_count += 1;
			} else {
				stats.historical_key_bytes += key_bytes;
				stats.historical_value_bytes += value_bytes;
				stats.historical_count += 1;
			}
		}

		// Update per-object stats
		if let Some(object_id) = object_id {
			// Subtract from source tier
			if let Some(stats) = inner.by_object.get_mut(&(from_tier, object_id)) {
				if is_current {
					stats.current_key_bytes = stats.current_key_bytes.saturating_sub(key_bytes);
					stats.current_value_bytes =
						stats.current_value_bytes.saturating_sub(value_bytes);
					stats.current_count = stats.current_count.saturating_sub(1);
				} else {
					stats.historical_key_bytes =
						stats.historical_key_bytes.saturating_sub(key_bytes);
					stats.historical_value_bytes =
						stats.historical_value_bytes.saturating_sub(value_bytes);
					stats.historical_count = stats.historical_count.saturating_sub(1);
				}
			}

			// Add to destination tier
			let stats = inner.by_object.entry((to_tier, object_id)).or_insert_with(StorageStats::new);
			if is_current {
				stats.current_key_bytes += key_bytes;
				stats.current_value_bytes += value_bytes;
				stats.current_count += 1;
			} else {
				stats.historical_key_bytes += key_bytes;
				stats.historical_value_bytes += value_bytes;
				stats.historical_count += 1;
			}
		}
	}

	// ========================================
	// Persistence methods
	// ========================================

	/// Check if a checkpoint is needed based on elapsed time.
	pub fn should_checkpoint(&self) -> bool {
		let inner = self.inner.read().unwrap();
		inner.last_checkpoint.elapsed() >= inner.config.checkpoint_interval
	}

	/// Persist current stats to storage.
	///
	/// Writes all tracked stats to the storage using `KeyKind::StorageTracker` keys.
	pub fn checkpoint<S: PrimitiveStorage>(&self, storage: &S) -> Result<()> {
		let mut inner = self.inner.write().unwrap();

		// Ensure the single-version table exists
		storage.ensure_table(TableId::Single)?;

		let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

		// Write per-type stats
		for ((tier, kind), stats) in &inner.by_type {
			let key = encode_type_stats_key(*tier, *kind);
			let value = encode_stats(stats);
			entries.push((key, value));
		}

		// Write per-object stats
		for ((tier, object_id), stats) in &inner.by_object {
			let key = encode_object_stats_key(*tier, *object_id);
			let value = encode_stats(stats);
			entries.push((key, value));
		}

		// Batch write all entries
		let batch: Vec<(&[u8], Option<&[u8]>)> =
			entries.iter().map(|(k, v)| (k.as_slice(), Some(v.as_slice()))).collect();
		storage.put(TableId::Single, &batch)?;

		// Reset checkpoint timer
		inner.last_checkpoint = Instant::now();

		Ok(())
	}

	/// Restore stats from storage on startup.
	///
	/// Loads previously persisted stats from storage using `KeyKind::StorageTracker` keys.
	pub fn restore<S: PrimitiveStorage>(storage: &S, config: StorageTrackerConfig) -> Result<Self> {
		let mut by_type: HashMap<(Tier, KeyKind), StorageStats> = HashMap::new();
		let mut by_object: HashMap<(Tier, ObjectId), StorageStats> = HashMap::new();

		// Read per-type stats
		let type_prefix = type_stats_key_prefix();
		// We need to find all keys that start with this prefix
		// Use range scan with prefix bounds
		let start_bound = Bound::Included(type_prefix.as_slice());
		let mut end_prefix = type_prefix.clone();
		// Increment last byte to create exclusive end bound
		if let Some(last) = end_prefix.last_mut() {
			*last = last.saturating_add(1);
		}
		let end_bound = Bound::Excluded(end_prefix.as_slice());

		for entry in storage.range(TableId::Single, start_bound, end_bound, 1000)? {
			let entry = entry?;
			if let Some((tier, kind)) = decode_type_stats_key(&entry.key) {
				if let Some(value) = entry.value {
					if let Some(stats) = decode_stats(&value) {
						by_type.insert((tier, kind), stats);
					}
				}
			}
		}

		// Read per-object stats
		let object_prefix = object_stats_key_prefix();
		let start_bound = Bound::Included(object_prefix.as_slice());
		let mut end_prefix = object_prefix.clone();
		if let Some(last) = end_prefix.last_mut() {
			*last = last.saturating_add(1);
		}
		let end_bound = Bound::Excluded(end_prefix.as_slice());

		for entry in storage.range(TableId::Single, start_bound, end_bound, 1000)? {
			let entry = entry?;
			if let Some((tier, object_id)) = decode_object_stats_key(&entry.key) {
				if let Some(value) = entry.value {
					if let Some(stats) = decode_stats(&value) {
						by_object.insert((tier, object_id), stats);
					}
				}
			}
		}

		// Compute by_tier from by_type
		let mut by_tier: HashMap<Tier, StorageStats> = HashMap::new();
		for ((tier, _kind), stats) in &by_type {
			let tier_stats = by_tier.entry(*tier).or_insert_with(StorageStats::new);
			*tier_stats += stats.clone();
		}

		Ok(Self {
			inner: Arc::new(RwLock::new(StorageTrackerInner {
				by_type,
				by_object,
				by_tier,
				config,
				last_checkpoint: Instant::now(),
			})),
		})
	}
}

#[cfg(test)]
mod tests {
	use std::thread::sleep;

	use reifydb_core::{interface::SourceId, key::Key};

	use super::*;
	use crate::stats::{accumulator::StatsAccumulator, extract_object_id};

	fn make_row_key(source_id: u64, row: u64) -> Vec<u8> {
		use reifydb_core::{interface::EncodableKey, key::RowKey};
		use reifydb_type::RowNumber;

		let key = RowKey {
			source: SourceId::table(source_id),
			row: RowNumber(row),
		};
		key.encode().to_vec()
	}

	fn get_object_id_from_key(key: &[u8]) -> Option<ObjectId> {
		Key::kind(key).map(|k| extract_object_id(key, k))
	}

	#[test]
	fn test_tracker_insert() {
		let tracker = StorageTracker::with_defaults();
		let key = make_row_key(1, 100);
		let key_bytes = key.len() as u64;

		let mut acc = StatsAccumulator::new();
		let kind = Key::kind(&key);
		let object_id = get_object_id_from_key(&key);
		acc.record_write(Tier::Hot, kind, object_id, key_bytes, 50, None);
		tracker.apply_deltas(&acc);

		let stats = tracker.total_stats();
		assert_eq!(stats.hot.current_key_bytes, key_bytes);
		assert_eq!(stats.hot.current_value_bytes, 50);
		assert_eq!(stats.hot.current_count, 1);
		assert_eq!(stats.hot.historical_count, 0);
	}

	#[test]
	fn test_tracker_update() {
		let tracker = StorageTracker::with_defaults();
		let key = make_row_key(1, 100);
		let key_bytes = key.len() as u64;

		// Insert first
		let mut acc = StatsAccumulator::new();
		let kind = Key::kind(&key);
		let object_id = get_object_id_from_key(&key);
		acc.record_write(Tier::Hot, kind, object_id, key_bytes, 50, None);
		tracker.apply_deltas(&acc);

		// Update with new value
		let pre_info = PreVersionInfo {
			key_bytes,
			value_bytes: 50,
		};
		let mut acc = StatsAccumulator::new();
		let kind = Key::kind(&key);
		let object_id = get_object_id_from_key(&key);
		let pre_version = Some((pre_info.key_bytes, pre_info.value_bytes));
		acc.record_write(Tier::Hot, kind, object_id, key_bytes, 75, pre_version);
		tracker.apply_deltas(&acc);

		let stats = tracker.total_stats();
		// Current should have new value
		assert_eq!(stats.hot.current_key_bytes, key_bytes);
		assert_eq!(stats.hot.current_value_bytes, 75);
		assert_eq!(stats.hot.current_count, 1);

		// Historical should have old value
		assert_eq!(stats.hot.historical_key_bytes, key_bytes);
		assert_eq!(stats.hot.historical_value_bytes, 50);
		assert_eq!(stats.hot.historical_count, 1);
	}

	#[test]
	fn test_tracker_delete() {
		let tracker = StorageTracker::with_defaults();
		let key = make_row_key(1, 100);
		let key_bytes = key.len() as u64;

		// Insert first
		let mut acc = StatsAccumulator::new();
		let kind = Key::kind(&key);
		let object_id = get_object_id_from_key(&key);
		acc.record_write(Tier::Hot, kind, object_id, key_bytes, 50, None);
		tracker.apply_deltas(&acc);

		// Delete
		let pre_info = PreVersionInfo {
			key_bytes,
			value_bytes: 50,
		};
		let mut acc = StatsAccumulator::new();
		let kind = Key::kind(&key);
		let object_id = get_object_id_from_key(&key);
		let pre_version = Some((pre_info.key_bytes, pre_info.value_bytes));
		acc.record_delete(Tier::Hot, kind, object_id, key_bytes, pre_version);
		tracker.apply_deltas(&acc);

		let stats = tracker.total_stats();
		// Current should be empty
		assert_eq!(stats.hot.current_count, 0);

		// Historical should have old value + tombstone
		assert_eq!(stats.hot.historical_count, 2);
	}

	#[test]
	fn test_tracker_by_type() {
		let tracker = StorageTracker::with_defaults();
		let key1 = make_row_key(1, 100);
		let key2 = make_row_key(2, 200);
		let key1_bytes = key1.len() as u64;
		let key2_bytes = key2.len() as u64;

		let mut acc = StatsAccumulator::new();
		let kind1 = Key::kind(&key1);
		let object_id1 = get_object_id_from_key(&key1);
		acc.record_write(Tier::Hot, kind1, object_id1, key1_bytes, 50, None);
		let kind2 = Key::kind(&key2);
		let object_id2 = get_object_id_from_key(&key2);
		acc.record_write(Tier::Hot, kind2, object_id2, key2_bytes, 60, None);
		tracker.apply_deltas(&acc);

		let by_type = tracker.stats_by_type(Tier::Hot);
		let row_stats = by_type.get(&KeyKind::Row).unwrap();

		assert_eq!(row_stats.current_count, 2);
		assert_eq!(row_stats.current_value_bytes, 110);
	}

	#[test]
	fn test_tracker_per_object() {
		let tracker = StorageTracker::with_defaults();
		let key1 = make_row_key(1, 100);
		let key2 = make_row_key(1, 200);
		let key3 = make_row_key(2, 100);
		let key1_bytes = key1.len() as u64;
		let key2_bytes = key2.len() as u64;
		let key3_bytes = key3.len() as u64;

		let mut acc = StatsAccumulator::new();
		let kind1 = Key::kind(&key1);
		let object_id1 = get_object_id_from_key(&key1);
		acc.record_write(Tier::Hot, kind1, object_id1, key1_bytes, 50, None);
		let kind2 = Key::kind(&key2);
		let object_id2 = get_object_id_from_key(&key2);
		acc.record_write(Tier::Hot, kind2, object_id2, key2_bytes, 60, None);
		let kind3 = Key::kind(&key3);
		let object_id3 = get_object_id_from_key(&key3);
		acc.record_write(Tier::Hot, kind3, object_id3, key3_bytes, 70, None);
		tracker.apply_deltas(&acc);

		// Object 1 (SourceId::table(1)) should have 2 entries
		let source1 = ObjectId::Source(SourceId::table(1));
		let stats1 = tracker.stats_for_object(source1).unwrap();
		assert_eq!(stats1.hot.current_count, 2);
		assert_eq!(stats1.hot.current_value_bytes, 110);

		// Object 2 (SourceId::table(2)) should have 1 entry
		let source2 = ObjectId::Source(SourceId::table(2));
		let stats2 = tracker.stats_for_object(source2).unwrap();
		assert_eq!(stats2.hot.current_count, 1);
		assert_eq!(stats2.hot.current_value_bytes, 70);
	}

	#[test]
	fn test_tracker_tier_migration() {
		let tracker = StorageTracker::with_defaults();
		let key = make_row_key(1, 100);
		let key_bytes = key.len() as u64;

		// Insert into hot tier
		let mut acc = StatsAccumulator::new();
		let kind = Key::kind(&key);
		let object_id = get_object_id_from_key(&key);
		acc.record_write(Tier::Hot, kind, object_id, key_bytes, 50, None);
		tracker.apply_deltas(&acc);

		// Migrate to warm tier
		tracker.record_tier_migration(Tier::Hot, Tier::Warm, &key, 50, true);

		let stats = tracker.total_stats();
		// Hot should be empty
		assert_eq!(stats.hot.current_count, 0);
		assert_eq!(stats.hot.current_bytes(), 0);

		// Warm should have the data
		assert_eq!(stats.warm.current_count, 1);
		assert_eq!(stats.warm.current_key_bytes, key_bytes);
		assert_eq!(stats.warm.current_value_bytes, 50);
	}

	#[test]
	fn test_top_objects() {
		let tracker = StorageTracker::with_defaults();

		// Create 3 objects with different sizes
		let key1 = make_row_key(1, 100);
		let key2 = make_row_key(2, 100);
		let key3 = make_row_key(3, 100);
		let key1_bytes = key1.len() as u64;
		let key2_bytes = key2.len() as u64;
		let key3_bytes = key3.len() as u64;

		let mut acc = StatsAccumulator::new();
		let kind1 = Key::kind(&key1);
		let object_id1 = get_object_id_from_key(&key1);
		acc.record_write(Tier::Hot, kind1, object_id1, key1_bytes, 100, None);
		let kind2 = Key::kind(&key2);
		let object_id2 = get_object_id_from_key(&key2);
		acc.record_write(Tier::Hot, kind2, object_id2, key2_bytes, 200, None);
		let kind3 = Key::kind(&key3);
		let object_id3 = get_object_id_from_key(&key3);
		acc.record_write(Tier::Hot, kind3, object_id3, key3_bytes, 50, None);
		tracker.apply_deltas(&acc);

		let top = tracker.top_objects_by_size(2);
		assert_eq!(top.len(), 2);

		// First should be source 2 (200 bytes)
		assert_eq!(top[0].0, ObjectId::Source(SourceId::table(2)));
		// Second should be source 1 (100 bytes)
		assert_eq!(top[1].0, ObjectId::Source(SourceId::table(1)));
	}

	// ============================================
	// Persistence tests
	// ============================================

	#[test]
	fn test_should_checkpoint_time_based() {
		let config = StorageTrackerConfig {
			checkpoint_interval: Duration::from_millis(50),
		};
		let tracker = StorageTracker::new(config);

		// Initially should not need checkpoint
		assert!(!tracker.should_checkpoint());

		// Wait for checkpoint interval to elapse
		sleep(Duration::from_millis(60));

		// Now should need checkpoint
		assert!(tracker.should_checkpoint());
	}

	#[test]
	fn test_checkpoint_and_restore_roundtrip() {
		use crate::backend::BackendStorage;

		// Create a memory storage backend
		let storage = BackendStorage::memory();

		// Create tracker with some data
		let config = StorageTrackerConfig {
			checkpoint_interval: Duration::from_secs(10),
		};
		let tracker = StorageTracker::new(config.clone());

		// Record some stats
		let key1 = make_row_key(1, 100);
		let key2 = make_row_key(2, 200);
		let key1_bytes = key1.len() as u64;
		let key2_bytes = key2.len() as u64;
		let mut acc = StatsAccumulator::new();
		let kind1 = Key::kind(&key1);
		let object_id1 = get_object_id_from_key(&key1);
		acc.record_write(Tier::Hot, kind1, object_id1, key1_bytes, 50, None);
		let kind2 = Key::kind(&key2);
		let object_id2 = get_object_id_from_key(&key2);
		acc.record_write(Tier::Hot, kind2, object_id2, key2_bytes, 100, None);
		acc.record_write(Tier::Warm, kind1, object_id1, key1_bytes, 75, None);
		tracker.apply_deltas(&acc);

		// Checkpoint
		tracker.checkpoint(&storage).unwrap();

		// Create a new tracker by restoring from storage
		let restored = StorageTracker::restore(&storage, config).unwrap();

		// Verify stats were restored correctly
		let original_stats = tracker.total_stats();
		let restored_stats = restored.total_stats();

		assert_eq!(original_stats.hot.current_key_bytes, restored_stats.hot.current_key_bytes);
		assert_eq!(original_stats.hot.current_value_bytes, restored_stats.hot.current_value_bytes);
		assert_eq!(original_stats.hot.current_count, restored_stats.hot.current_count);
		assert_eq!(original_stats.warm.current_key_bytes, restored_stats.warm.current_key_bytes);
		assert_eq!(original_stats.warm.current_value_bytes, restored_stats.warm.current_value_bytes);

		// Verify per-type stats
		let original_by_type = tracker.stats_by_type(Tier::Hot);
		let restored_by_type = restored.stats_by_type(Tier::Hot);
		assert_eq!(
			original_by_type.get(&KeyKind::Row).unwrap().current_count,
			restored_by_type.get(&KeyKind::Row).unwrap().current_count
		);

		// Verify per-object stats
		let source1 = ObjectId::Source(SourceId::table(1));
		let original_obj = tracker.stats_for_object(source1).unwrap();
		let restored_obj = restored.stats_for_object(source1).unwrap();
		assert_eq!(original_obj.hot.current_value_bytes, restored_obj.hot.current_value_bytes);
	}

	#[test]
	fn test_checkpoint_resets_timer() {
		use crate::backend::BackendStorage;

		let storage = BackendStorage::memory();
		let config = StorageTrackerConfig {
			checkpoint_interval: Duration::from_millis(50),
		};
		let tracker = StorageTracker::new(config);

		// Wait for checkpoint interval
		sleep(Duration::from_millis(60));
		assert!(tracker.should_checkpoint());

		// Checkpoint should reset the timer
		tracker.checkpoint(&storage).unwrap();

		// Immediately after checkpoint, should not need another one
		assert!(!tracker.should_checkpoint());

		// Wait again
		sleep(Duration::from_millis(60));

		// Should need checkpoint again
		assert!(tracker.should_checkpoint());
	}

	#[test]
	fn test_restore_empty_storage() {
		use crate::backend::BackendStorage;

		// Create empty storage
		let storage = BackendStorage::memory();

		let config = StorageTrackerConfig {
			checkpoint_interval: Duration::from_secs(10),
		};

		// Restore should succeed with empty stats
		let tracker = StorageTracker::restore(&storage, config).unwrap();
		let stats = tracker.total_stats();

		assert_eq!(stats.hot.current_count, 0);
		assert_eq!(stats.warm.current_count, 0);
		assert_eq!(stats.cold.current_count, 0);
	}

	#[test]
	fn test_batch_drop_maintains_invariant() {
		let tracker = StorageTracker::with_defaults();
		let key = make_row_key(1, 100);
		let key_bytes = key.len() as u64;

		// Insert initial version
		let mut acc = StatsAccumulator::new();
		let kind = Key::kind(&key);
		let object_id = get_object_id_from_key(&key);
		acc.record_write(Tier::Hot, kind, object_id, key_bytes, 100, None);
		tracker.apply_deltas(&acc);

		// Update multiple times to create historical versions
		let mut acc = StatsAccumulator::new();
		acc.record_write(Tier::Hot, kind, object_id, key_bytes, 200, Some((key_bytes, 100)));
		tracker.apply_deltas(&acc);

		let mut acc = StatsAccumulator::new();
		acc.record_write(Tier::Hot, kind, object_id, key_bytes, 150, Some((key_bytes, 200)));
		tracker.apply_deltas(&acc);

		let mut acc = StatsAccumulator::new();
		acc.record_write(Tier::Hot, kind, object_id, key_bytes, 250, Some((key_bytes, 150)));
		tracker.apply_deltas(&acc);

		// At this point: historical has 3 entries (100, 200, 150 bytes)
		let stats_before = tracker.total_stats();
		assert_eq!(stats_before.hot.historical_count, 3);
		assert_eq!(stats_before.hot.historical_key_bytes, key_bytes * 3);
		assert_eq!(stats_before.hot.historical_value_bytes, 450); // 100 + 200 + 150

		// Simulate dropping all historical versions in batch
		let mut acc = StatsAccumulator::new();
		acc.record_drop(
			Tier::Hot,
			kind,
			object_id,
			key_bytes * 3, // total key bytes
			450,           // total value bytes (100 + 200 + 150)
			3,             // count
		);
		tracker.apply_deltas(&acc);

		// CRITICAL: All three fields must reach zero together
		let stats_after = tracker.total_stats();
		assert_eq!(stats_after.hot.historical_count, 0);
		assert_eq!(stats_after.hot.historical_key_bytes, 0);
		assert_eq!(stats_after.hot.historical_value_bytes, 0);
	}

	#[test]
	fn test_batch_drop_partial() {
		let tracker = StorageTracker::with_defaults();
		let key = make_row_key(1, 100);
		let key_bytes = key.len() as u64;

		// Create 5 historical versions
		let mut acc = StatsAccumulator::new();
		let kind = Key::kind(&key);
		let object_id = get_object_id_from_key(&key);
		acc.record_write(Tier::Hot, kind, object_id, key_bytes, 100, None);
		tracker.apply_deltas(&acc);

		for i in 1..=5 {
			let mut acc = StatsAccumulator::new();
			acc.record_write(
				Tier::Hot,
				kind,
				object_id,
				key_bytes,
				100 + i * 10,
				Some((key_bytes, 100 + (i - 1) * 10)),
			);
			tracker.apply_deltas(&acc);
		}

		// Historical: 100, 110, 120, 130, 140 = 600 bytes total, 5 entries
		let stats_before = tracker.total_stats();
		assert_eq!(stats_before.hot.historical_count, 5);
		assert_eq!(stats_before.hot.historical_value_bytes, 600);

		// Drop oldest 3 versions (100, 110, 120 = 330 bytes)
		let mut acc = StatsAccumulator::new();
		acc.record_drop(Tier::Hot, kind, object_id, key_bytes * 3, 330, 3);
		tracker.apply_deltas(&acc);

		// Should have 2 versions left (130, 140 = 270 bytes)
		let stats_after = tracker.total_stats();
		assert_eq!(stats_after.hot.historical_count, 2);
		assert_eq!(stats_after.hot.historical_key_bytes, key_bytes * 2);
		assert_eq!(stats_after.hot.historical_value_bytes, 270);
	}

	#[test]
	fn test_batch_drop_varying_sizes() {
		let tracker = StorageTracker::with_defaults();
		let key = make_row_key(1, 100);
		let key_bytes = key.len() as u64;

		// Insert with varying value sizes
		let sizes = vec![50, 500, 150, 1000, 200];
		let mut acc = StatsAccumulator::new();
		let kind = Key::kind(&key);
		let object_id = get_object_id_from_key(&key);
		acc.record_write(Tier::Hot, kind, object_id, key_bytes, sizes[0], None);
		tracker.apply_deltas(&acc);

		for i in 1..sizes.len() {
			let mut acc = StatsAccumulator::new();
			acc.record_write(
				Tier::Hot,
				kind,
				object_id,
				key_bytes,
				sizes[i],
				Some((key_bytes, sizes[i - 1])),
			);
			tracker.apply_deltas(&acc);
		}

		// Historical: 50, 500, 150, 1000 = 1700 bytes, 4 entries
		let total_historical_bytes: u64 = sizes[..sizes.len() - 1].iter().sum();
		assert_eq!(total_historical_bytes, 1700);

		let stats_before = tracker.total_stats();
		assert_eq!(stats_before.hot.historical_count, 4);
		assert_eq!(stats_before.hot.historical_value_bytes, 1700);

		// Drop all historical
		let mut acc = StatsAccumulator::new();
		acc.record_drop(Tier::Hot, kind, object_id, key_bytes * 4, 1700, 4);
		tracker.apply_deltas(&acc);

		let stats_after = tracker.total_stats();
		assert_eq!(stats_after.hot.historical_count, 0);
		assert_eq!(stats_after.hot.historical_key_bytes, 0);
		assert_eq!(stats_after.hot.historical_value_bytes, 0);
	}

	#[test]
	fn test_batch_drop_prevents_desync_bug() {
		// This test simulates the original bug scenario where calling
		// record_drop() per entry caused count/key_bytes/value_bytes desync

		let tracker = StorageTracker::with_defaults();
		let key = make_row_key(1, 100);
		let key_bytes = key.len() as u64;

		// Create scenario: 3 versions with sizes 100, 200, 150
		let mut acc = StatsAccumulator::new();
		let kind = Key::kind(&key);
		let object_id = get_object_id_from_key(&key);
		acc.record_write(Tier::Hot, kind, object_id, key_bytes, 100, None);
		tracker.apply_deltas(&acc);

		let mut acc = StatsAccumulator::new();
		acc.record_write(Tier::Hot, kind, object_id, key_bytes, 200, Some((key_bytes, 100)));
		tracker.apply_deltas(&acc);

		let mut acc = StatsAccumulator::new();
		acc.record_write(Tier::Hot, kind, object_id, key_bytes, 150, Some((key_bytes, 200)));
		tracker.apply_deltas(&acc);

		// Verify historical setup
		let stats = tracker.total_stats();
		assert_eq!(stats.hot.historical_count, 2);
		assert_eq!(stats.hot.historical_key_bytes, key_bytes * 2);
		assert_eq!(stats.hot.historical_value_bytes, 300); // 100 + 200

		let mut acc = StatsAccumulator::new();
		acc.record_drop(
			Tier::Hot,
			kind,
			object_id,
			key_bytes * 2, // BOTH keys
			300,           // BOTH values (100 + 200)
			2,             // BOTH entries
		);
		tracker.apply_deltas(&acc);

		// The bug caused:
		// - historical_count: 0 (correct)
		// - historical_key_bytes: 0 (correct)
		// - historical_value_bytes: NON-ZERO (BUG!)
		let stats = tracker.total_stats();
		assert_eq!(stats.hot.historical_count, 0, "Count must be zero");
		assert_eq!(stats.hot.historical_key_bytes, 0, "Key bytes must be zero");
		assert_eq!(stats.hot.historical_value_bytes, 0, "Value bytes must be zero - THIS WAS THE BUG");
	}
}
