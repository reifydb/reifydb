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

	/// Record a write operation (insert or update).
	///
	/// - `tier`: Which storage tier this write goes to
	/// - `key`: The encoded key bytes (unversioned, used for KeyKind lookup)
	/// - `key_bytes`: Size of the key as stored (typically versioned key size)
	/// - `value_bytes`: Size of the value being written
	/// - `pre_version`: Information about the previous version, if the key already existed
	pub fn record_write(
		&self,
		tier: Tier,
		key: &[u8],
		key_bytes: u64,
		value_bytes: u64,
		pre_version: Option<PreVersionInfo>,
	) {
		let kind = Key::kind(key);
		let object_id = kind.map(|k| extract_object_id(key, k));

		let mut inner = self.inner.write().unwrap();

		// Update per-tier totals (always, regardless of KeyKind)
		{
			let stats = inner.by_tier.entry(tier).or_insert_with(StorageStats::new);
			if let Some(pre) = &pre_version {
				stats.record_update(key_bytes, value_bytes, pre.key_bytes, pre.value_bytes);
			} else {
				stats.record_insert(key_bytes, value_bytes);
			}
		}

		// Update per-type stats
		if let Some(kind) = kind {
			let stats = inner.by_type.entry((tier, kind)).or_insert_with(StorageStats::new);

			if let Some(pre) = &pre_version {
				stats.record_update(key_bytes, value_bytes, pre.key_bytes, pre.value_bytes);
			} else {
				stats.record_insert(key_bytes, value_bytes);
			}
		}

		// Update per-object stats
		if let Some(object_id) = object_id {
			let stats = inner.by_object.entry((tier, object_id)).or_insert_with(StorageStats::new);

			if let Some(pre) = &pre_version {
				stats.record_update(key_bytes, value_bytes, pre.key_bytes, pre.value_bytes);
			} else {
				stats.record_insert(key_bytes, value_bytes);
			}
		}
	}

	/// Record a delete operation (tombstone).
	///
	/// - `tier`: Which storage tier this delete goes to
	/// - `key`: The encoded key bytes (unversioned, used for KeyKind lookup)
	/// - `key_bytes`: Size of the tombstone key as stored (typically versioned key size)
	/// - `pre_version`: Information about the previous version being deleted
	pub fn record_delete(&self, tier: Tier, key: &[u8], key_bytes: u64, pre_version: Option<PreVersionInfo>) {
		// If there was no previous version, nothing to track
		let Some(pre) = pre_version else {
			return;
		};

		let kind = Key::kind(key);
		let object_id = kind.map(|k| extract_object_id(key, k));

		let mut inner = self.inner.write().unwrap();

		// Update per-tier totals (always, regardless of KeyKind)
		{
			let stats = inner.by_tier.entry(tier).or_insert_with(StorageStats::new);
			stats.record_delete(key_bytes, pre.key_bytes, pre.value_bytes);
		}

		// Update per-type stats
		if let Some(kind) = kind {
			let stats = inner.by_type.entry((tier, kind)).or_insert_with(StorageStats::new);
			stats.record_delete(key_bytes, pre.key_bytes, pre.value_bytes);
		}

		// Update per-object stats
		if let Some(object_id) = object_id {
			if let Some(stats) = inner.by_object.get_mut(&(tier, object_id)) {
				stats.record_delete(key_bytes, pre.key_bytes, pre.value_bytes);
			}
		}
	}

	/// Record a drop operation (physical removal of historical version entry).
	///
	/// Unlike delete, drop doesn't create tombstones - it physically removes
	/// entries from storage. Used for MVCC cleanup of old versions.
	///
	/// - `tier`: Which storage tier the drop occurred in
	/// - `key`: The original (unversioned) encoded key bytes
	/// - `versioned_key_bytes`: Size of the versioned key being dropped
	/// - `value_bytes`: Size of the value being dropped
	pub fn record_drop(&self, tier: Tier, key: &[u8], versioned_key_bytes: u64, value_bytes: u64) {
		let kind = Key::kind(key);
		let object_id = kind.map(|k| extract_object_id(key, k));

		let mut inner = self.inner.write().unwrap();

		// Update per-tier totals (always, regardless of KeyKind)
		if let Some(stats) = inner.by_tier.get_mut(&tier) {
			stats.record_drop(versioned_key_bytes, value_bytes);
		}

		// Update per-type stats
		if let Some(kind) = kind {
			if let Some(stats) = inner.by_type.get_mut(&(tier, kind)) {
				stats.record_drop(versioned_key_bytes, value_bytes);
			}
		}

		// Update per-object stats
		if let Some(object_id) = object_id {
			if let Some(stats) = inner.by_object.get_mut(&(tier, object_id)) {
				stats.record_drop(versioned_key_bytes, value_bytes);
			}
		}
	}

	/// Record CDC bytes for a specific change.
	///
	/// Called for each change in a CDC entry to attribute bytes to the source object.
	/// - `tier`: Which storage tier the CDC entry was written to
	/// - `key`: The change key (identifies the source object)
	/// - `value_bytes`: Bytes attributed to this change (distributed overhead)
	/// - `count`: Number of CDC entries to record (typically 1)
	pub fn record_cdc_for_change(&self, tier: Tier, key: &[u8], value_bytes: u64, count: u64) {
		let key_bytes = key.len() as u64;

		let kind = Key::kind(key);
		let object_id = kind.map(|k| extract_object_id(key, k));

		let mut inner = self.inner.write().unwrap();

		// Update per-tier totals (always, regardless of KeyKind)
		{
			let stats = inner.by_tier.entry(tier).or_insert_with(StorageStats::new);
			stats.record_cdc(key_bytes, value_bytes, count);
		}

		// Update per-type stats
		if let Some(kind) = kind {
			let stats = inner.by_type.entry((tier, kind)).or_insert_with(StorageStats::new);
			stats.record_cdc(key_bytes, value_bytes, count);
		}

		// Update per-object stats
		if let Some(object_id) = object_id {
			let stats = inner.by_object.entry((tier, object_id)).or_insert_with(StorageStats::new);
			stats.record_cdc(key_bytes, value_bytes, count);
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
	pub async fn checkpoint_async<S: PrimitiveStorage>(&self, storage: &S) -> Result<()> {
		// Ensure the single-version table exists
		storage.ensure_table(TableId::Single).await?;

		let entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = {
			let inner = self.inner.read().unwrap();

			let mut entries = Vec::new();

			// Write per-type stats
			for ((tier, kind), stats) in &inner.by_type {
				let key = encode_type_stats_key(*tier, *kind);
				let value = encode_stats(stats);
				entries.push((key, Some(value)));
			}

			// Write per-object stats
			for ((tier, object_id), stats) in &inner.by_object {
				let key = encode_object_stats_key(*tier, *object_id);
				let value = encode_stats(stats);
				entries.push((key, Some(value)));
			}

			entries
		};

		// Batch write all entries
		storage.put(TableId::Single, entries).await?;

		// Reset checkpoint timer
		{
			let mut inner = self.inner.write().unwrap();
			inner.last_checkpoint = Instant::now();
		}

		Ok(())
	}

	/// Restore stats from storage on startup.
	///
	/// Loads previously persisted stats from storage using `KeyKind::StorageTracker` keys.
	pub async fn restore_async<S: PrimitiveStorage>(storage: &S, config: StorageTrackerConfig) -> Result<Self> {
		let mut by_type: HashMap<(Tier, KeyKind), StorageStats> = HashMap::new();
		let mut by_object: HashMap<(Tier, ObjectId), StorageStats> = HashMap::new();

		// Read per-type stats
		let type_prefix = type_stats_key_prefix();
		let mut end_prefix = type_prefix.clone();
		if let Some(last) = end_prefix.last_mut() {
			*last = last.saturating_add(1);
		}

		let batch = storage
			.range_batch(TableId::Single, Bound::Included(type_prefix), Bound::Excluded(end_prefix), 1000)
			.await?;

		for entry in batch.entries {
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
		let mut end_prefix = object_prefix.clone();
		if let Some(last) = end_prefix.last_mut() {
			*last = last.saturating_add(1);
		}

		let batch = storage
			.range_batch(TableId::Single, Bound::Included(object_prefix), Bound::Excluded(end_prefix), 1000)
			.await?;

		for entry in batch.entries {
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
	use reifydb_core::interface::SourceId;

	use super::*;

	fn make_row_key(source_id: u64, row: u64) -> Vec<u8> {
		use reifydb_core::{interface::EncodableKey, key::RowKey};
		use reifydb_type::RowNumber;

		let key = RowKey {
			source: SourceId::table(source_id),
			row: RowNumber(row),
		};
		key.encode().to_vec()
	}

	#[test]
	fn test_tracker_insert() {
		let tracker = StorageTracker::with_defaults();
		let key = make_row_key(1, 100);
		let key_bytes = key.len() as u64;

		tracker.record_write(Tier::Hot, &key, key_bytes, 50, None);

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
		tracker.record_write(Tier::Hot, &key, key_bytes, 50, None);

		// Update with new value
		let pre_info = PreVersionInfo {
			key_bytes,
			value_bytes: 50,
		};
		tracker.record_write(Tier::Hot, &key, key_bytes, 75, Some(pre_info));

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
		tracker.record_write(Tier::Hot, &key, key_bytes, 50, None);

		// Delete
		let pre_info = PreVersionInfo {
			key_bytes,
			value_bytes: 50,
		};
		tracker.record_delete(Tier::Hot, &key, key_bytes, Some(pre_info));

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

		tracker.record_write(Tier::Hot, &key1, key1_bytes, 50, None);
		tracker.record_write(Tier::Hot, &key2, key2_bytes, 60, None);

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

		tracker.record_write(Tier::Hot, &key1, key1_bytes, 50, None);
		tracker.record_write(Tier::Hot, &key2, key2_bytes, 60, None);
		tracker.record_write(Tier::Hot, &key3, key3_bytes, 70, None);

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
		tracker.record_write(Tier::Hot, &key, key_bytes, 50, None);

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

		tracker.record_write(Tier::Hot, &key1, key1_bytes, 100, None);
		tracker.record_write(Tier::Hot, &key2, key2_bytes, 200, None);
		tracker.record_write(Tier::Hot, &key3, key3_bytes, 50, None);

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
		std::thread::sleep(Duration::from_millis(60));

		// Now should need checkpoint
		assert!(tracker.should_checkpoint());
	}

	#[tokio::test]
	async fn test_checkpoint_and_restore_roundtrip() {
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
		tracker.record_write(Tier::Hot, &key1, key1_bytes, 50, None);
		tracker.record_write(Tier::Hot, &key2, key2_bytes, 100, None);
		tracker.record_write(Tier::Warm, &key1, key1_bytes, 75, None);

		// Checkpoint
		tracker.checkpoint_async(&storage).await.unwrap();

		// Create a new tracker by restoring from storage
		let restored = StorageTracker::restore_async(&storage, config).await.unwrap();

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

	#[tokio::test]
	async fn test_checkpoint_resets_timer() {
		use crate::backend::BackendStorage;

		let storage = BackendStorage::memory();
		let config = StorageTrackerConfig {
			checkpoint_interval: Duration::from_millis(50),
		};
		let tracker = StorageTracker::new(config);

		// Wait for checkpoint interval
		tokio::time::sleep(Duration::from_millis(60)).await;
		assert!(tracker.should_checkpoint());

		// Checkpoint should reset the timer
		tracker.checkpoint_async(&storage).await.unwrap();

		// Immediately after checkpoint, should not need another one
		assert!(!tracker.should_checkpoint());

		// Wait again
		tokio::time::sleep(Duration::from_millis(60)).await;

		// Should need checkpoint again
		assert!(tracker.should_checkpoint());
	}

	#[tokio::test]
	async fn test_restore_empty_storage() {
		use crate::backend::BackendStorage;

		// Create empty storage
		let storage = BackendStorage::memory();

		let config = StorageTrackerConfig {
			checkpoint_interval: Duration::from_secs(10),
		};

		// Restore should succeed with empty stats
		let tracker = StorageTracker::restore_async(&storage, config).await.unwrap();
		let stats = tracker.total_stats();

		assert_eq!(stats.hot.current_count, 0);
		assert_eq!(stats.warm.current_count, 0);
		assert_eq!(stats.cold.current_count, 0);
	}
}
