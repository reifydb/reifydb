// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Query methods for storage statistics.

use std::collections::HashMap;

use reifydb_core::key::KeyKind;

use super::{
	tracker::{StorageTracker, StorageTrackerConfig},
	types::{ObjectId, StorageStats, Tier, TierStats},
};

impl StorageTracker {
	/// Get total storage stats across all tiers.
	pub fn total_stats(&self) -> TierStats {
		let inner = self.inner.read().unwrap();
		let mut result = TierStats::new();

		for ((tier, _kind), stats) in &inner.by_type {
			*result.get_mut(*tier) += stats.clone();
		}

		result
	}

	/// Get stats aggregated by KeyKind for a specific tier.
	pub fn stats_by_type(&self, tier: Tier) -> HashMap<KeyKind, StorageStats> {
		let inner = self.inner.read().unwrap();
		let mut result = HashMap::new();

		for ((t, kind), stats) in &inner.by_type {
			if *t == tier {
				result.insert(*kind, stats.clone());
			}
		}

		result
	}

	/// Get stats aggregated by KeyKind across all tiers.
	pub fn stats_by_type_all_tiers(&self) -> HashMap<KeyKind, TierStats> {
		let inner = self.inner.read().unwrap();
		let mut result: HashMap<KeyKind, TierStats> = HashMap::new();

		for ((tier, kind), stats) in &inner.by_type {
			let tier_stats = result.entry(*kind).or_insert_with(TierStats::new);
			*tier_stats.get_mut(*tier) += stats.clone();
		}

		result
	}

	/// Get stats for a specific object across all tiers.
	pub fn stats_for_object(&self, object_id: ObjectId) -> Option<TierStats> {
		let inner = self.inner.read().unwrap();

		let mut result = TierStats::new();
		let mut found = false;

		for tier in [Tier::Hot, Tier::Warm, Tier::Cold] {
			if let Some(stats) = inner.by_object.get(&(tier, object_id)) {
				*result.get_mut(tier) = stats.clone();
				found = true;
			}
		}

		if found {
			Some(result)
		} else {
			None
		}
	}

	/// Get all objects for a specific tier, sorted by total bytes descending.
	pub fn objects_by_tier(&self, tier: Tier) -> Vec<(ObjectId, StorageStats)> {
		let inner = self.inner.read().unwrap();
		let mut result: Vec<_> = inner
			.by_object
			.iter()
			.filter(|((t, _), _)| *t == tier)
			.map(|((_, obj_id), stats)| (*obj_id, stats.clone()))
			.collect();

		result.sort_by(|(_, a), (_, b)| b.total_bytes().cmp(&a.total_bytes()));
		result
	}

	/// Get top N objects by total storage consumption across all tiers.
	pub fn top_objects_by_size(&self, n: usize) -> Vec<(ObjectId, TierStats)> {
		let inner = self.inner.read().unwrap();

		// Aggregate stats by object across all tiers
		let mut by_object: HashMap<ObjectId, TierStats> = HashMap::new();
		for ((tier, obj_id), stats) in &inner.by_object {
			let tier_stats = by_object.entry(*obj_id).or_insert_with(TierStats::new);
			*tier_stats.get_mut(*tier) = stats.clone();
		}

		// Sort by total bytes
		let mut result: Vec<_> = by_object.into_iter().collect();
		result.sort_by(|(_, a), (_, b)| b.total_bytes().cmp(&a.total_bytes()));
		result.truncate(n);
		result
	}

	/// Get configuration.
	pub fn config(&self) -> StorageTrackerConfig {
		self.inner.read().unwrap().config.clone()
	}
}
