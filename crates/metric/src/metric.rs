// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Combined stats reader for catalog vtables.
//!
//! This module provides a unified reader that combines storage and CDC stats
//! for use by virtual tables that need to display both.

use std::collections::HashMap;

use reifydb_core::interface::store::SingleVersionStore;

use crate::{
	MetricId,
	cdc::{CdcStats, CdcStatsReader},
	multi::{MultiStorageStats, StorageStatsReader, Tier},
};

/// Combined storage and CDC statistics for a single object.
#[derive(Debug, Clone, Default)]
pub struct CombinedStats {
	/// MVCC storage statistics
	pub storage: MultiStorageStats,
	/// CDC statistics
	pub cdc: CdcStats,
}

impl CombinedStats {
	/// Total bytes for current (latest) data only.
	pub fn current_bytes(&self) -> u64 {
		self.storage.current_bytes()
	}

	/// Total bytes for historical (older) data only.
	pub fn historical_bytes(&self) -> u64 {
		self.storage.historical_bytes()
	}

	/// Total storage bytes across current and historical.
	pub fn total_bytes(&self) -> u64 {
		self.storage.total_bytes()
	}

	/// Total CDC bytes.
	pub fn cdc_total_bytes(&self) -> u64 {
		self.cdc.total_bytes()
	}
}

/// Combined reader for storage and CDC statistics.
///
/// This provides a single interface for querying both storage and CDC stats,
/// primarily used by virtual tables.
#[derive(Clone)]
pub struct MetricReader<S> {
	storage_reader: StorageStatsReader<S>,
	cdc_reader: CdcStatsReader<S>,
}

impl<S: SingleVersionStore> MetricReader<S> {
	/// Create a new combined stats reader.
	pub fn new(storage: S) -> Self {
		Self {
			storage_reader: StorageStatsReader::new(storage.clone()),
			cdc_reader: CdcStatsReader::new(storage),
		}
	}

	/// Scan all objects for a specific tier, returning combined stats.
	///
	/// Returns storage stats for the tier merged with CDC stats (which are not tiered).
	pub fn scan_tier(&self, tier: Tier) -> reifydb_type::Result<Vec<(MetricId, CombinedStats)>> {
		// Get storage stats for this tier
		let storage_stats = self.storage_reader.scan_tier(tier)?;

		// Get all CDC stats (not tiered)
		let cdc_stats: HashMap<MetricId, CdcStats> = self.cdc_reader.scan_all()?.into_iter().collect();

		// Combine into results
		let results: Vec<(MetricId, CombinedStats)> = storage_stats
			.into_iter()
			.map(|(obj_id, storage)| {
				let cdc = cdc_stats.get(&obj_id).cloned().unwrap_or_default();
				(
					obj_id,
					CombinedStats {
						storage,
						cdc,
					},
				)
			})
			.collect();

		Ok(results)
	}

	/// Get combined stats for a specific object and tier.
	pub fn get(&self, tier: Tier, id: MetricId) -> reifydb_type::Result<Option<CombinedStats>> {
		let storage = self.storage_reader.get(tier, id)?;
		let cdc = self.cdc_reader.get(id)?;

		match (storage, cdc) {
			(Some(storage), cdc) => Ok(Some(CombinedStats {
				storage,
				cdc: cdc.unwrap_or_default(),
			})),
			(None, Some(cdc)) => Ok(Some(CombinedStats {
				storage: MultiStorageStats::default(),
				cdc,
			})),
			(None, None) => Ok(None),
		}
	}

	/// Get the underlying storage stats reader.
	pub fn storage_reader(&self) -> &StorageStatsReader<S> {
		&self.storage_reader
	}

	/// Get the underlying CDC stats reader.
	pub fn cdc_reader(&self) -> &CdcStatsReader<S> {
		&self.cdc_reader
	}
}
