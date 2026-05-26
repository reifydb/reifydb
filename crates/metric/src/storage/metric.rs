// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::store::{SingleVersionStore, Tier};
use reifydb_type::Result;

use crate::{
	MetricId,
	storage::{
		cdc::{CdcStats, CdcStatsReader},
		multi::{MultiStorageStats, StorageStatsReader},
	},
};

#[derive(Debug, Clone, Default)]
pub struct CombinedStats {
	pub storage: MultiStorageStats,
	pub cdc: CdcStats,
}

impl CombinedStats {
	pub fn current_bytes(&self) -> u64 {
		self.storage.current_bytes()
	}

	pub fn historical_bytes(&self) -> u64 {
		self.storage.historical_bytes()
	}

	pub fn total_bytes(&self) -> u64 {
		self.storage.total_bytes()
	}

	pub fn cdc_total_bytes(&self) -> u64 {
		self.cdc.total_bytes()
	}
}

#[derive(Clone)]
pub struct MetricReader<S> {
	storage: StorageStatsReader<S>,
	cdc: CdcStatsReader<S>,
}

impl<S: SingleVersionStore> MetricReader<S> {
	pub fn new(storage: S) -> Self {
		Self {
			storage: StorageStatsReader::new(storage.clone()),
			cdc: CdcStatsReader::new(storage),
		}
	}

	pub fn scan_tier(&self, tier: Tier) -> Result<Vec<(MetricId, CombinedStats)>> {
		let storage_stats = self.storage.scan_tier(tier)?;

		let cdc_stats: HashMap<MetricId, CdcStats> = self.cdc.scan_all()?.into_iter().collect();

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

	pub fn get(&self, tier: Tier, id: MetricId) -> Result<Option<CombinedStats>> {
		let storage = self.storage.get(tier, id)?;
		let cdc = self.cdc.get(id)?;

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

	pub fn storage_reader(&self) -> &StorageStatsReader<S> {
		&self.storage
	}

	pub fn cdc_reader(&self) -> &CdcStatsReader<S> {
		&self.cdc
	}
}
