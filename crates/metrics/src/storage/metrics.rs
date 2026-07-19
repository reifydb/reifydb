// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::interface::store::{SingleVersionStore, Tier};
use reifydb_value::Result;

use crate::{
	MetricsId,
	storage::{
		cdc::{CdcMetrics, CdcMetricsReader},
		multi::{MultiStorageMetrics, StorageMetricsReader},
	},
};

#[derive(Debug, Clone, Default)]
pub struct CombinedMetrics {
	pub storage: MultiStorageMetrics,
	pub cdc: CdcMetrics,
}

impl CombinedMetrics {
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
pub struct MetricsReader<S> {
	storage: StorageMetricsReader<S>,
	cdc: CdcMetricsReader<S>,
}

impl<S: SingleVersionStore> MetricsReader<S> {
	pub fn new(storage: S) -> Self {
		Self {
			storage: StorageMetricsReader::new(storage.clone()),
			cdc: CdcMetricsReader::new(storage),
		}
	}

	pub fn scan_tier(&self, tier: Tier) -> Result<Vec<(MetricsId, CombinedMetrics)>> {
		let storage_stats = self.storage.scan_tier(tier)?;

		let cdc_stats: HashMap<MetricsId, CdcMetrics> = self.cdc.scan_all()?.into_iter().collect();

		let results: Vec<(MetricsId, CombinedMetrics)> = storage_stats
			.into_iter()
			.map(|(obj_id, storage)| {
				let cdc = cdc_stats.get(&obj_id).cloned().unwrap_or_default();
				(
					obj_id,
					CombinedMetrics {
						storage,
						cdc,
					},
				)
			})
			.collect();

		Ok(results)
	}

	pub fn get(&self, tier: Tier, id: MetricsId) -> Result<Option<CombinedMetrics>> {
		let storage = self.storage.get(tier, id)?;
		let cdc = self.cdc.get(id)?;

		match (storage, cdc) {
			(Some(storage), cdc) => Ok(Some(CombinedMetrics {
				storage,
				cdc: cdc.unwrap_or_default(),
			})),
			(None, Some(cdc)) => Ok(Some(CombinedMetrics {
				storage: MultiStorageMetrics::default(),
				cdc,
			})),
			(None, None) => Ok(None),
		}
	}

	pub fn storage_reader(&self) -> &StorageMetricsReader<S> {
		&self.storage
	}

	pub fn cdc_reader(&self) -> &CdcMetricsReader<S> {
		&self.cdc
	}
}
