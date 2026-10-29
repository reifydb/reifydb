// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::{
	catalog::metrics::{MetricsId, storage::MultiStorageMetrics},
	store::{SingleVersionStore, Tier},
};
use reifydb_value::Result;

use crate::metrics::storage::multi::StorageMetricsReader;

#[derive(Debug, Clone, Default)]
pub struct CombinedMetrics {
	pub storage: MultiStorageMetrics,
}

impl CombinedMetrics {
	pub fn estimated_current_bytes(&self) -> u64 {
		self.storage.estimated_current_bytes()
	}

	pub fn estimated_historical_bytes(&self) -> u64 {
		self.storage.estimated_historical_bytes()
	}

	pub fn estimated_total_bytes(&self) -> u64 {
		self.storage.estimated_total_bytes()
	}
}

#[derive(Clone)]
pub struct MetricsReader<S> {
	storage: StorageMetricsReader<S>,
}

impl<S: SingleVersionStore> MetricsReader<S> {
	pub fn new(storage: S) -> Self {
		Self {
			storage: StorageMetricsReader::new(storage),
		}
	}

	pub fn scan_tier(&self, tier: Tier) -> Result<Vec<(MetricsId, CombinedMetrics)>> {
		Ok(self.storage
			.scan_tier(tier)?
			.into_iter()
			.map(|(obj_id, storage)| {
				(
					obj_id,
					CombinedMetrics {
						storage,
					},
				)
			})
			.collect())
	}

	pub fn get(&self, tier: Tier, id: MetricsId) -> Result<Option<CombinedMetrics>> {
		Ok(self.storage.get(tier, id)?.map(|storage| CombinedMetrics {
			storage,
		}))
	}

	pub fn storage_reader(&self) -> &StorageMetricsReader<S> {
		&self.storage
	}
}
