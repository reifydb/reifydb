// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use dashmap::DashMap;
use reifydb_core::fingerprint::StatementFingerprint;

use crate::statement::StatementMetricsAggregate;

pub struct StatementMetricsAccumulator {
	map: DashMap<StatementFingerprint, Arc<StatementMetricsAggregate>>,
}

impl Default for StatementMetricsAccumulator {
	fn default() -> Self {
		Self::new()
	}
}

impl StatementMetricsAccumulator {
	pub fn new() -> Self {
		Self {
			map: DashMap::new(),
		}
	}

	pub fn record(
		&self,
		fingerprint: StatementFingerprint,
		normalized_rql: &str,
		duration_us: u64,
		compute_us: u64,
		rows: u64,
		success: bool,
	) {
		if let Some(stats) = self.map.get(&fingerprint) {
			stats.record(duration_us, compute_us, rows, success);
			return;
		}

		let stats =
			self.map.entry(fingerprint)
				.or_insert_with(|| Arc::new(StatementMetricsAggregate::new(normalized_rql.to_owned())))
				.clone();

		stats.record(duration_us, compute_us, rows, success);
	}

	#[must_use]
	pub fn snapshot(&self) -> Vec<(StatementFingerprint, Arc<StatementMetricsAggregate>)> {
		self.map.iter().map(|entry| (*entry.key(), entry.value().clone())).collect()
	}
}
