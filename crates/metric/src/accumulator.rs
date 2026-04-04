// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use dashmap::DashMap;
use reifydb_core::fingerprint::StatementFingerprint;

use crate::statement::StatementStats;

/// Lock-free per-fingerprint query stats accumulator.
pub struct StatementStatsAccumulator {
	map: DashMap<StatementFingerprint, Arc<StatementStats>>,
}

impl Default for StatementStatsAccumulator {
	fn default() -> Self {
		Self::new()
	}
}

impl StatementStatsAccumulator {
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
		// Fast path: read lock only, no Arc clone
		if let Some(stats) = self.map.get(&fingerprint) {
			stats.record(duration_us, compute_us, rows, success);
			return;
		}

		// Slow path: write lock for insertion
		let stats =
			self.map.entry(fingerprint)
				.or_insert_with(|| Arc::new(StatementStats::new(normalized_rql.to_owned())))
				.clone();

		stats.record(duration_us, compute_us, rows, success);
	}

	#[must_use]
	pub fn snapshot(&self) -> Vec<(StatementFingerprint, Arc<StatementStats>)> {
		self.map.iter().map(|entry| (*entry.key(), entry.value().clone())).collect()
	}
}
