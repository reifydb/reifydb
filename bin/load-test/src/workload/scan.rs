// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use rand::{Rng, rngs::StdRng};

use crate::{
	client::Operation,
	workload::{SetupQuery, Workload},
};

/// Scan workload - table scans with filters
///
/// Pre-populates a table and performs range scans with LIMIT.
pub struct ScanWorkload {
	table_size: u64,
}

impl ScanWorkload {
	pub fn new(table_size: u64) -> Self {
		Self {
			table_size,
		}
	}
}

impl Workload for ScanWorkload {
	fn description(&self) -> &str {
		"SCAN (range scans)"
	}

	fn setup_queries(&self) -> Vec<SetupQuery> {
		let mut queries = vec![
			SetupQuery::command("create namespace if not exists bench"),
			SetupQuery::command("create table bench.users { id: int8, name: utf8, email: utf8 }"),
		];

		// Insert data in batches of 1000
		let batch_size = 1000u64;
		for batch_start in (0..self.table_size).step_by(batch_size as usize) {
			let batch_end = (batch_start + batch_size).min(self.table_size);
			let rows: Vec<String> = (batch_start..batch_end)
				.map(|i| {
					format!(
						"{{ id: {}, name: \"user_{}\", email: \"user_{}@bench.test\" }}",
						i, i, i
					)
				})
				.collect();

			queries.push(SetupQuery::command(format!("INSERT bench.users FROM [{}]", rows.join(", "))));
		}

		queries
	}

	fn next_operation(&self, rng: &mut StdRng, _worker_id: usize) -> Operation {
		// Random starting point for scan
		let start_id = rng.random_range(0..self.table_size.saturating_sub(100));
		Operation::Query(format!("from bench.users filter id > {} take 100", start_id))
	}

	fn teardown_queries(&self) -> Vec<String> {
		vec!["drop namespace bench".to_string()]
	}
}
