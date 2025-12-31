// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

use rand::{Rng, rngs::StdRng};

use crate::{
	client::Operation,
	workload::{SetupQuery, Workload},
};

/// Mixed workload - configurable read/write ratio
///
/// Pre-populates a table with data and performs a mix of reads and writes.
pub struct MixedWorkload {
	table_size: u64,
	read_percent: u8,
	#[allow(dead_code)]
	write_percent: u8,
	/// Counter for new write IDs
	write_counter: AtomicU64,
}

impl MixedWorkload {
	pub fn new(table_size: u64, read_percent: u8, write_percent: u8) -> Self {
		Self {
			table_size,
			read_percent,
			write_percent,
			write_counter: AtomicU64::new(table_size),
		}
	}
}

impl Workload for MixedWorkload {
	fn description(&self) -> &str {
		"MIXED (80% read, 20% write)"
	}

	fn setup_queries(&self) -> Vec<SetupQuery> {
		let mut queries = vec![
			SetupQuery::command("create namespace if not exists bench"),
			SetupQuery::command("create table bench.users { id: int8, name: utf8, email: utf8 }"),
		];

		// Insert initial data in batches of 1000
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

			queries.push(SetupQuery::command(format!("from [{}] insert bench.users", rows.join(", "))));
		}

		queries
	}

	fn next_operation(&self, rng: &mut StdRng, _worker_id: usize) -> Operation {
		let roll: u8 = rng.random_range(0..100);

		if roll < self.read_percent {
			// Read operation - random point lookup
			let id = rng.random_range(0..self.table_size);
			Operation::Query(format!("from bench.users filter id == {}", id))
		} else {
			// Write operation - insert new row
			let new_id = self.write_counter.fetch_add(1, Ordering::Relaxed);
			Operation::Command(format!(
				"from [{{ id: {}, name: \"user_{}\", email: \"user_{}@bench.test\" }}] insert bench.users",
				new_id, new_id, new_id
			))
		}
	}

	fn teardown_queries(&self) -> Vec<String> {
		vec!["drop namespace bench".to_string()]
	}
}
