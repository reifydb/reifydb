// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use rand::{Rng, rngs::StdRng};

use crate::{
	client::Operation,
	workload::{SetupQuery, Workload},
};

/// Read workload - point lookups by primary key
///
/// Pre-populates a table with data and performs random point lookups.
pub struct ReadWorkload {
	table_size: u64,
}

impl ReadWorkload {
	pub fn new(table_size: u64) -> Self {
		Self {
			table_size,
		}
	}
}

impl Workload for ReadWorkload {
	fn description(&self) -> &str {
		"READ (point lookups)"
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

			queries.push(SetupQuery::command(format!("from [{}] insert bench.users", rows.join(", "))));
		}

		queries
	}

	fn next_operation(&self, rng: &mut StdRng, _worker_id: usize) -> Operation {
		let id = rng.random_range(0..self.table_size);
		Operation::Query(format!("from bench.users filter id == {}", id))
	}

	fn teardown_queries(&self) -> Vec<String> {
		vec!["drop namespace bench".to_string()]
	}
}
