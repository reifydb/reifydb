// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use rand::{Rng, rngs::StdRng};

use crate::{
	client::Operation,
	workload::{SetupQuery, Workload},
};

/// Join workload - join queries across two tables
///
/// Creates orders and customers tables, performs join queries.
pub struct JoinWorkload {
	table_size: u64,
}

impl JoinWorkload {
	pub fn new(table_size: u64) -> Self {
		Self {
			table_size,
		}
	}
}

impl Workload for JoinWorkload {
	fn description(&self) -> &str {
		"JOIN (two-table joins)"
	}

	fn setup_queries(&self) -> Vec<SetupQuery> {
		let mut queries = vec![
			SetupQuery::command("create namespace if not exists bench"),
			SetupQuery::command("create table bench.customers { id: int8, name: utf8 }"),
			SetupQuery::command("create table bench.orders { id: int8, customer_id: int8, amount: int4 }"),
		];

		// Insert customers in batches
		let batch_size = 1000u64;
		for batch_start in (0..self.table_size).step_by(batch_size as usize) {
			let batch_end = (batch_start + batch_size).min(self.table_size);
			let rows: Vec<String> = (batch_start..batch_end)
				.map(|i| format!("{{ id: {}, name: \"customer_{}\" }}", i, i))
				.collect();

			queries.push(SetupQuery::command(format!("INSERT bench.customers FROM [{}]", rows.join(", "))));
		}

		// Insert orders in batches (3 orders per customer on average)
		let order_count = self.table_size * 3;
		for batch_start in (0..order_count).step_by(batch_size as usize) {
			let batch_end = (batch_start + batch_size).min(order_count);
			let rows: Vec<String> = (batch_start..batch_end)
				.map(|i| {
					format!(
						"{{ id: {}, customer_id: {}, amount: {} }}",
						i,
						i % self.table_size,
						(i * 17) % 10000
					)
				})
				.collect();

			queries.push(SetupQuery::command(format!("INSERT bench.orders FROM [{}]", rows.join(", "))));
		}

		queries
	}

	fn next_operation(&self, rng: &mut StdRng, _worker_id: usize) -> Operation {
		let customer_id = rng.random_range(0..self.table_size);
		Operation::Query(format!(
			"from bench.orders filter customer_id == {} left join {{ from bench.customers }} c on c.id == customer_id",
			customer_id
		))
	}

	fn teardown_queries(&self) -> Vec<String> {
		vec!["drop namespace bench".to_string()]
	}
}
