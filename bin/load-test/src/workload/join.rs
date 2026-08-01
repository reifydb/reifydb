// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use rand::{RngExt, rngs::StdRng};

use crate::{
	client::Operation,
	workload::{SetupQuery, Workload},
};

/// Seeds customers and orders, then joins across the two.
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

		let batch_size = 1000u64;
		for batch_start in (0..self.table_size).step_by(batch_size as usize) {
			let batch_end = (batch_start + batch_size).min(self.table_size);
			let rows: Vec<String> = (batch_start..batch_end)
				.map(|i| format!("{{ id: {}, name: \"customer_{}\" }}", i, i))
				.collect();

			queries.push(SetupQuery::command(format!("INSERT bench.customers [{}]", rows.join(", "))));
		}

		// Three orders per customer, so the join fans out rather than matching one to one.
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

			queries.push(SetupQuery::command(format!("INSERT bench.orders [{}]", rows.join(", "))));
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
