// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use rand::RngExt;
use reifydb_value::value::Value;

use crate::{
	dataset::{Dataset, RowCount, TableSeed},
	profile::{SCALES, StopCondition, THREADS, scaled_matrix},
	query::{NamedQuery, QueryTemplate},
	scenario::Scenario,
	scenarios::{NAMESPACE, create_namespace, drop_namespace},
};

pub const ITERATIONS: u64 = 20_000;
pub const ORDERS_PER_CUSTOMER: u64 = 3;

const CUSTOMERS_COLUMNS: &[&str] = &["id", "name"];
const ORDERS_COLUMNS: &[&str] = &["id", "customer_id", "amount"];

fn create_customers() -> String {
	format!("create table {}::customers {{ id: int8, name: utf8 }}", NAMESPACE)
}

fn create_orders() -> String {
	format!("create table {}::orders {{ id: int8, customer_id: int8, amount: int4 }}", NAMESPACE)
}

fn customer_row(index: u64, _scale: u64) -> Vec<Value> {
	vec![Value::Int8(index as i64), Value::Utf8(format!("customer_{}", index))]
}

fn order_row(index: u64, scale: u64) -> Vec<Value> {
	vec![
		Value::Int8(index as i64),
		Value::Int8((index % scale.max(1)) as i64),
		Value::Int4(((index.wrapping_mul(17)) % 10_000) as i32),
	]
}

pub fn scenario() -> Scenario {
	Scenario {
		name: "join",
		description: "Left join of orders onto customers, fanning out three orders per customer",
		dataset: Dataset::generated(
			vec![create_namespace(), create_customers(), create_orders()],
			vec![
				TableSeed {
					table: "bench::customers",
					columns: CUSTOMERS_COLUMNS,
					count: RowCount::Scaled,
					row: customer_row,
				},
				TableSeed {
					table: "bench::orders",
					columns: ORDERS_COLUMNS,
					count: RowCount::ScaledTimes(ORDERS_PER_CUSTOMER),
					row: order_row,
				},
			],
		),
		queries: vec![
			NamedQuery::query(
				"left_join",
				QueryTemplate::Parameterized(|rng, scale| {
					format!(
						"from {}::orders filter customer_id == {} left join {{ from {}::customers }} as customers using (customer_id, customers.id)",
						NAMESPACE,
						rng.random_range(0..scale.max(1)),
						NAMESPACE
					)
				}),
			),
			NamedQuery::query(
				"full_join",
				QueryTemplate::Fixed(format!(
					"from {}::orders left join {{ from {}::customers }} as customers using (customer_id, customers.id)",
					NAMESPACE, NAMESPACE
				)),
			),
		],
		profiles: scaled_matrix(&THREADS, &SCALES, StopCondition::Iterations(ITERATIONS)),
		teardown: vec![drop_namespace()],
	}
}
