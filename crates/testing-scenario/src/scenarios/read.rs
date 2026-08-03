// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use rand::RngExt;

use crate::{
	dataset::{Dataset, RowCount, TableSeed},
	profile::{SCALES, StopCondition, THREADS, scaled_matrix},
	query::{NamedQuery, QueryTemplate},
	scenario::Scenario,
	scenarios::{NAMESPACE, USERS_COLUMNS, create_namespace, create_users, drop_namespace, user_row},
};

pub const ITERATIONS: u64 = 100_000;

pub fn scenario() -> Scenario {
	Scenario {
		name: "read",
		description: "Point lookups by primary key against a seeded table",
		dataset: Dataset::generated(
			vec![create_namespace(), create_users()],
			vec![TableSeed {
				table: "bench::users",
				columns: USERS_COLUMNS,
				count: RowCount::Scaled,
				row: user_row,
			}],
		),
		queries: vec![NamedQuery::query(
			"point_lookup",
			QueryTemplate::Parameterized(|rng, scale| {
				format!("from {}::users filter id == {}", NAMESPACE, rng.random_range(0..scale.max(1)))
			}),
		)],
		profiles: scaled_matrix(&THREADS, &SCALES, StopCondition::Iterations(ITERATIONS)),
		teardown: vec![drop_namespace()],
	}
}
