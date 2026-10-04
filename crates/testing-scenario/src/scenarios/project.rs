// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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
		name: "project",
		description: "Map every row of a seeded table: plain columns, a cast and arithmetic",
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
			"map_all",
			QueryTemplate::Fixed(format!(
				"from {}::users map {{ id, name, n: cast(id, utf8), m: id + 1 }}",
				NAMESPACE
			)),
		)],
		profiles: scaled_matrix(&THREADS, &SCALES, StopCondition::Iterations(ITERATIONS)),
		teardown: vec![drop_namespace()],
	}
}
