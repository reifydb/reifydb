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

pub const ITERATIONS: u64 = 20_000;
pub const WINDOW: u64 = 100;
pub const FULL_SCAN_MATCHES: u64 = 50;

pub fn scenario() -> Scenario {
	Scenario {
		name: "scan",
		description: "Bounded range scans with a filter, starting from a random offset",
		dataset: Dataset::generated(
			vec![create_namespace(), create_users()],
			vec![TableSeed {
				table: "bench::users",
				columns: USERS_COLUMNS,
				count: RowCount::Scaled,
				row: user_row,
			}],
		),
		queries: vec![
			NamedQuery::query(
				"range_scan",
				QueryTemplate::Parameterized(|rng, scale| {
					let start = rng.random_range(0..scale.saturating_sub(WINDOW).max(1));
					format!("from {}::users filter id > {} take {}", NAMESPACE, start, WINDOW)
				}),
			),
			NamedQuery::query(
				"full_scan",
				QueryTemplate::Fixed(format!(
					"from {}::users filter id < {} take {}",
					NAMESPACE, FULL_SCAN_MATCHES, WINDOW
				)),
			),
		],
		profiles: scaled_matrix(&THREADS, &SCALES, StopCondition::Iterations(ITERATIONS)),
		teardown: vec![drop_namespace()],
	}
}
