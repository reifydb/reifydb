// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	dataset::Dataset,
	profile::{StopCondition, THREADS, fixed_matrix},
	query::{NamedQuery, QueryTemplate},
	scenario::Scenario,
	scenarios::{NAMESPACE, create_namespace, create_users, drop_namespace},
};

pub const ITERATIONS: u64 = 50_000;

pub fn scenario() -> Scenario {
	Scenario {
		name: "write",
		description: "Sequential inserts into an empty table",
		dataset: Dataset::manual(vec![create_namespace(), create_users()], Vec::new()),
		queries: vec![NamedQuery::command(
			"insert",
			QueryTemplate::Sequential(|sequence| {
				format!(
					"INSERT {}::users [{{ id: {}, name: \"user_{}\", email: \"user_{}@bench.test\" }}]",
					NAMESPACE, sequence, sequence, sequence
				)
			}),
		)],
		profiles: fixed_matrix(&THREADS, StopCondition::Iterations(ITERATIONS)),
		teardown: vec![drop_namespace()],
	}
}
