// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	dataset::Dataset,
	profile::{StopCondition, THREADS, fixed_matrix},
	query::{NamedQuery, QueryTemplate},
	scenario::Scenario,
};

pub const ITERATIONS: u64 = 100_000;

pub fn scenario() -> Scenario {
	Scenario {
		name: "ping",
		description: "Baseline latency: a single-row map that touches no storage",
		dataset: Dataset::manual(Vec::new(), Vec::new()),
		queries: vec![NamedQuery::query("ping", QueryTemplate::Fixed("MAP { 1 }".to_string()))],
		profiles: fixed_matrix(&THREADS, StopCondition::Iterations(ITERATIONS)),
		teardown: Vec::new(),
	}
}
