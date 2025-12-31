// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use rand::rngs::StdRng;

use crate::{
	client::Operation,
	workload::{SetupQuery, Workload},
};

/// Ping workload - baseline latency test
///
/// Executes a minimal query that returns a single row.
/// This measures the minimum latency for a round-trip.
pub struct PingWorkload;

impl PingWorkload {
	pub fn new() -> Self {
		Self
	}
}

impl Workload for PingWorkload {
	fn description(&self) -> &str {
		"PING (baseline latency)"
	}

	fn setup_queries(&self) -> Vec<SetupQuery> {
		vec![]
	}

	fn next_operation(&self, _rng: &mut StdRng, _worker_id: usize) -> Operation {
		Operation::Query("MAP 1".to_string())
	}

	fn teardown_queries(&self) -> Vec<String> {
		vec![]
	}
}
