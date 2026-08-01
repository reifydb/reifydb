// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use rand::rngs::StdRng;

use crate::{
	client::Operation,
	workload::{SetupQuery, Workload},
};

/// Minimal single-row query, so the measurement is round-trip latency and nothing else.
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
