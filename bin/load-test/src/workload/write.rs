// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

use rand::rngs::StdRng;

use crate::{
	client::Operation,
	workload::{SetupQuery, Workload},
};

/// Write workload - insert operations
///
/// Creates a table and performs sequential inserts.
pub struct WriteWorkload {
	/// Starting ID for inserts (after initial data)
	next_id: AtomicU64,
}

impl WriteWorkload {
	pub fn new(start_id: u64) -> Self {
		Self {
			next_id: AtomicU64::new(start_id),
		}
	}
}

impl Workload for WriteWorkload {
	fn description(&self) -> &str {
		"WRITE (inserts)"
	}

	fn setup_queries(&self) -> Vec<SetupQuery> {
		vec![
			SetupQuery::command("create namespace if not exists bench"),
			SetupQuery::command("create table bench.users { id: int8, name: utf8, email: utf8 }"),
		]
	}

	fn next_operation(&self, _rng: &mut StdRng, _worker_id: usize) -> Operation {
		let id = self.next_id.fetch_add(1, Ordering::Relaxed);
		Operation::Command(format!(
			"from [{{ id: {}, name: \"user_{}\", email: \"user_{}@bench.test\" }}] insert bench.users",
			id, id, id
		))
	}

	fn teardown_queries(&self) -> Vec<String> {
		vec!["drop namespace bench".to_string()]
	}
}
