// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use rand::rngs::StdRng;
use reifydb_testing_scenario::{
	query::{NamedQuery, OperationKind},
	scenario::{Scenario, Statement},
};

use crate::client::Operation;

pub struct Runner {
	scenario: Scenario,
	query: usize,
	sequence: Arc<AtomicU64>,
	scale: u64,
}

impl Runner {
	pub fn new(scenario: Scenario, query: usize, scale: u64) -> Self {
		Self {
			scenario,
			query,
			sequence: Arc::new(AtomicU64::new(0)),
			scale,
		}
	}

	pub fn description(&self) -> String {
		format!("{} / {} ({})", self.scenario.name, self.query().name, self.scenario.description)
	}

	pub fn query(&self) -> &NamedQuery {
		&self.scenario.queries[self.query]
	}

	pub fn setup_operations(&self) -> Vec<Operation> {
		self.scenario.setup_statements(self.scale).into_iter().map(to_operation).collect()
	}

	pub fn teardown_operations(&self) -> Vec<Operation> {
		self.scenario.teardown_statements().into_iter().map(to_operation).collect()
	}

	pub fn next_operation(&self, rng: &mut StdRng) -> Operation {
		let query = self.query();
		let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
		let rql = query.rql.render(rng, self.scale, sequence);

		match query.kind {
			OperationKind::Query => Operation::Query(rql),
			OperationKind::Command => Operation::Command(rql),
			OperationKind::Admin => Operation::Admin(rql),
		}
	}
}

fn to_operation(statement: Statement) -> Operation {
	match statement.kind {
		OperationKind::Query => Operation::Query(statement.rql),
		OperationKind::Command => Operation::Command(statement.rql),
		OperationKind::Admin => Operation::Admin(statement.rql),
	}
}

pub fn select_query(scenario: &Scenario, requested: Option<&str>) -> Result<usize, String> {
	match requested {
		Some(name) => scenario.queries.iter().position(|query| query.name == name).ok_or_else(|| {
			format!(
				"scenario '{}' has no query '{}'; available: {}",
				scenario.name,
				name,
				scenario.queries.iter().map(|query| query.name).collect::<Vec<_>>().join(", ")
			)
		}),
		None if scenario.queries.len() == 1 => Ok(0),
		None => Err(format!(
			"scenario '{}' defines several queries, so --query is required; available: {}",
			scenario.name,
			scenario.queries.iter().map(|query| query.name).collect::<Vec<_>>().join(", ")
		)),
	}
}

#[cfg(test)]
mod tests {
	use rand::{SeedableRng, rngs::StdRng};
	use reifydb_testing_scenario::registry::by_name;

	use crate::{
		client::Operation,
		runner::{Runner, select_query},
	};

	#[test]
	fn setup_routes_ddl_to_admin_and_rows_to_command() {
		// DDL on the command path is rejected outright by the engine, so this routing is the
		// difference between a seeded table and a run against nothing.
		let scenario = by_name("read").unwrap();
		let runner = Runner::new(scenario, 0, 10);
		let operations = runner.setup_operations();

		assert!(matches!(operations[0], Operation::Admin(_)));
		assert!(matches!(operations[1], Operation::Admin(_)));
		assert!(matches!(operations[2], Operation::Command(_)));
	}

	#[test]
	fn teardown_is_admin_because_dropping_a_namespace_is_ddl() {
		let scenario = by_name("read").unwrap();
		let runner = Runner::new(scenario, 0, 10);

		let operations = runner.teardown_operations();
		assert_eq!(operations.len(), 1);
		assert!(matches!(operations[0], Operation::Admin(_)));
	}

	#[test]
	fn sequence_advances_across_calls_so_concurrent_writers_do_not_collide() {
		// Every worker shares one Runner; a per-worker counter would hand the same primary key
		// to several connections at once.
		let scenario = by_name("write").unwrap();
		let runner = Runner::new(scenario, 0, 0);
		let mut rng = StdRng::seed_from_u64(1);

		let first = runner.next_operation(&mut rng);
		let second = runner.next_operation(&mut rng);

		assert!(first.rql().contains("id: 0"), "{}", first.rql());
		assert!(second.rql().contains("id: 1"), "{}", second.rql());
	}

	#[test]
	fn write_operations_travel_on_the_command_path() {
		let scenario = by_name("write").unwrap();
		let runner = Runner::new(scenario, 0, 0);
		let mut rng = StdRng::seed_from_u64(1);

		assert!(matches!(runner.next_operation(&mut rng), Operation::Command(_)));
	}

	#[test]
	fn a_single_query_scenario_needs_no_explicit_selection() {
		let scenario = by_name("scan").unwrap();
		assert_eq!(select_query(&scenario, None), Ok(0));
	}

	#[test]
	fn an_unknown_query_name_is_rejected_with_the_available_names() {
		let scenario = by_name("scan").unwrap();
		let error = select_query(&scenario, Some("nope")).unwrap_err();

		assert!(error.contains("has no query 'nope'"), "{}", error);
		assert!(error.contains("range_scan"), "{}", error);
	}
}
