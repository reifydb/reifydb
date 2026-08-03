// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use rand::rngs::StdRng;

pub type ParameterizedQuery = fn(rng: &mut StdRng, scale: u64) -> String;
pub type SequentialQuery = fn(sequence: u64) -> String;

pub struct NamedQuery {
	pub name: &'static str,
	pub kind: OperationKind,
	pub rql: QueryTemplate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationKind {
	Query,
	Command,
	Admin,
}

pub enum QueryTemplate {
	Fixed(String),
	Parameterized(ParameterizedQuery),
	Sequential(SequentialQuery),
}

impl QueryTemplate {
	pub fn render(&self, rng: &mut StdRng, scale: u64, sequence: u64) -> String {
		match self {
			QueryTemplate::Fixed(rql) => rql.clone(),
			QueryTemplate::Parameterized(build) => build(rng, scale),
			QueryTemplate::Sequential(build) => build(sequence),
		}
	}

	pub fn is_deterministic(&self) -> bool {
		!matches!(self, QueryTemplate::Parameterized(_))
	}
}

impl NamedQuery {
	pub fn query(name: &'static str, rql: QueryTemplate) -> Self {
		Self {
			name,
			kind: OperationKind::Query,
			rql,
		}
	}

	pub fn command(name: &'static str, rql: QueryTemplate) -> Self {
		Self {
			name,
			kind: OperationKind::Command,
			rql,
		}
	}

	pub fn admin(name: &'static str, rql: QueryTemplate) -> Self {
		Self {
			name,
			kind: OperationKind::Admin,
			rql,
		}
	}
}

#[cfg(test)]
mod tests {
	use rand::{RngExt, SeedableRng, rngs::StdRng};

	use crate::query::{NamedQuery, OperationKind, QueryTemplate};

	#[test]
	fn fixed_template_ignores_rng_scale_and_sequence() {
		let template = QueryTemplate::Fixed("MAP 1".to_string());
		let mut rng = StdRng::seed_from_u64(1);

		assert_eq!(template.render(&mut rng, 10_000, 7), "MAP 1");
		assert_eq!(template.render(&mut rng, 0, 0), "MAP 1");
	}

	#[test]
	fn parameterized_template_replays_identically_from_the_same_seed() {
		// Reproducibility is the whole reason the seed is threaded through: a benchmark run and
		// its rerun must issue the same statements, or the comparison is meaningless.
		let template = QueryTemplate::Parameterized(|rng, scale| {
			format!("from bench::users filter id == {}", rng.random_range(0..scale))
		});

		let render_all = |seed: u64| {
			let mut rng = StdRng::seed_from_u64(seed);
			(0..5).map(|_| template.render(&mut rng, 1_000, 0)).collect::<Vec<String>>()
		};

		assert_eq!(render_all(42), render_all(42));
		assert_ne!(render_all(42), render_all(43));
	}

	#[test]
	fn parameterized_template_actually_varies_within_a_run() {
		// A generator that ignored the rng would turn a point-lookup workload into a single
		// hot key, which measures cache behaviour rather than lookup cost.
		let template = QueryTemplate::Parameterized(|rng, scale| format!("{}", rng.random_range(0..scale)));

		let mut rng = StdRng::seed_from_u64(7);
		let rendered: Vec<String> = (0..20).map(|_| template.render(&mut rng, 1_000_000, 0)).collect();

		assert!(rendered.iter().any(|value| value != &rendered[0]));
	}

	#[test]
	fn sequential_template_follows_the_supplied_sequence() {
		// Write workloads must not collide on primary keys across workers, so the sequence is
		// supplied by the runner rather than drawn from the rng.
		let template =
			QueryTemplate::Sequential(|sequence| format!("INSERT bench::t [{{ id: {} }}]", sequence));
		let mut rng = StdRng::seed_from_u64(1);

		assert_eq!(template.render(&mut rng, 0, 0), "INSERT bench::t [{ id: 0 }]");
		assert_eq!(template.render(&mut rng, 0, 41), "INSERT bench::t [{ id: 41 }]");
	}

	#[test]
	fn only_parameterized_templates_are_non_deterministic() {
		assert!(QueryTemplate::Fixed("MAP 1".to_string()).is_deterministic());
		assert!(QueryTemplate::Sequential(|sequence| sequence.to_string()).is_deterministic());
		assert!(!QueryTemplate::Parameterized(|_, _| String::new()).is_deterministic());
	}

	#[test]
	fn constructors_tag_the_execution_path() {
		// Routing a DML statement through the query path fails at the server, so the kind has
		// to travel with the statement rather than being guessed from its text.
		assert_eq!(
			NamedQuery::query("a", QueryTemplate::Fixed("MAP 1".to_string())).kind,
			OperationKind::Query
		);
		assert_eq!(
			NamedQuery::command("b", QueryTemplate::Fixed("INSERT bench::t [{ id: 1 }]".to_string())).kind,
			OperationKind::Command
		);
	}
}
