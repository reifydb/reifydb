// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashSet;

use crate::{
	dataset::{DEFAULT_INSERT_BATCH, Dataset},
	error::ScenarioError,
	profile::{Profile, Scale},
	query::{NamedQuery, OperationKind},
	render::insert_statements,
};

pub struct Scenario {
	pub name: &'static str,
	pub description: &'static str,
	pub dataset: Dataset,
	pub queries: Vec<NamedQuery>,
	pub profiles: Vec<Profile>,
	pub teardown: Vec<String>,
}

pub struct Statement {
	pub rql: String,
	pub kind: OperationKind,
}

impl Scenario {
	pub fn query(&self, name: &str) -> Option<&NamedQuery> {
		self.queries.iter().find(|query| query.name == name)
	}

	pub fn profile(&self, name: &str) -> Option<&Profile> {
		self.profiles.iter().find(|profile| profile.name == name)
	}

	pub fn setup_statements(&self, scale: u64) -> Vec<Statement> {
		let mut statements: Vec<Statement> = self
			.dataset
			.ddl()
			.iter()
			.map(|rql| Statement {
				rql: rql.clone(),
				kind: OperationKind::Admin,
			})
			.collect();

		match &self.dataset {
			Dataset::Manual(dataset) => {
				statements.extend(dataset.rows.iter().map(|rql| Statement {
					rql: rql.clone(),
					kind: OperationKind::Command,
				}));
			}
			Dataset::Generated(dataset) => {
				for seed in &dataset.seeds {
					statements.extend(insert_statements(
						seed.table,
						seed.columns,
						seed.rows(scale),
						DEFAULT_INSERT_BATCH,
					)
					.into_iter()
					.map(|rql| Statement {
						rql,
						kind: OperationKind::Command,
					}));
				}
			}
		}

		statements
	}

	pub fn teardown_statements(&self) -> Vec<Statement> {
		self.teardown
			.iter()
			.map(|rql| Statement {
				rql: rql.clone(),
				kind: OperationKind::Admin,
			})
			.collect()
	}

	pub fn validate(&self) -> Result<(), ScenarioError> {
		if self.queries.is_empty() {
			return Err(ScenarioError::NoQueries {
				scenario: self.name.to_string(),
			});
		}

		if self.profiles.is_empty() {
			return Err(ScenarioError::NoProfiles {
				scenario: self.name.to_string(),
			});
		}

		let mut seen_queries = HashSet::new();
		for query in &self.queries {
			if !seen_queries.insert(query.name) {
				return Err(ScenarioError::DuplicateQuery {
					scenario: self.name.to_string(),
					query: query.name.to_string(),
				});
			}
		}

		let mut seen_profiles = HashSet::new();
		for profile in &self.profiles {
			if !seen_profiles.insert(profile.name.as_str()) {
				return Err(ScenarioError::DuplicateProfile {
					scenario: self.name.to_string(),
					profile: profile.name.clone(),
				});
			}

			match (self.dataset.is_manual(), profile.scale) {
				(true, Scale::Rows(_)) => {
					return Err(ScenarioError::ScaledProfileOnManualDataset {
						scenario: self.name.to_string(),
						profile: profile.name.clone(),
					});
				}
				(false, Scale::Fixed) => {
					return Err(ScenarioError::FixedProfileOnGeneratedDataset {
						scenario: self.name.to_string(),
						profile: profile.name.clone(),
					});
				}
				_ => {}
			}
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::Value;

	use crate::{
		dataset::{Dataset, RowCount, TableSeed},
		error::ScenarioError,
		profile::{Profile, StopCondition},
		query::{NamedQuery, OperationKind, QueryTemplate},
		scenario::Scenario,
	};

	fn generated_dataset() -> Dataset {
		Dataset::generated(
			vec!["create table bench::users { id: int8, name: utf8 }".to_string()],
			vec![TableSeed {
				table: "bench::users",
				columns: &["id", "name"],
				count: RowCount::Scaled,
				row: |index, _| vec![Value::Int8(index as i64), Value::Utf8(format!("user_{}", index))],
			}],
		)
	}

	fn scenario(dataset: Dataset, profiles: Vec<Profile>) -> Scenario {
		Scenario {
			name: "test",
			description: "test",
			dataset,
			queries: vec![NamedQuery::query(
				"lookup",
				QueryTemplate::Fixed("from bench::users".to_string()),
			)],
			profiles,
			teardown: vec!["drop namespace bench".to_string()],
		}
	}

	#[test]
	fn generated_setup_emits_ddl_then_batched_inserts() {
		let scenario =
			scenario(generated_dataset(), vec![Profile::scaled(1, 10_000, StopCondition::Iterations(1))]);
		let statements = scenario.setup_statements(2500);

		// DDL is rejected outright on the command path, so the kind has to say Admin here; the
		// engine test in tests/engine.rs is what proved this rather than the type checker.
		assert_eq!(statements[0].rql, "create table bench::users { id: int8, name: utf8 }");
		assert_eq!(statements[0].kind, OperationKind::Admin);
		assert_eq!(statements[1].kind, OperationKind::Command);
		// 2500 rows at the default batch of 1000 is three inserts, so four statements total.
		assert_eq!(statements.len(), 4);
		assert!(statements[1].rql.starts_with("INSERT bench::users [{ id: 0, name: \"user_0\" }"));
	}

	#[test]
	fn generated_setup_row_count_tracks_the_requested_scale() {
		let scenario =
			scenario(generated_dataset(), vec![Profile::scaled(1, 10_000, StopCondition::Iterations(1))]);

		// The seeder is the only thing standing between a "1m" profile label and a table that
		// actually holds a million rows; a scale it ignored would silently benchmark 10k.
		assert_eq!(scenario.setup_statements(10_000).len(), 1 + 10);
		assert_eq!(scenario.setup_statements(1_000_000).len(), 1 + 1000);
	}

	#[test]
	fn manual_setup_passes_hand_written_rows_through_verbatim() {
		let dataset = Dataset::manual(
			vec!["create table bench::t { id: int8 }".to_string()],
			vec!["INSERT bench::t [{ id: 1 }, { id: 2 }]".to_string()],
		);
		let scenario = scenario(dataset, vec![Profile::fixed(1, StopCondition::Iterations(1))]);
		let statements = scenario.setup_statements(0);

		assert_eq!(statements.len(), 2);
		assert_eq!(statements[1].rql, "INSERT bench::t [{ id: 1 }, { id: 2 }]");
	}

	#[test]
	fn manual_dataset_rejects_a_scaled_profile() {
		// A hand-written fixture is the size it is. Accepting Scale::Rows would report results
		// under a "1m" label while running against whatever the literal rows happened to be.
		let dataset = Dataset::manual(vec![], vec![]);
		let scenario = scenario(dataset, vec![Profile::scaled(1, 1_000_000, StopCondition::Iterations(1))]);

		assert_eq!(
			scenario.validate(),
			Err(ScenarioError::ScaledProfileOnManualDataset {
				scenario: "test".to_string(),
				profile: "t1_1m".to_string(),
			})
		);
	}

	#[test]
	fn generated_dataset_rejects_a_fixed_profile() {
		// Without a scale a generated seed would produce zero rows, so every query would run
		// against an empty table and report suspiciously good numbers.
		let scenario = scenario(generated_dataset(), vec![Profile::fixed(1, StopCondition::Iterations(1))]);

		assert_eq!(
			scenario.validate(),
			Err(ScenarioError::FixedProfileOnGeneratedDataset {
				scenario: "test".to_string(),
				profile: "t1".to_string(),
			})
		);
	}

	#[test]
	fn duplicate_profile_names_are_rejected() {
		// Duplicate names make --profile ambiguous and would make two rows of a report
		// indistinguishable.
		let scenario = scenario(
			generated_dataset(),
			vec![
				Profile::scaled(1, 10_000, StopCondition::Iterations(1)),
				Profile::scaled(1, 10_000, StopCondition::Iterations(2)),
			],
		);

		assert_eq!(
			scenario.validate(),
			Err(ScenarioError::DuplicateProfile {
				scenario: "test".to_string(),
				profile: "t1_10k".to_string(),
			})
		);
	}

	#[test]
	fn a_well_formed_scenario_validates() {
		let scenario =
			scenario(generated_dataset(), vec![Profile::scaled(1, 10_000, StopCondition::Iterations(1))]);
		assert_eq!(scenario.validate(), Ok(()));
		assert!(scenario.query("lookup").is_some());
		assert!(scenario.query("missing").is_none());
	}
}
