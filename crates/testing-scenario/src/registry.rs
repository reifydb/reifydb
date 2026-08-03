// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::{
	scenario::Scenario,
	scenarios::{join, ping, read, scan, write},
};

pub fn all() -> Vec<Scenario> {
	vec![ping::scenario(), read::scenario(), write::scenario(), scan::scenario(), join::scenario()]
}

pub fn names() -> Vec<&'static str> {
	all().into_iter().map(|scenario| scenario.name).collect()
}

pub fn by_name(name: &str) -> Option<Scenario> {
	all().into_iter().find(|scenario| scenario.name == name)
}

#[cfg(test)]
mod tests {
	use std::collections::HashSet;

	use crate::{
		profile::Scale,
		query::{OperationKind, QueryTemplate},
		registry::{all, by_name, names},
	};

	#[test]
	fn every_registered_scenario_validates() {
		// Validation is the only thing that catches a scenario whose profiles contradict its
		// dataset, and a broken scenario would otherwise surface as a confusing runtime failure
		// deep inside a benchmark or load run.
		for scenario in all() {
			assert_eq!(scenario.validate(), Ok(()), "scenario '{}' failed validation", scenario.name);
		}
	}

	#[test]
	fn the_migrated_workloads_are_all_present() {
		// These are the load tester's existing --workload presets, less `mixed`, which was an
		// 80/20 read-write blend and cannot be expressed now that profiles carry no weights.
		// Losing any of the rest is a silent capability regression for an existing invocation.
		let mut registered = names();
		registered.sort();
		assert_eq!(registered, vec!["join", "ping", "read", "scan", "write"]);
	}

	#[test]
	fn scenario_names_are_unique() {
		let registered = names();
		let unique: HashSet<&str> = registered.iter().copied().collect();
		assert_eq!(unique.len(), registered.len());
	}

	#[test]
	fn lookup_by_name_finds_registered_scenarios_only() {
		assert!(by_name("scan").is_some());
		assert!(by_name("nope").is_none());
	}

	#[test]
	fn generated_scenarios_seed_rows_at_every_declared_scale() {
		// A generated scenario whose profile scale produced no inserts would benchmark an empty
		// table, which reads as a spectacular and entirely fake speedup.
		for scenario in all() {
			if scenario.dataset.is_manual() {
				continue;
			}

			for profile in &scenario.profiles {
				let rows = profile.scale.rows();
				assert!(
					scenario.dataset.row_count(rows) >= rows,
					"scenario '{}' profile '{}' seeds fewer rows than its scale",
					scenario.name,
					profile.name
				);
			}
		}
	}

	#[test]
	fn manual_scenarios_pair_with_fixed_profiles_and_generated_ones_with_scaled() {
		for scenario in all() {
			for profile in &scenario.profiles {
				let fixed = matches!(profile.scale, Scale::Fixed);
				assert_eq!(
					fixed,
					scenario.dataset.is_manual(),
					"scenario '{}' profile '{}' pairs the wrong scale with its dataset",
					scenario.name,
					profile.name
				);
			}
		}
	}

	#[test]
	fn write_paths_are_tagged_as_commands() {
		// An INSERT routed through the query path is rejected by the server, so the scenario
		// definitions must carry the right kind rather than relying on the runner to guess.
		for scenario in all() {
			for query in &scenario.queries {
				if let QueryTemplate::Sequential(_) = query.rql {
					assert_eq!(
						query.kind,
						OperationKind::Command,
						"scenario '{}' query '{}' inserts through the query path",
						scenario.name,
						query.name
					);
				}
			}
		}
	}
}
