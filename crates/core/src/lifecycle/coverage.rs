// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_runtime::sync::mutex::Mutex;

use crate::lifecycle::class::RetentionClass;

#[derive(Clone)]
pub struct RetentionCoverage {
	owners: Arc<Mutex<BTreeMap<RetentionClass, &'static str>>>,
}

impl RetentionCoverage {
	pub fn new() -> Self {
		Self {
			owners: Arc::new(Mutex::new(BTreeMap::new())),
		}
	}

	pub fn cover(&self, class: RetentionClass, owner: &'static str) {
		self.owners.lock().entry(class).or_insert(owner);
	}

	pub fn owner(&self, class: RetentionClass) -> Option<&'static str> {
		self.owners.lock().get(&class).copied()
	}

	pub fn is_covered(&self, class: RetentionClass) -> bool {
		self.owners.lock().contains_key(&class)
	}

	pub fn len(&self) -> usize {
		self.owners.lock().len()
	}

	pub fn is_empty(&self) -> bool {
		self.owners.lock().is_empty()
	}
}

impl Default for RetentionCoverage {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_class_reclaimed_outside_the_lifecycle_subsystem_still_counts_as_covered() {
		// The group classes are reclaimed on the flow tick, not by a LifecycleTask, so a covered set
		// derived only from this subsystem's tasks reports them unreclaimed on every boot - an error
		// indistinguishable from a genuinely dead lane. Coverage is declared by whoever executes it.
		let coverage = RetentionCoverage::new();
		coverage.cover(RetentionClass::TombstoneReap, "tombstone-reap");
		coverage.cover(RetentionClass::OperatorGroupData, "flow-tick-reclaim");

		assert_eq!(coverage.owner(RetentionClass::OperatorGroupData), Some("flow-tick-reclaim"));
		assert!(coverage.is_covered(RetentionClass::TombstoneReap));
		assert!(
			!coverage.is_covered(RetentionClass::OperatorGroupIdentity),
			"a class nobody claimed must stay uncovered so the report can still name it"
		);
	}

	#[test]
	fn the_first_owner_of_a_class_keeps_it() {
		// Registration order across subsystems is a builder detail; keeping the first registrant makes
		// the reported owner stable when subsystems are reordered.
		let coverage = RetentionCoverage::new();
		coverage.cover(RetentionClass::VacuumBudget, "vacuum-budget");
		coverage.cover(RetentionClass::VacuumBudget, "someone-else");

		assert_eq!(coverage.owner(RetentionClass::VacuumBudget), Some("vacuum-budget"));
		assert_eq!(coverage.len(), 1, "a second claim must not create a second entry");
	}

	#[test]
	fn a_fresh_registry_claims_nothing() {
		let coverage = RetentionCoverage::new();

		assert!(coverage.is_empty());
		for class in RetentionClass::all() {
			assert!(!coverage.is_covered(*class), "{} must start uncovered", class.name());
		}
	}
}
