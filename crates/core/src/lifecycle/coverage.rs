// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, sync::Arc};

use reifydb_runtime::sync::mutex::Mutex;

use crate::lifecycle::class::RetentionClass;

#[derive(Clone)]
pub struct RetentionCoverage {
	owners: Arc<Mutex<BTreeMap<RetentionClass, &'static str>>>,
	absences: Arc<Mutex<BTreeMap<RetentionClass, &'static str>>>,
}

impl RetentionCoverage {
	pub fn new() -> Self {
		Self {
			owners: Arc::new(Mutex::new(BTreeMap::new())),
			absences: Arc::new(Mutex::new(BTreeMap::new())),
		}
	}

	pub fn cover(&self, class: RetentionClass, owner: &'static str) {
		self.owners.lock().entry(class).or_insert(owner);
	}

	pub fn absent(&self, class: RetentionClass, reason: &'static str) {
		self.absences.lock().entry(class).or_insert(reason);
	}

	pub fn owner(&self, class: RetentionClass) -> Option<&'static str> {
		self.owners.lock().get(&class).copied()
	}

	pub fn absence(&self, class: RetentionClass) -> Option<&'static str> {
		self.absences.lock().get(&class).copied()
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
		// Coverage is declared by whoever executes it, wherever that lives: a covered set derived
		// only from the lifecycle subsystem's own tasks would report an externally reclaimed class
		// as unreclaimed on every boot - an error indistinguishable from a genuinely dead lane.
		let coverage = RetentionCoverage::new();
		coverage.cover(RetentionClass::TombstoneReap, "tombstone-reap");
		coverage.cover(RetentionClass::CdcTruncate, "cdc-subsystem");

		assert_eq!(coverage.owner(RetentionClass::CdcTruncate), Some("cdc-subsystem"));
		assert!(coverage.is_covered(RetentionClass::TombstoneReap));
		assert!(
			!coverage.is_covered(RetentionClass::EpochLog),
			"a class nobody claimed must stay uncovered so the report can still name it"
		);
	}

	#[test]
	fn the_first_owner_of_a_class_keeps_it() {
		// Registration order across subsystems is a builder detail; keeping the first registrant makes
		// the reported owner stable when subsystems are reordered.
		let coverage = RetentionCoverage::new();
		coverage.cover(RetentionClass::TombstoneReap, "tombstone-reap");
		coverage.cover(RetentionClass::TombstoneReap, "someone-else");

		assert_eq!(coverage.owner(RetentionClass::TombstoneReap), Some("tombstone-reap"));
		assert_eq!(coverage.len(), 1, "a second claim must not create a second entry");
	}

	#[test]
	fn a_fresh_registry_claims_nothing() {
		let coverage = RetentionCoverage::new();

		assert!(coverage.is_empty());
		for class in RetentionClass::all() {
			assert!(!coverage.is_covered(*class), "{} must start uncovered", class.name());
			assert!(coverage.absence(*class).is_none(), "{} must start with no absence", class.name());
		}
	}

	#[test]
	fn a_lane_declared_absent_is_explained_without_becoming_covered() {
		// Absence answers a different question than coverage: "nothing produces here" is not "something
		// reclaims here". Recording it as a pseudo-owner would report a reason string where the report
		// prints an executor name, and would fold the class into the covered set that liveness assertions
		// read - a lane nothing ever runs would then be expected to record slices.
		let coverage = RetentionCoverage::new();
		coverage.absent(RetentionClass::TombstoneReap, "store has no persistent tier");

		assert_eq!(coverage.absence(RetentionClass::TombstoneReap), Some("store has no persistent tier"));
		assert!(
			!coverage.is_covered(RetentionClass::TombstoneReap),
			"an absent lane has no executor, so it must not count as covered"
		);
		assert_eq!(coverage.owner(RetentionClass::TombstoneReap), None);
	}

	#[test]
	fn covering_a_class_never_declares_it_absent() {
		// The opposite conflation: a covered class picking up an absence would downgrade its report line
		// from "reclaimed by X" to "nothing produces here", hiding a live lane behind an excuse.
		let coverage = RetentionCoverage::new();
		coverage.cover(RetentionClass::EpochLog, "epoch-log");

		assert!(
			coverage.absence(RetentionClass::EpochLog).is_none(),
			"a class with an executor has no absence to report"
		);
	}

	#[test]
	fn the_first_reason_a_lane_is_declared_absent_is_the_one_reported() {
		// Same stability contract as the owner: declaration order across subsystems is a builder detail,
		// and a reason that changes with it makes the boot report unreproducible.
		let coverage = RetentionCoverage::new();
		coverage.absent(RetentionClass::CdcTruncate, "no cdc store registered");
		coverage.absent(RetentionClass::CdcTruncate, "some later excuse");

		assert_eq!(coverage.absence(RetentionClass::CdcTruncate), Some("no cdc store registered"));
	}
}
