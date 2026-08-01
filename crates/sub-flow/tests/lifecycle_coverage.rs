// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The lifecycle boot report is the only place that answers "is anything reclaiming this class?".
//! Coverage is declared by whoever executes it, wherever that lives: a covered-set derived only
//! from the lifecycle subsystem's own tasks reports the flow-tick classes at ERROR on every boot.

use reifydb::{WithSubsystem, embedded};
use reifydb_core::lifecycle::{class::RetentionClass, coverage::RetentionCoverage};

const FLOW_TICK_RECLAIM: &str = "flow-tick-reclaim";

fn coverage_of_a_flow_enabled_database() -> RetentionCoverage {
	let db = embedded::memory().with_flow(|f| f).build().expect("memory database with flow must build");
	db.engine()
		.ioc()
		.try_resolve::<RetentionCoverage>()
		.expect("the builder must register RetentionCoverage before any subsystem is created")
}

#[test]
fn the_flow_tick_reclaimer_declares_the_operator_classes_it_owns() {
	// Neither class is reclaimed by a LifecycleTask - the group reclaim driver runs inside
	// FlowTransaction on the flow tick. Registration is what makes that visible to the report
	// instead of leaving it to look like a dead lane.
	let coverage = coverage_of_a_flow_enabled_database();

	assert_eq!(coverage.owner(RetentionClass::OperatorGroupData), Some(FLOW_TICK_RECLAIM));
	assert_eq!(coverage.owner(RetentionClass::OperatorGroupIdentity), Some(FLOW_TICK_RECLAIM));
}

#[test]
fn a_lifecycle_task_still_owns_the_classes_it_registered() {
	// The registry must not have become a flow-only side channel: the subsystem's own tasks claim
	// their classes through the same path, and the owner reported is the task's name. If this
	// regressed, every non-flow class would start reporting as unreclaimed.
	let coverage = coverage_of_a_flow_enabled_database();

	assert_eq!(coverage.owner(RetentionClass::EpochLog), Some("epoch-log"));
	assert_eq!(coverage.owner(RetentionClass::CompactionReclaim), Some("compaction-reclaim"));
}

/// Executors that exist only when the store has the tier they operate on. On a memory store the
/// factory skips them, so their classes have no owner and the boot report names them - accurate,
/// not a defect. Every other class must be claimed on any store.
const STORAGE_CONDITIONAL: &[RetentionClass] =
	&[RetentionClass::PersistentFlush, RetentionClass::TombstoneReap, RetentionClass::VacuumBudget];

#[test]
fn every_retention_class_a_memory_database_can_reclaim_has_an_owner() {
	// The report's ERROR branch exists to make this claim: with flow on, anything unclaimed is
	// either genuinely unreclaimed or a lane that forgot to declare itself. Scoping to what a
	// memory store can reclaim keeps that signal without asserting absent tiers into existence.
	let coverage = coverage_of_a_flow_enabled_database();

	let unclaimed: Vec<&str> = RetentionClass::all()
		.iter()
		.filter(|c| !STORAGE_CONDITIONAL.contains(c))
		.filter(|c| !coverage.is_covered(**c))
		.map(|c| c.name())
		.collect();

	assert!(unclaimed.is_empty(), "no executor declared for: {unclaimed:?}");
}

#[test]
fn the_storage_conditional_lanes_are_unclaimed_here_because_the_tier_is_absent() {
	// Pins why the exclusion above is legitimate so it cannot become a blanket exemption: if one of
	// these is ever claimed on a memory store, the class stopped being storage-conditional and the
	// exclusion is hiding a lane that should be asserted like every other.
	let coverage = coverage_of_a_flow_enabled_database();

	for class in STORAGE_CONDITIONAL {
		assert!(
			!coverage.is_covered(*class),
			"{class} is claimed on a memory store, so it is no longer storage-conditional and must \
			 rejoin the assertion above"
		);
	}
}
