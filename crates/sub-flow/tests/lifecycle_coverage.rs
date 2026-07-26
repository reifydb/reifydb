// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The lifecycle boot report is the only place that answers "is anything reclaiming this class?",
//! and criterion 7 of the acceptance plan requires it to be trustworthy. Its covered-set used to be
//! derived purely from the lifecycle subsystem's own tasks, so the two classes reclaimed by the FLOW
//! tick were reported at ERROR as having no executor on every single boot of every ingestor - while
//! the reclaim pass was in fact running thousands of times a minute. An alarm that is always wrong
//! is worse than no alarm: it is indistinguishable from the real thing, so the real thing gets
//! ignored. Coverage is now declared by whoever executes it, wherever that lives.

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

/// Executors that exist only when the store has the tier they operate on: the flush lane needs a
/// flush engine, and reaping and vacuuming need a persistent tier. On a memory store the factory
/// skips registering them, so their classes have no owner and the boot report names them - which is
/// accurate, not a defect. Every OTHER class must be claimed on any store.
const STORAGE_CONDITIONAL: &[RetentionClass] =
	&[RetentionClass::PersistentFlush, RetentionClass::TombstoneReap, RetentionClass::VacuumBudget];

#[test]
fn every_retention_class_a_memory_database_can_reclaim_has_an_owner() {
	// This is the assertion the boot report's ERROR branch exists to make: with flow on, anything
	// left unclaimed is either a genuinely unreclaimed class or a lane that forgot to declare
	// itself, and both need a human. Scoping it to what a memory store can actually reclaim keeps
	// that signal - a newly added or newly forgotten lane still fails here - without asserting the
	// storage-conditional lanes into existence on a store that has no tier for them to work on.
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
	// Pins WHY the exclusion above is legitimate, so it cannot quietly become a blanket exemption.
	// If any of these ever gets claimed on a memory store, either the factory started registering
	// an executor with nothing to operate on, or the class stopped being storage-conditional - and
	// then the exclusion is hiding a lane that should be asserted like every other.
	let coverage = coverage_of_a_flow_enabled_database();

	for class in STORAGE_CONDITIONAL {
		assert!(
			!coverage.is_covered(*class),
			"{class} is claimed on a memory store, so it is no longer storage-conditional and must \
			 rejoin the assertion above"
		);
	}
}
