// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Registration matrix for the lifecycle subsystem.
//!
//! Maintenance that silently is not running stays invisible unless something asserts the class should have been
//! there, so these tests assert the registered set by name, exhaustively. A class dropping out of the plane fails
//! here, and so does a new class added without a deliberate decision about whether it is always-on.

use std::sync::Arc;

use reifydb_core::{
	lifecycle::{
		class::RetentionClass, coverage::RetentionCoverage, metrics::RetentionMetrics,
		registry::LifecycleRegistry,
	},
	util::ioc::IocContainer,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{actor::system::ActorSpawner, sync::waiter::WaiterHandle};
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_sub_lifecycle::{
	actor::LifecycleMessage, factory::LifecycleSubsystemFactory, subsystem::LifecycleSubsystem,
};
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::duration::Duration;

/// Classes that must register on EVERY boot, under every store configuration. If one of these ever becomes
/// conditional, the leak it guards against comes back silently.
const ALWAYS_ON: [&str; 5] =
	["retention-evict-silent", "historical-gc", "epoch-log", "queue-lease-reap", "queue-retention"];

/// Classes this subsystem registers no lifecycle task for on this fixture; adding one is a reviewed decision that
/// exempts it from the coverage assertion below. Two need a persistent tier the memory store lacks and
/// cdc-truncate needs a CdcStore in the IoC.
const CONDITIONAL: [RetentionClass; 3] =
	[RetentionClass::PersistentFlush, RetentionClass::CdcTruncate, RetentionClass::TombstoneReap];

fn lifecycle(subsystem: &dyn Subsystem) -> &LifecycleSubsystem {
	subsystem
		.as_any()
		.downcast_ref::<LifecycleSubsystem>()
		.expect("the lifecycle factory must produce a LifecycleSubsystem")
}

fn task_names(subsystem: &dyn Subsystem) -> Vec<String> {
	let mut names: Vec<String> = lifecycle(subsystem).task_names().iter().map(|n| n.to_string()).collect();
	names.sort();
	names
}

fn create(ioc: &IocContainer) -> Box<dyn Subsystem> {
	Box::new(LifecycleSubsystemFactory).create(ioc).expect("the lifecycle subsystem must always start")
}

#[test]
fn registers_every_always_on_class_plus_cdc_truncation_when_a_cdc_store_exists() {
	let test_engine = TestEngine::new();
	let engine: StandardEngine = test_engine.inner().clone();
	let ioc = engine
		.ioc()
		.clone()
		.register(engine.clone())
		.register(LifecycleRegistry::new())
		.register(RetentionMetrics::new());

	let names = task_names(create(&ioc).as_ref());

	for expected in ALWAYS_ON {
		assert!(
			names.iter().any(|n| n == expected),
			"'{expected}' must register on every boot - a missing always-on class is exactly the \
			 silently-not-running defect this subsystem exists to prevent; registered: {names:?}"
		);
	}
	assert!(
		names.iter().any(|n| n == "cdc-ttl"),
		"cdc-ttl must register when a CdcStore is resolvable, otherwise cdc.db grows without bound; \
		 registered: {names:?}"
	);
}

#[test]
fn the_epoch_log_registers_even_though_nothing_visibly_depends_on_it() {
	// epoch-log deletes no user data; it is the only writer keeping the time-to-version map answerable and
	// bounded, so nothing else fails visibly when it is absent.
	let test_engine = TestEngine::new();
	let engine: StandardEngine = test_engine.inner().clone();
	let ioc = engine
		.ioc()
		.clone()
		.register(engine.clone())
		.register(LifecycleRegistry::new())
		.register(RetentionMetrics::new());

	let names = task_names(create(&ioc).as_ref());

	assert!(
		names.iter().any(|n| n == "epoch-log"),
		"epoch-log must register on every boot; without it no ttl in the system can resolve a cutoff after a \
		 restart; registered: {names:?}"
	);
}

#[test]
fn omits_cdc_truncation_when_no_cdc_store_is_present_without_dropping_any_always_on_class() {
	// The always-on set must not be coupled to an optional component: building the whole task list inside one
	// `if let Some(cdc_store)` block is how a conditional dependency takes unrelated classes down with it.
	let test_engine = TestEngine::new();
	let engine: StandardEngine = test_engine.inner().clone();
	let spawner = engine.ioc().resolve::<ActorSpawner>().expect("test engine registers a spawner");

	let ioc = IocContainer::new()
		.register(engine.clone())
		.register(spawner)
		.register(LifecycleRegistry::new())
		.register(RetentionMetrics::new());

	let names = task_names(create(&ioc).as_ref());

	assert!(
		!names.iter().any(|n| n == "cdc-ttl"),
		"cdc-ttl must NOT register without a CdcStore - registering it would panic or truncate nothing; \
		 registered: {names:?}"
	);
	for expected in ALWAYS_ON {
		assert!(
			names.iter().any(|n| n == expected),
			"'{expected}' must still register when CDC is absent - the always-on set may not depend on \
			 an optional component; registered: {names:?}"
		);
	}
}

#[test]
fn omits_persistent_flush_when_the_store_has_no_persistent_tier() {
	// With no persistent tier there is nothing to flush to, so registering the task would only schedule a no-op.
	// The positive direction rides the store-multi flush tests.
	let test_engine = TestEngine::new();
	let engine: StandardEngine = test_engine.inner().clone();
	let ioc = engine
		.ioc()
		.clone()
		.register(engine.clone())
		.register(LifecycleRegistry::new())
		.register(RetentionMetrics::new());

	let names = task_names(create(&ioc).as_ref());

	assert!(
		!names.iter().any(|n| n == "persistent-flush"),
		"persistent-flush must not register on a memory-only store; registered: {names:?}"
	);
}

#[test]
fn omits_tombstone_reap_when_the_store_has_no_persistent_tier() {
	// With no persistent tier there are no tables holding flushed tombstones, so registering the task would only
	// schedule a no-op. The positive direction rides the sqlite-backed store tests.
	let test_engine = TestEngine::new();
	let engine: StandardEngine = test_engine.inner().clone();
	let ioc = engine
		.ioc()
		.clone()
		.register(engine.clone())
		.register(LifecycleRegistry::new())
		.register(RetentionMetrics::new());

	let names = task_names(create(&ioc).as_ref());

	assert!(
		!names.iter().any(|n| n == "tombstone-reap"),
		"tombstone-reap must not register on a memory-only store; registered: {names:?}"
	);
}

#[test]
fn drains_the_registry_so_the_subsystem_is_the_sole_owner_of_every_task() {
	// The registry is a handoff point, not a shared list: if the factory left tasks behind, a second subsystem
	// (or a later drain) would run the same task from a second lane, double-driving reclamation cursors.
	let test_engine = TestEngine::new();
	let engine: StandardEngine = test_engine.inner().clone();
	let registry = LifecycleRegistry::new();
	let ioc = engine
		.ioc()
		.clone()
		.register(engine.clone())
		.register(registry.clone())
		.register(RetentionMetrics::new());

	let subsystem = create(&ioc);

	assert!(
		registry.is_empty(),
		"the factory must drain the registry; {} task(s) left behind would be run by whoever drains next",
		registry.len()
	);
	assert!(
		!task_names(subsystem.as_ref()).is_empty(),
		"draining the registry must hand the tasks to the subsystem, not discard them"
	);
}

#[test]
fn tasks_registered_by_other_crates_are_adopted_by_the_plane() {
	// The registry lets other subsystems put work on this lane instead of spawning their own timer; ignoring
	// pre-registered tasks sends every such crate back to running maintenance out-of-band.
	use reifydb_core::lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask};
	use reifydb_value::value::duration::Duration;

	struct ForeignTask;
	impl LifecycleTask for ForeignTask {
		fn name(&self) -> &'static str {
			"foreign-class"
		}

		fn interval(&self) -> Duration {
			Duration::from_seconds(3600).unwrap()
		}

		// Borrows the lane without owning a class in this crate's matrix.
		fn classes(&self) -> &'static [RetentionClass] {
			&[]
		}

		fn run_slice(&mut self) -> Progress {
			Progress::Exhausted
		}
	}

	let test_engine = TestEngine::new();
	let engine: StandardEngine = test_engine.inner().clone();
	let registry = LifecycleRegistry::new();
	registry.register(Box::new(ForeignTask));
	let ioc = engine.ioc().clone().register(engine.clone()).register(registry).register(RetentionMetrics::new());

	let names = task_names(create(&ioc).as_ref());

	assert!(
		names.iter().any(|n| n == "foreign-class"),
		"a task registered before the factory ran must be adopted onto the lane; registered: {names:?}"
	);
	for expected in ALWAYS_ON {
		assert!(
			names.iter().any(|n| n == expected),
			"adopting a foreign task must not displace built-in class '{expected}'; registered: {names:?}"
		);
	}
}

#[test]
fn every_registered_class_has_a_distinct_name() {
	// Names identify a class in the boot report, in metrics and in a stuck-horizon alarm, so a duplicate makes
	// one class invisible exactly when you are trying to find which one stopped.
	let test_engine = TestEngine::new();
	let engine: StandardEngine = test_engine.inner().clone();
	let ioc = engine
		.ioc()
		.clone()
		.register(engine.clone())
		.register(LifecycleRegistry::new())
		.register(RetentionMetrics::new());

	let names = task_names(create(&ioc).as_ref());
	let mut unique = names.clone();
	unique.dedup();

	assert_eq!(unique, names, "duplicate lifecycle class name would make one class unattributable: {names:?}");
}

#[test]
fn no_retention_class_is_left_without_an_executor_by_accident() {
	// A class with nothing registered to execute it reclaims nothing while the subsystem reports healthy, so
	// only the classes named in CONDITIONAL above may be uncovered here.
	let test_engine = TestEngine::new();
	let engine: StandardEngine = test_engine.inner().clone();
	let ioc = engine
		.ioc()
		.clone()
		.register(engine.clone())
		.register(LifecycleRegistry::new())
		.register(RetentionMetrics::new());
	let subsystem = create(&ioc);

	let covered = lifecycle(subsystem.as_ref()).covered_classes();
	let unexplained: Vec<&str> = RetentionClass::all()
		.iter()
		.filter(|class| !covered.contains(class) && !CONDITIONAL.contains(class))
		.map(|class| class.name())
		.collect();

	assert!(
		unexplained.is_empty(),
		"retention classes with no registered executor and no declared condition: {unexplained:?}"
	);
}

/// Classes the factory registers no task for on a memory store, each because the tier it operates on is
/// absent. The factory must say so on the shared registry rather than leave the boot report to guess.
const PERSISTENT_ONLY: [RetentionClass; 2] = [RetentionClass::PersistentFlush, RetentionClass::TombstoneReap];

#[test]
fn a_store_without_a_persistent_tier_declares_the_persistent_lanes_absent() {
	// Skipping a task and saying nothing is what made the boot report call every tier-absent lane an
	// unreclaimed one at ERROR. An operator cannot tell that from a lane that genuinely stopped, so the
	// same branch that skips the task owes the report a reason.
	let test_engine = TestEngine::new();
	let engine: StandardEngine = test_engine.inner().clone();
	let coverage = RetentionCoverage::new();
	let ioc = engine
		.ioc()
		.clone()
		.register(engine.clone())
		.register(LifecycleRegistry::new())
		.register(RetentionMetrics::new())
		.register(coverage.clone());

	create(&ioc);

	for class in PERSISTENT_ONLY {
		assert!(
			coverage.absence(class).is_some(),
			"{class} registers no task on a memory store, so the factory must declare why; without it the \
			 boot report reads as a dead lane"
		);
		assert!(
			!coverage.is_covered(class),
			"{class} has no executor here - declaring it absent must not fake one"
		);
	}
}

#[test]
fn an_ioc_without_a_cdc_store_declares_cdc_truncate_absent() {
	let test_engine = TestEngine::new();
	let engine: StandardEngine = test_engine.inner().clone();
	let spawner = engine.ioc().resolve::<ActorSpawner>().expect("test engine registers a spawner");
	let coverage = RetentionCoverage::new();
	let ioc = IocContainer::new()
		.register(engine.clone())
		.register(spawner)
		.register(LifecycleRegistry::new())
		.register(RetentionMetrics::new())
		.register(coverage.clone());

	create(&ioc);

	assert_eq!(coverage.absence(RetentionClass::CdcTruncate), Some("no cdc store registered"));
	assert!(!coverage.is_covered(RetentionClass::CdcTruncate));
}

#[test]
fn a_cdc_store_makes_cdc_truncate_covered_rather_than_absent() {
	// The paired direction, and the one that catches a blanket declaration: an absence recorded outside the
	// skip branch would still satisfy the test above while excusing a lane that is present and expected to
	// run, turning a genuinely stalled cdc-truncate into an info line.
	let test_engine = TestEngine::new();
	let engine: StandardEngine = test_engine.inner().clone();
	let coverage = RetentionCoverage::new();
	let ioc = engine
		.ioc()
		.clone()
		.register(engine.clone())
		.register(LifecycleRegistry::new())
		.register(RetentionMetrics::new())
		.register(coverage.clone());

	create(&ioc);

	assert_eq!(coverage.owner(RetentionClass::CdcTruncate), Some("cdc-ttl"));
	assert!(
		coverage.absence(RetentionClass::CdcTruncate).is_none(),
		"cdc-truncate has a registered executor here, so excusing it would hide a stall behind an absence"
	);
}

#[test]
fn the_factory_declares_onto_the_registered_coverage_not_a_private_default() {
	// The factory falls back to a default registry when none is registered. Writing to that fallback while
	// one exists in the IoC would leave every declaration invisible to the boot report - the same silent
	// outcome as never declaring at all.
	let test_engine = TestEngine::new();
	let engine: StandardEngine = test_engine.inner().clone();
	let coverage = RetentionCoverage::new();
	let ioc = engine
		.ioc()
		.clone()
		.register(engine.clone())
		.register(LifecycleRegistry::new())
		.register(RetentionMetrics::new())
		.register(coverage.clone());

	create(&ioc);

	assert_eq!(coverage.owner(RetentionClass::EpochLog), Some("epoch-log"));
	assert_eq!(coverage.absence(RetentionClass::TombstoneReap), Some("store has no persistent tier"));
}

#[test]
fn every_class_reports_a_slice_once_the_lane_has_run_each_task() {
	// Declaring a class is not executing one. A task can name a class and never reach the plane, which reads
	// identically to a healthy idle class unless liveness is asserted from the counters the lane actually wrote.
	let test_engine = TestEngine::new();
	let engine: StandardEngine = test_engine.inner().clone();
	let ioc = engine
		.ioc()
		.clone()
		.register(engine.clone())
		.register(LifecycleRegistry::new())
		.register(RetentionMetrics::new());
	let subsystem = create(&ioc);
	let lifecycle = lifecycle(subsystem.as_ref());

	for index in 0..lifecycle.task_names().len() {
		let waiter = Arc::new(WaiterHandle::new());
		let sent = lifecycle.actor_ref().send(LifecycleMessage::RunToExhaustion {
			index,
			waiter: waiter.clone(),
		});
		assert!(sent.is_ok(), "the lifecycle lane must accept a drain request for task {index}");
		assert!(
			waiter.wait_timeout(Duration::from_seconds(10).unwrap()),
			"task {index} ({}) never finished its slice",
			lifecycle.task_names()[index]
		);
	}

	let silent: Vec<&str> = lifecycle
		.plane()
		.report()
		.into_iter()
		.filter(|(class, snapshot)| lifecycle.covered_classes().contains(class) && snapshot.slices == 0)
		.map(|(class, _)| class.name())
		.collect();

	assert!(silent.is_empty(), "registered classes that never recorded a slice: {silent:?}");
}
