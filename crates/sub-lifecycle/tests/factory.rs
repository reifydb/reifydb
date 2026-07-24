// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Registration matrix for the lifecycle subsystem.
//!
//! The defect this subsystem exists to eliminate is maintenance that silently is not running: an evictor skipped
//! under one store configuration, a settings sweep that only engages on a fresh database, a truncation pass nobody
//! notices never fired. Those bugs are invisible precisely because nothing asserts the class should have been there.
//!
//! So these tests assert the registered SET, by name, exhaustively - not merely that registration did not panic. A
//! class silently dropping out of the plane fails here, and a new class added without a deliberate decision about
//! whether it is always-on also fails here, which is the point: the matrix is the reviewed contract.

use std::sync::Arc;

use reifydb_core::{
	lifecycle::{class::RetentionClass, metrics::RetentionMetrics, registry::LifecycleRegistry},
	util::ioc::IocContainer,
};
use reifydb_engine::{engine::StandardEngine, test_harness::TestEngine};
use reifydb_runtime::{actor::system::ActorSpawner, sync::waiter::WaiterHandle};
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_sub_lifecycle::{
	actor::LifecycleMessage, factory::LifecycleSubsystemFactory, subsystem::LifecycleSubsystem,
};
use reifydb_value::value::duration::Duration;

/// Classes that must register on EVERY boot, under every store configuration. If one of these ever becomes
/// conditional, the leak it guards against comes back silently.
const ALWAYS_ON: [&str; 6] = [
	"retention-evict-silent",
	"retention-evict-announced",
	"operator-ttl",
	"compaction-reclaim",
	"historical-gc",
	"epoch-log",
];

/// Classes this subsystem does not register an executor for. Adding to this list is a reviewed decision: it exempts
/// the class from the coverage assertion below. Two reasons appear here, and they are not equivalent. The first four
/// register only when the store provides the tier they reclaim, and each has its own test pinning that condition.
/// The two operator-group classes are executed by the FLOW tick rather than by this lane (the group reclaim driver
/// runs inside FlowTransaction), so no lifecycle task will ever cover them and the boot report's "no registered
/// executor" line is expected for them.
const CONDITIONAL: [RetentionClass; 6] = [
	RetentionClass::PersistentFlush,
	RetentionClass::CdcTruncate,
	RetentionClass::TombstoneReap,
	RetentionClass::VacuumBudget,
	RetentionClass::OperatorGroupData,
	RetentionClass::OperatorGroupIdentity,
];

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
	// epoch-log is the one class whose absence is completely silent: it deletes nothing itself, it just keeps the
	// time-to-version map answerable. Without it every OTHER class resolves a none cutoff and reclaims nothing
	// while still reporting success - which is exactly how TTLs came to be declared-but-never-enforced. It has to
	// be unconditional for the same reason it is easy to forget.
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
	// The always-on set must not be coupled to optional components. An earlier shape of this wiring built the
	// whole task list inside one `if let Some(cdc_store)`-style block, which is how a conditional dependency
	// silently takes unrelated classes down with it.
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
	// PersistentFlushTask drives the commit-buffer -> sqlite sweep. With no persistent tier there is nothing to
	// flush to, so registering it would schedule a task that can only no-op. This pins the conditional in the
	// direction we can construct here; the positive case rides the store-multi flush tests.
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
	// TombstoneReapTask physically deletes flushed delete-mode tombstones from the persistent tables. With no
	// persistent tier there are no such tables, so registering it would schedule a task that can only no-op. This
	// pins the conditional in the direction we can construct here; the positive case rides the store-multi and
	// executor tests that build a sqlite-backed store.
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
fn omits_vacuum_budget_when_the_store_has_no_persistent_tier() {
	// VacuumBudgetTask runs incremental_vacuum on the persistent sqlite file. With no persistent tier there is no
	// file to compact, so registering it would schedule a task that can only no-op. This pins the conditional in
	// the direction we can construct here; the positive case rides the store-multi and executor tests.
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
		!names.iter().any(|n| n == "vacuum-budget"),
		"vacuum-budget must not register on a memory-only store; registered: {names:?}"
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
	// The registry is the extension point that lets other subsystems put work on this lane instead of spawning
	// their own timer. If the factory ignored pre-registered tasks, every such crate would quietly go back to
	// running maintenance out-of-band, which is the fragmentation this subsystem replaces.
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
	// Names are how a class is identified in the boot report, in metrics, and in a stuck-horizon alarm. Two
	// classes sharing a name makes one of them invisible in exactly the situation where you are trying to find
	// out which one stopped making progress.
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
	// A class in the matrix with nothing registered to execute it reclaims nothing while the subsystem reports
	// healthy. Only the two classes whose registration is conditional on a store tier may be uncovered, and each
	// of those has its own test pinning the condition; anything else is a class nobody wired up.
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
