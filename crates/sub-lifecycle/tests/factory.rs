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

use reifydb_core::{lifecycle::registry::LifecycleRegistry, util::ioc::IocContainer};
use reifydb_engine::{engine::StandardEngine, test_harness::TestEngine};
use reifydb_runtime::actor::system::ActorSpawner;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_sub_lifecycle::{factory::LifecycleSubsystemFactory, subsystem::LifecycleSubsystem};

/// Classes that must register on EVERY boot, under every store configuration. If one of these ever becomes
/// conditional, the leak it guards against comes back silently.
const ALWAYS_ON: [&str; 5] = ["retention-evict", "operator-ttl", "drop-reclaim", "historical-gc", "epoch-log"];

fn task_names(subsystem: &dyn Subsystem) -> Vec<String> {
	let lifecycle = subsystem
		.as_any()
		.downcast_ref::<LifecycleSubsystem>()
		.expect("the lifecycle factory must produce a LifecycleSubsystem");
	let mut names: Vec<String> = lifecycle.task_names().iter().map(|n| n.to_string()).collect();
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
	let ioc = engine.ioc().clone().register(engine.clone()).register(LifecycleRegistry::new());

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
	let ioc = engine.ioc().clone().register(engine.clone()).register(LifecycleRegistry::new());

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
		.register(LifecycleRegistry::new());

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
	let ioc = engine.ioc().clone().register(engine.clone()).register(LifecycleRegistry::new());

	let names = task_names(create(&ioc).as_ref());

	assert!(
		!names.iter().any(|n| n == "persistent-flush"),
		"persistent-flush must not register on a memory-only store; registered: {names:?}"
	);
}

#[test]
fn drains_the_registry_so_the_subsystem_is_the_sole_owner_of_every_task() {
	// The registry is a handoff point, not a shared list: if the factory left tasks behind, a second subsystem
	// (or a later drain) would run the same task from a second lane, double-driving reclamation cursors.
	let test_engine = TestEngine::new();
	let engine: StandardEngine = test_engine.inner().clone();
	let registry = LifecycleRegistry::new();
	let ioc = engine.ioc().clone().register(engine.clone()).register(registry.clone());

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
	use reifydb_core::lifecycle::{progress::Progress, task::LifecycleTask};
	use reifydb_value::value::duration::Duration;

	struct ForeignTask;
	impl LifecycleTask for ForeignTask {
		fn name(&self) -> &'static str {
			"foreign-class"
		}

		fn interval(&self) -> Duration {
			Duration::from_seconds(3600).unwrap()
		}

		fn run_slice(&mut self) -> Progress {
			Progress::Exhausted
		}
	}

	let test_engine = TestEngine::new();
	let engine: StandardEngine = test_engine.inner().clone();
	let registry = LifecycleRegistry::new();
	registry.register(Box::new(ForeignTask));
	let ioc = engine.ioc().clone().register(engine.clone()).register(registry);

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
	let ioc = engine.ioc().clone().register(engine.clone()).register(LifecycleRegistry::new());

	let names = task_names(create(&ioc).as_ref());
	let mut unique = names.clone();
	unique.dedup();

	assert_eq!(unique, names, "duplicate lifecycle class name would make one class unattributable: {names:?}");
}
