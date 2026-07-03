// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Factory integration tests for the profiler subsystem. Validates the with_subsystem path against a real
//! bootstrapped engine so the per-category vtable registration step (which requires
//! `system::metrics::profiler::*` namespaces) actually executes.

use std::{sync::Arc, thread, time::Instant};

use reifydb_catalog::bootstrap::bootstrap_system_objects;
use reifydb_core::{
	common::CommitVersion, event::EventBus, interface::catalog::config::ConfigKey, util::ioc::IocContainer,
};
use reifydb_engine::test_harness::TestEngine;
use reifydb_profiler::{
	category::{CategorySet, ProfilerCategory},
	intern::DimInterner,
	record::{DIM_UNSET, MAX_EXTRAS, SpanIdent},
	sink::NoopSink,
};
use reifydb_runtime::{actor::system::ActorSpawner, context::clock::Clock, sync::rwlock::RwLock};
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_sub_profiler::{
	accumulator::ProfilerAccumulator, factory::ProfilerSubsystemFactory, subsystem::ProfilerSubsystem,
};
use reifydb_value::value::{Value, duration::Duration, value_type::ValueType};

/// Polls `f` every 10ms until it returns `Some` or `timeout` elapses. Used to assert on real
/// background-actor timing (the snapshot actor's tick) without a fixed sleep.
fn poll_until<T>(mut f: impl FnMut() -> Option<T>, timeout: Duration) -> Option<T> {
	let deadline = Instant::now() + timeout.to_std();
	loop {
		if let Some(value) = f() {
			return Some(value);
		}
		if Instant::now() >= deadline {
			return None;
		}
		thread::sleep(Duration::from_milliseconds(10).unwrap().to_std());
	}
}

#[test]
fn with_subsystem_returns_provided_and_registers_vtables() {
	let test_engine = TestEngine::new();
	let engine = (*test_engine).clone();

	let services = engine.services();
	let multi = engine.multi().clone();
	let single = engine.single().clone();
	let catalog_cache = services.catalog.cache().clone();
	let eventbus: EventBus = services.ioc.resolve::<EventBus>().expect("EventBus must be in TestEngine IoC");
	let clock: Clock = services.ioc.resolve::<Clock>().expect("Clock must be in TestEngine IoC");
	let spawner: ActorSpawner =
		services.ioc.resolve::<ActorSpawner>().expect("ActorSpawner must be in TestEngine IoC");

	bootstrap_system_objects(&multi, &single, &catalog_cache, &eventbus).expect("bootstrap must succeed");

	let interner = Arc::new(DimInterner::new());
	let accumulator = Arc::new(RwLock::new(ProfilerAccumulator::new(16, 0)));
	let sink: Arc<dyn reifydb_profiler::sink::ProfilerSink> = Arc::new(NoopSink);
	let subsystem = ProfilerSubsystem::new(false, CategorySet::empty(), interner, accumulator, sink, clock.clone());

	let ioc = IocContainer::new().register(engine.clone()).register(spawner).register(eventbus.clone());

	let factory = Box::new(ProfilerSubsystemFactory::with_subsystem(subsystem));
	let result = factory.create(&ioc).expect("create should succeed with engine in IoC");

	let downcast = result.as_any().downcast_ref::<ProfilerSubsystem>();
	assert!(downcast.is_some(), "returned subsystem must be ProfilerSubsystem");
	assert!(downcast.unwrap().is_running());

	// Verify each per-category aggregates vtable was registered under the matching namespace.
	for name in ["query", "txn", "storage", "plan", "cdc", "flow"] {
		let frames = test_engine.query(&format!("from system::metrics::profiler::{name}::current"));
		assert!(!frames.is_empty(), "vtable system::metrics::profiler::{name}::current should be queryable");
	}
}

fn upsert(accumulator: &Arc<RwLock<ProfilerAccumulator>>, interner: &DimInterner, ident: SpanIdent) {
	accumulator.write().upsert(ident, "vm::executor", 250, &[0; MAX_EXTRAS], interner);
}

#[test]
fn snapshot_actor_is_not_spawned_when_interval_is_none() {
	let test_engine = TestEngine::new();
	let engine = (*test_engine).clone();

	let services = engine.services();
	let multi = engine.multi().clone();
	let single = engine.single().clone();
	let catalog_cache = services.catalog.cache().clone();
	let eventbus: EventBus = services.ioc.resolve::<EventBus>().expect("EventBus must be in TestEngine IoC");
	let clock: Clock = services.ioc.resolve::<Clock>().expect("Clock must be in TestEngine IoC");
	let spawner: ActorSpawner =
		services.ioc.resolve::<ActorSpawner>().expect("ActorSpawner must be in TestEngine IoC");

	bootstrap_system_objects(&multi, &single, &catalog_cache, &eventbus).expect("bootstrap must succeed");

	catalog_cache
		.set_config(
			ConfigKey::MetricsProfilerSnapshotInterval,
			CommitVersion(1),
			Value::None {
				inner: ValueType::Duration,
			},
		)
		.expect("disabling snapshot persistence must be a valid config value");

	let ioc = IocContainer::new()
		.register(engine.clone())
		.register(spawner)
		.register(eventbus.clone())
		.register(clock);

	let factory = Box::new(ProfilerSubsystemFactory::new());
	let result = factory.create(&ioc).expect("create should succeed even with persistence disabled");
	let subsystem = result.as_any().downcast_ref::<ProfilerSubsystem>().expect("must be ProfilerSubsystem");

	// Direct check: the actor was never spawned, so no scope was ever set on the subsystem.
	assert!(!subsystem.snapshot_persistence_enabled(), "MetricsProfilerSnapshotInterval=none must skip spawning");

	// Behavioral check: even with real accumulator activity, no row ever reaches the persisted series -
	// proving the actor genuinely isn't ticking, not just that a flag happens to be set.
	let interner = subsystem.interner();
	let flow_ident = SpanIdent::new(ProfilerCategory::Flow, 1, [DIM_UNSET; 2]);
	upsert(&subsystem.accumulator(), &interner, flow_ident);

	let saw_a_row = poll_until(
		|| {
			let frames = test_engine.query("from system::metrics::profiler::flow::snapshots");
			(TestEngine::row_count(&frames) > 0).then_some(())
		},
		Duration::from_milliseconds(300).unwrap(),
	);
	assert!(saw_a_row.is_none(), "disabled MetricsProfilerSnapshotInterval must prevent all snapshot writes");
}

#[test]
fn snapshot_actor_uses_configured_interval() {
	let test_engine = TestEngine::new();
	let engine = (*test_engine).clone();

	let services = engine.services();
	let multi = engine.multi().clone();
	let single = engine.single().clone();
	let catalog_cache = services.catalog.cache().clone();
	let eventbus: EventBus = services.ioc.resolve::<EventBus>().expect("EventBus must be in TestEngine IoC");
	let clock: Clock = services.ioc.resolve::<Clock>().expect("Clock must be in TestEngine IoC");
	let spawner: ActorSpawner =
		services.ioc.resolve::<ActorSpawner>().expect("ActorSpawner must be in TestEngine IoC");

	bootstrap_system_objects(&multi, &single, &catalog_cache, &eventbus).expect("bootstrap must succeed");

	catalog_cache
		.set_config(
			ConfigKey::MetricsProfilerSnapshotInterval,
			CommitVersion(1),
			Value::duration_milliseconds(20),
		)
		.expect("20ms must be a valid snapshot interval");

	let ioc = IocContainer::new()
		.register(engine.clone())
		.register(spawner)
		.register(eventbus.clone())
		.register(clock);

	let factory = Box::new(ProfilerSubsystemFactory::new());
	let result = factory.create(&ioc).expect("create should succeed");
	let subsystem = result.as_any().downcast_ref::<ProfilerSubsystem>().expect("must be ProfilerSubsystem");

	assert!(subsystem.snapshot_persistence_enabled(), "a Some(interval) config must spawn the snapshot actor");

	let interner = subsystem.interner();
	let flow_ident = SpanIdent::new(ProfilerCategory::Flow, 1, [DIM_UNSET; 2]);
	upsert(&subsystem.accumulator(), &interner, flow_ident);

	// Proves .with_interval() actually wired the configured 20ms into the actor, not just that
	// spawning happened with the historical hardcoded 10s default.
	let saw_a_row = poll_until(
		|| {
			let frames = test_engine.query("from system::metrics::profiler::flow::snapshots");
			(TestEngine::row_count(&frames) > 0).then_some(())
		},
		Duration::from_milliseconds(500).unwrap(),
	);
	assert!(saw_a_row.is_some(), "20ms configured interval must produce a snapshot row within 500ms");
}
