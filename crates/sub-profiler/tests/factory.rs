// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Factory integration tests for the profiler subsystem. Validates the with_subsystem path against a real
//! bootstrapped engine so the per-category vtable registration step (which requires
//! `system::metrics::profiler::*` namespaces) actually executes.

use std::sync::Arc;

use reifydb_catalog::bootstrap::bootstrap_system_objects;
use reifydb_core::{event::EventBus, util::ioc::IocContainer};
use reifydb_engine::test_harness::TestEngine;
use reifydb_profiler::{category::CategorySet, intern::DimInterner, sink::NoopSink};
use reifydb_runtime::{actor::system::ActorSpawner, context::clock::Clock, sync::rwlock::RwLock};
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_sub_profiler::{
	accumulator::ProfilerAccumulator, factory::ProfilerSubsystemFactory, subsystem::ProfilerSubsystem,
};

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
		let frames = test_engine.query(&format!("from system::metrics::profiler::{name}::spans"));
		assert!(!frames.is_empty(), "vtable system::metrics::profiler::{name}::spans should be queryable");
	}
}
