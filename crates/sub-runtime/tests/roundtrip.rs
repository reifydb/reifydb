// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! End-to-end test for the runtime-metrics subsystem and its per-domain split:
//!
//! - Bootstraps `system::metrics::runtime::{memory,watermarks,operators}` namespaces + `snapshots` series.
//! - Registers a live `current` vtable per domain and asserts the domain split: watermark metrics (`watermark_lag`,
//!   `oracle_window_count`) live under `watermarks`, NOT under `memory`.
//! - Drives `RuntimeSamplerActor::sample` (what the scheduled tick calls) and asserts that NO snapshot rows are
//!   written: snapshots are disabled for now, so the bootstrapped `snapshots` series must exist but stay empty while
//!   the `current` vtables stay live. This is the tripwire against accidentally re-enabling the insert path.

use std::sync::Arc;

use reifydb_catalog::bootstrap::bootstrap_system_objects;
use reifydb_core::{
	event::EventBus,
	util::memory::{MemoryRegistry, MemoryReporter, MemorySample},
};
use reifydb_engine::test_harness::TestEngine;
use reifydb_runtime::context::clock::Clock;
use reifydb_sub_runtime::{actor::RuntimeSamplerActor, collect::Collectors, domain::Domain, vtable::RuntimeVTable};

const WATERMARK_METRICS: usize = 9; // mvcc(6) + cdc(3), platform-independent

#[test]
fn per_domain_current_stays_live_and_snapshots_stay_empty() {
	let test_engine = TestEngine::new();
	let engine = (*test_engine).clone();

	let services = engine.services();
	let multi = engine.multi().clone();
	let single = engine.single().clone();
	let catalog_cache = services.catalog.cache().clone();
	let eventbus: EventBus = services.ioc.resolve::<EventBus>().expect("EventBus must be in TestEngine IoC");

	bootstrap_system_objects(&multi, &single, &catalog_cache, &eventbus).expect("bootstrap must succeed");

	// Component memory flows in through the MemoryRegistry (production registers the store's
	// reporters into it); registry contents are not the subject of this test, so an empty
	// registry is sufficient to exercise the surfaces.
	let collectors = Collectors {
		engine: engine.clone(),
		registry: MemoryRegistry::new(),
	};

	for domain in Domain::ALL {
		engine.register_virtual_table(
			domain.namespace(),
			"current",
			RuntimeVTable::new(collectors.clone(), Clock::Real, domain),
		)
		.unwrap_or_else(|e| panic!("register {}::current: {e}", domain.local_name()));
	}

	// Domain split: watermark metrics belong to `watermarks`, never `memory`.
	let wm_lag_in_memory = test_engine
		.query("from system::metrics::runtime::memory::current filter { metric == \"watermark_lag\" }");
	assert_eq!(TestEngine::row_count(&wm_lag_in_memory), 0, "watermark_lag must not appear in the memory domain");
	let wm_lag_in_watermarks = test_engine
		.query("from system::metrics::runtime::watermarks::current filter { metric == \"watermark_lag\" }");
	assert_eq!(
		TestEngine::row_count(&wm_lag_in_watermarks),
		1,
		"watermark_lag must appear in the watermarks domain"
	);
	let oracle_in_watermarks = test_engine.query(
		"from system::metrics::runtime::watermarks::current filter { metric == \"oracle_window_count\" }",
	);
	assert_eq!(
		TestEngine::row_count(&oracle_in_watermarks),
		1,
		"oracle_window_count must be in the watermarks domain"
	);

	// Watermarks live view returns its full set on every platform.
	let wm_current = test_engine.query("from system::metrics::runtime::watermarks::current");
	assert_eq!(TestEngine::row_count(&wm_current), WATERMARK_METRICS, "watermarks::current must expose mvcc+cdc");

	// The operators domain is registered by the same Domain::ALL loop: its current vtable must be
	// queryable and empty on an engine with no flow-operator state.
	let operators_current = test_engine.query("from system::metrics::runtime::operators::current");
	assert_eq!(
		TestEngine::row_count(&operators_current),
		0,
		"operators::current must be queryable and empty without any flow-operator state"
	);

	// History series exist (bootstrapped for consistency) and start empty.
	let before = test_engine.query("from system::metrics::runtime::watermarks::snapshots");
	assert_eq!(TestEngine::row_count(&before), 0, "snapshots must be empty before the first sample");
	let operators_before = test_engine.query("from system::metrics::runtime::operators::snapshots");
	assert_eq!(
		TestEngine::row_count(&operators_before),
		0,
		"the operators snapshots series must be bootstrapped and empty"
	);

	let actor = RuntimeSamplerActor::new(
		collectors,
		reifydb_value::value::duration::Duration::from_seconds(5).unwrap(),
	);

	// Snapshots are disabled: sampling (twice, as the scheduled tick would) must write NO history
	// rows into any domain's snapshots series.
	actor.sample();
	actor.sample();
	let wm_after =
		TestEngine::row_count(&test_engine.query("from system::metrics::runtime::watermarks::snapshots"));
	assert_eq!(wm_after, 0, "sampling must not write watermark history while snapshots are disabled");
	let mem_after = TestEngine::row_count(&test_engine.query("from system::metrics::runtime::memory::snapshots"));
	assert_eq!(mem_after, 0, "sampling must not write memory history while snapshots are disabled");
	let ops_after =
		TestEngine::row_count(&test_engine.query("from system::metrics::runtime::operators::snapshots"));
	assert_eq!(ops_after, 0, "sampling must not write operator history while snapshots are disabled");

	// Live view is unaffected by sampling.
	let wm_current_again = test_engine.query("from system::metrics::runtime::watermarks::current");
	assert_eq!(TestEngine::row_count(&wm_current_again), WATERMARK_METRICS, "live current stays stable");
}

#[test]
fn registered_memory_reporters_flow_into_the_live_surface_only() {
	// The MemoryRegistry is the extension seam for component memory: anything registered must
	// surface in the live memory::current vtable without any collector change - that is the
	// contract that lets components self-report. With snapshots disabled, the same sample must
	// NOT be persisted by the sampler tick. A fixed reporter stands in for a real component
	// (read buffer, commit buffer, ...).
	struct Fixed;
	impl MemoryReporter for Fixed {
		fn report(&self, out: &mut Vec<MemorySample>) {
			out.push(MemorySample::new("test_component", "resident_bytes", 4096.0, "bytes"));
		}
	}

	let test_engine = TestEngine::new();
	let engine = (*test_engine).clone();

	let services = engine.services();
	let multi = engine.multi().clone();
	let single = engine.single().clone();
	let catalog_cache = services.catalog.cache().clone();
	let eventbus: EventBus = services.ioc.resolve::<EventBus>().expect("EventBus must be in TestEngine IoC");

	bootstrap_system_objects(&multi, &single, &catalog_cache, &eventbus).expect("bootstrap must succeed");

	let registry = MemoryRegistry::new();
	registry.register(Arc::new(Fixed));
	let collectors = Collectors {
		engine: engine.clone(),
		registry,
	};

	for domain in Domain::ALL {
		engine.register_virtual_table(
			domain.namespace(),
			"current",
			RuntimeVTable::new(collectors.clone(), Clock::Real, domain),
		)
		.unwrap_or_else(|e| panic!("register {}::current: {e}", domain.local_name()));
	}

	let live = test_engine
		.query("from system::metrics::runtime::memory::current filter { scope == \"test_component\" }");
	assert_eq!(
		TestEngine::row_count(&live),
		1,
		"a registered reporter's sample must appear in the live memory::current vtable"
	);

	let actor = RuntimeSamplerActor::new(
		collectors,
		reifydb_value::value::duration::Duration::from_seconds(5).unwrap(),
	);
	actor.sample();

	let snapshotted = test_engine
		.query("from system::metrics::runtime::memory::snapshots filter { scope == \"test_component\" }");
	assert_eq!(
		TestEngine::row_count(&snapshotted),
		0,
		"the reporter's sample must NOT be persisted by the tick while snapshots are disabled"
	);
}
