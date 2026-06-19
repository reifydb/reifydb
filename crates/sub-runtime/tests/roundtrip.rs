// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! End-to-end test for the runtime-metrics subsystem and its per-domain split:
//!
//! - Bootstraps `system::metrics::runtime::{memory,watermarks}` namespaces + `snapshots` series.
//! - Registers a live `current` vtable per domain and asserts the domain split: watermark metrics (`watermark_lag`,
//!   `oracle_window_count`) live under `watermarks`, NOT under `memory`.
//! - Drives `RuntimeSamplerActor::sample` (what the scheduled tick calls) and asserts each domain's `snapshots` series
//!   goes empty -> one set -> two sets, while `current` stays live.

use reifydb_catalog::bootstrap::bootstrap_system_objects;
use reifydb_core::event::EventBus;
use reifydb_engine::test_harness::TestEngine;
use reifydb_runtime::context::clock::Clock;
use reifydb_store_multi::MultiStore;
use reifydb_sub_runtime::{actor::RuntimeSamplerActor, collect::Collectors, domain::Domain, vtable::RuntimeVTable};
use reifydb_value::value::datetime::DateTime;

const WATERMARK_METRICS: usize = 9; // mvcc(6) + cdc(3), platform-independent

#[test]
fn per_domain_current_and_snapshots_roundtrip() {
	let test_engine = TestEngine::new();
	let engine = (*test_engine).clone();

	let services = engine.services();
	let multi = engine.multi().clone();
	let single = engine.single().clone();
	let catalog_cache = services.catalog.cache().clone();
	let eventbus: EventBus = services.ioc.resolve::<EventBus>().expect("EventBus must be in TestEngine IoC");

	bootstrap_system_objects(&multi, &single, &catalog_cache, &eventbus).expect("bootstrap must succeed");

	// TestEngine does not register MultiStore in its IoC (production does); buffer metrics are not the
	// subject of this test, so a standalone store is sufficient to exercise the surfaces.
	let collectors = Collectors {
		engine: engine.clone(),
		multi_store: MultiStore::testing_memory(),
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

	// History series start empty until the sampler runs.
	let before = test_engine.query("from system::metrics::runtime::watermarks::snapshots");
	assert_eq!(TestEngine::row_count(&before), 0, "snapshots must be empty before the first sample");

	let actor = RuntimeSamplerActor::new(
		collectors,
		engine.clone(),
		reifydb_value::value::duration::Duration::from_seconds(5).unwrap(),
	);

	actor.sample(DateTime::from_timestamp_millis(1_000).expect("valid timestamp"));
	let wm_after_one =
		TestEngine::row_count(&test_engine.query("from system::metrics::runtime::watermarks::snapshots"));
	assert_eq!(wm_after_one, WATERMARK_METRICS, "first sample writes one watermarks set");
	let mem_after_one =
		TestEngine::row_count(&test_engine.query("from system::metrics::runtime::memory::snapshots"));
	assert!(mem_after_one >= 1, "first sample writes memory rows too, got {mem_after_one}");

	// A second tick appends another full set per domain (history accumulates).
	actor.sample(DateTime::from_timestamp_millis(6_000).expect("valid timestamp"));
	let wm_after_two =
		TestEngine::row_count(&test_engine.query("from system::metrics::runtime::watermarks::snapshots"));
	assert_eq!(wm_after_two, WATERMARK_METRICS * 2, "second sample appends another watermarks set");

	// Live view is unaffected by snapshotting.
	let wm_current_again = test_engine.query("from system::metrics::runtime::watermarks::current");
	assert_eq!(TestEngine::row_count(&wm_current_again), WATERMARK_METRICS, "live current stays stable");
}
