// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Runtime metrics domain split, ported from the retired sub-runtime roundtrip test.
//!
//! Builds the runtime MetricSources over a bare engine, registers their generic CurrentVTables, and asserts the
//! per-domain partitioning: watermark metrics live under `watermarks`, never `memory`, and an engine with no flow
//! state exposes an empty `operators::current`. This pins that the collectors moved into sub-metric keep their
//! domain assignment and that the framework's CurrentVTable serves each domain's samples on query.

use reifydb_catalog::bootstrap::bootstrap_system_objects;
use reifydb_core::{event::EventBus, metrics::registry::MetricsRegistry};
use reifydb_engine::test_harness::TestEngine;
use reifydb_runtime::context::clock::Clock;
use reifydb_sub_metrics::{
	domains::runtime::{collect::Collectors, runtime_sources},
	framework::current::CurrentVTable,
};

#[test]
fn runtime_current_vtables_split_watermarks_from_memory() {
	let test_engine = TestEngine::new();
	let engine = (*test_engine).clone();

	let services = engine.services();
	let multi = engine.multi().clone();
	let single = engine.single().clone();
	let catalog_cache = services.catalog.cache().clone();
	let eventbus: EventBus = services.ioc.resolve::<EventBus>().expect("EventBus must be in TestEngine IoC");

	bootstrap_system_objects(&multi, &single, &catalog_cache, &eventbus).expect("bootstrap must succeed");

	let collectors = Collectors {
		engine: engine.clone(),
		registry: MetricsRegistry::new(),
	};
	for source in runtime_sources(&collectors) {
		let namespace = source.namespace();
		engine.register_virtual_table(namespace, "current", CurrentVTable::new(source, Clock::Real))
			.expect("register runtime current vtable");
	}

	let wm_in_memory = test_engine
		.query("from system::metrics::runtime::memory::current filter { metric == \"watermark_lag\" }");
	assert_eq!(TestEngine::row_count(&wm_in_memory), 0, "watermark_lag must not appear in the memory domain");

	let wm_in_watermarks = test_engine
		.query("from system::metrics::runtime::watermarks::current filter { metric == \"watermark_lag\" }");
	assert_eq!(TestEngine::row_count(&wm_in_watermarks), 1, "watermark_lag must appear in the watermarks domain");

	let oracle = test_engine.query(
		"from system::metrics::runtime::watermarks::current filter { metric == \"oracle_window_count\" }",
	);
	assert_eq!(TestEngine::row_count(&oracle), 1, "oracle_window_count must be in the watermarks domain");

	let operators = test_engine.query("from system::metrics::runtime::operators::current");
	assert_eq!(
		TestEngine::row_count(&operators),
		0,
		"operators::current must be queryable and empty without any flow-operator state"
	);
}
