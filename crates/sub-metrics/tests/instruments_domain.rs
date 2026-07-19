// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Every registered reporter must be readable with RQL through
//! `system::metrics::instruments::current` in the uniform ts/scope/metric/value/unit shape,
//! histograms flattened to six scalar rows.

use std::sync::Arc;

use reifydb_catalog::bootstrap::bootstrap_system_objects;
use reifydb_core::{event::EventBus, metrics::registry::MetricsRegistry};
use reifydb_engine::test_harness::TestEngine;
use reifydb_profiler::category::{ALL_CATEGORIES, ProfilerCategory};
use reifydb_runtime::context::clock::Clock;
use reifydb_sub_metrics::{
	domains::instruments::InstrumentsSource,
	framework::{current::CurrentVTable, source::MetricsSource},
	profiler::instruments::ProfilerInstruments,
};

#[test]
fn instruments_current_serves_every_registered_reporter() {
	let test_engine = TestEngine::new();
	let engine = (*test_engine).clone();

	let services = engine.services();
	let multi = engine.multi().clone();
	let single = engine.single().clone();
	let catalog_cache = services.catalog.cache().clone();
	let eventbus: EventBus = services.ioc.resolve::<EventBus>().expect("EventBus must be in TestEngine IoC");

	bootstrap_system_objects(&multi, &single, &catalog_cache, &eventbus).expect("bootstrap must succeed");

	let registry = MetricsRegistry::new();
	let instruments = ProfilerInstruments::new();
	instruments.histogram_for(ProfilerCategory::Query).observe(150.0);
	instruments.register_into(&registry);

	let source: Arc<dyn MetricsSource> = Arc::new(InstrumentsSource::new(registry));
	engine.register_virtual_table(source.namespace(), "current", CurrentVTable::new(source, Clock::Real))
		.expect("register instruments current vtable");

	let all = test_engine.query("from system::metrics::instruments::current");
	assert_eq!(
		TestEngine::row_count(&all),
		ALL_CATEGORIES.len() * 6 + 3,
		"every instrument must appear; a missing one silently vanishes from the surface"
	);

	let query_hist = test_engine
		.query("from system::metrics::instruments::current filter { scope == \"profiler.query.duration_us\" }");
	assert_eq!(
		TestEngine::row_count(&query_hist),
		6,
		"one observed histogram must serve exactly count/sum/p50/p95/p99/max rows"
	);
}
