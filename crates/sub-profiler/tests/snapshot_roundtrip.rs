// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! End-to-end test for the profiler -> metric integration:
//!
//! - Bootstraps the `system::metrics::profiler::*` namespaces and snapshots Series.
//! - Drives a `ProfilerAccumulator` by hand so the test is deterministic (no clock racing, no tracing subscriber
//!   wiring). The collector actor is not exercised here; its fold path has its own unit tests in `accumulator.rs`.
//! - Registers the per-category live VTables via `ProfilerAggregatesVTable`.
//! - Calls `ProfilerSnapshotActor::flush` directly and queries both the live `spans` VTable and the persisted
//!   `snapshots` Series, asserting snapshot semantics: live remains non-empty after flush (cumulative aggregates), and
//!   the Series receives one row per active category per flush so a second flush at a later timestamp adds another row.

use std::sync::Arc;

use parking_lot::RwLock;
use reifydb_catalog::bootstrap::bootstrap_system_objects;
use reifydb_core::{event::EventBus, util::ioc::IocContainer};
use reifydb_engine::test_harness::TestEngine;
use reifydb_profiler::{
	category::ProfilerCategory,
	intern::DimInterner,
	record::{DIM_UNSET, MAX_EXTRAS, SpanIdent},
};
use reifydb_sub_profiler::{
	accumulator::ProfilerAccumulator, snapshot_actor::ProfilerSnapshotActor, vtable::ProfilerAggregatesVTable,
};
use reifydb_type::value::datetime::DateTime;

fn upsert(
	accumulator: &Arc<RwLock<ProfilerAccumulator>>,
	interner: &DimInterner,
	ident: SpanIdent,
	span_name: &'static str,
	duration_us: u32,
) {
	accumulator.write().upsert(ident, span_name, duration_us, &[0; MAX_EXTRAS], interner);
}

#[test]
fn end_to_end_drain_into_history_series() {
	let test_engine = TestEngine::new();
	let engine = (*test_engine).clone();

	let services = engine.services();
	let multi = engine.multi().clone();
	let single = engine.single().clone();
	let catalog_cache = services.catalog.cache().clone();
	let eventbus: EventBus = services.ioc.resolve::<EventBus>().expect("EventBus must be in TestEngine IoC");

	bootstrap_system_objects(&multi, &single, &catalog_cache, &eventbus).expect("bootstrap must succeed");

	// Shared accumulator: used both as the snapshot actor's drain source AND as the live VTable reader source.
	let interner = Arc::new(DimInterner::new());
	let accumulator = Arc::new(RwLock::new(ProfilerAccumulator::new(256, 0)));

	// Wire the live VTables under the bootstrapped per-category namespaces.
	let reader_factory = || reifydb_sub_profiler::reader::ProfilerReader::new(Arc::clone(&accumulator));
	for category in reifydb_profiler::category::ALL_CATEGORIES {
		let namespace = format!("system::metrics::profiler::{}", category.name());
		engine.register_virtual_table(
			&namespace,
			"spans",
			ProfilerAggregatesVTable::new(reader_factory(), category),
		)
		.unwrap_or_else(|e| panic!("register {namespace}::spans: {e}"));
	}

	// Drive records into two categories so we can verify per-category partitioning works.
	let query_ident = SpanIdent::new(ProfilerCategory::Query, 11, [DIM_UNSET; 2]);
	let flow_ident = SpanIdent::new(ProfilerCategory::Flow, 22, [DIM_UNSET; 2]);
	upsert(&accumulator, &interner, query_ident, "vm::executor", 250);
	upsert(&accumulator, &interner, query_ident, "vm::executor", 750);
	upsert(&accumulator, &interner, flow_ident, "flow::engine::apply", 1_000);
	assert_eq!(accumulator.read().len(), 2, "two distinct callsites should be folded into the accumulator");

	// Live view: pre-flush both categories show records, an idle category is empty.
	let pre_query_frames = test_engine.query("from system::metrics::profiler::query::spans");
	assert_eq!(
		TestEngine::row_count(&pre_query_frames),
		1,
		"live query::spans should reflect the current partial interval"
	);
	let pre_flow_frames = test_engine.query("from system::metrics::profiler::flow::spans");
	assert_eq!(TestEngine::row_count(&pre_flow_frames), 1);
	let pre_cdc_frames = test_engine.query("from system::metrics::profiler::cdc::spans");
	assert_eq!(
		TestEngine::row_count(&pre_cdc_frames),
		0,
		"category with no observations should be empty even mid-interval"
	);

	// Flush via the snapshot actor; this is what the scheduled tick would call.
	let actor = ProfilerSnapshotActor::new(Arc::clone(&accumulator), engine.clone(), eventbus.clone());
	let snapshot_ts = DateTime::from_timestamp_millis(1_500).expect("valid timestamp");
	actor.flush(snapshot_ts);

	// Snapshot semantics: accumulator retains every record, live VTables keep returning them.
	assert_eq!(accumulator.read().len(), 2, "snapshot must not clear the accumulator");
	let post_query_live = test_engine.query("from system::metrics::profiler::query::spans");
	assert_eq!(TestEngine::row_count(&post_query_live), 1, "live spans remain visible after snapshot");

	// Snapshots Series: query category gets the one folded callsite; flow category gets its own; quiet categories
	// stay empty.
	let query_snapshots = test_engine.query("from system::metrics::profiler::query::snapshots");
	assert_eq!(
		TestEngine::row_count(&query_snapshots),
		1,
		"query::snapshots should contain exactly one row for vm::executor"
	);
	let flow_snapshots = test_engine.query("from system::metrics::profiler::flow::snapshots");
	assert_eq!(TestEngine::row_count(&flow_snapshots), 1);
	let cdc_snapshots = test_engine.query("from system::metrics::profiler::cdc::snapshots");
	assert_eq!(TestEngine::row_count(&cdc_snapshots), 0, "idle category should write no snapshot row");

	// Cumulative behavior: a second flush at a later ts writes another snapshot row per active category
	// even with no new observations, because the accumulator still holds the aggregates.
	let next_ts = DateTime::from_timestamp_millis(11_500).expect("valid timestamp");
	actor.flush(next_ts);
	let query_snapshots2 = test_engine.query("from system::metrics::profiler::query::snapshots");
	assert_eq!(
		TestEngine::row_count(&query_snapshots2),
		2,
		"second snapshot must add a row carrying the unchanged cumulative aggregate"
	);

	// IoC sanity: nothing in this test should have leaked into the engine container.
	let _ = IocContainer::new();
}
