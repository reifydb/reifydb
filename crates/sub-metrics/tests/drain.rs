// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Executed requests must land in system::metrics::request_history and per-fingerprint
//! aggregates in system::metrics::statement_stats on the flush tick.

use std::{sync::Arc, thread};

use reifydb_catalog::bootstrap::bootstrap_system_objects;
use reifydb_core::{
	event::{
		EventBus,
		metric::{Request, RequestExecutedEvent},
	},
	fingerprint::{RequestFingerprint, StatementFingerprint},
	metrics::execution::StatementMetrics,
};
use reifydb_engine::test_harness::TestEngine;
use reifydb_runtime::{actor::system::ActorSpawner, context::clock::Clock};
use reifydb_sub_metrics::{
	accumulator::StatementMetricsAccumulator, actor::MetricsFlushActor, listener::RequestMetricsEventListener,
};
use reifydb_value::value::{datetime::DateTime, duration::Duration};

#[test]
fn executed_requests_drain_into_both_ringbuffers() {
	let test_engine = TestEngine::new();
	let engine = (*test_engine).clone();

	let services = engine.services();
	let multi = engine.multi().clone();
	let single = engine.single().clone();
	let catalog_cache = services.catalog.cache().clone();
	let eventbus: EventBus = services.ioc.resolve::<EventBus>().expect("EventBus must be in TestEngine IoC");
	let spawner: ActorSpawner =
		services.ioc.resolve::<ActorSpawner>().expect("ActorSpawner must be in TestEngine IoC");

	bootstrap_system_objects(&multi, &single, &catalog_cache, &eventbus).expect("bootstrap must succeed");

	let accumulator = Arc::new(StatementMetricsAccumulator::new());
	accumulator.record(
		StatementFingerprint::new(7),
		"from test::t",
		Duration::from_micros_infallible(120),
		Duration::from_micros_infallible(80),
		2,
		true,
	);

	let actor = MetricsFlushActor::new(
		Arc::clone(&accumulator),
		eventbus.clone(),
		single.read_store(),
		multi.store().clone(),
	)
	.with_drain(engine.clone(), Clock::Real)
	.with_flush_interval(Duration::from_milliseconds(10).unwrap());
	let handle = spawner.spawn_coordination("metrics-flush", actor);
	eventbus.register::<RequestExecutedEvent, _>(RequestMetricsEventListener::new(handle.actor_ref().clone()));

	eventbus.emit(RequestExecutedEvent::new(
		Request::Query {
			fingerprint: RequestFingerprint::default(),
			statements: vec![StatementMetrics {
				fingerprint: StatementFingerprint::new(7),
				normalized_rql: "from test::t".to_string(),
				compile_duration: Duration::from_microseconds(40).unwrap(),
				execute_duration: Duration::from_microseconds(80).unwrap(),
				rows_affected: 2,
			}],
		},
		Duration::from_microseconds(120).unwrap(),
		Duration::from_microseconds(80).unwrap(),
		true,
		DateTime::from_timestamp_millis(1000).unwrap(),
	));

	thread::sleep(Duration::from_milliseconds(300).unwrap().to_std());

	let history = test_engine.query("from system::metrics::request_history");
	assert_eq!(TestEngine::row_count(&history), 1, "the executed request must appear in request_history");

	let stats = test_engine.query("from system::metrics::statement_stats");
	assert!(
		TestEngine::row_count(&stats) >= 1,
		"the statement aggregate must appear in statement_stats; each tick appends a snapshot"
	);
}
