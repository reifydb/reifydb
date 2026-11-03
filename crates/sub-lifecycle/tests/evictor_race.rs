// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	sync::atomic::{AtomicBool, Ordering},
	thread::scope,
	time::{Duration, Instant},
};

use reifydb_core::{
	interface::catalog::config::ConfigKey,
	lifecycle::{class::RetentionClass, metrics::RetentionMetrics, task::LifecycleTask},
	retry::is_conflict,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_sub_lifecycle::{plane::RetentionPlane, retention::evictor::RetentionEvictTask};
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{params::Params, value::Value};

const CONFLICT_SEARCH_LIMIT: Duration = Duration::from_secs(60);
const PAST_TTL_SECONDS: u64 = 3_601;

struct StopOnDrop<'a>(&'a AtomicBool);

impl Drop for StopOnDrop<'_> {
	fn drop(&mut self) {
		self.0.store(true, Ordering::Release);
	}
}

#[test]
fn a_writer_racing_the_evictor_never_stops_the_process() {
	// Batch size must exceed capacity: otherwise a live cursor reports backlog and stuck_slices proves no conflict.
	let test = TestEngine::new();
	test.admin("create namespace test;");
	test.admin("CREATE RINGBUFFER test::rb { v: int4 } WITH { time: processing, capacity: 64, row: { ttl: 1h } }");
	test.set_config(ConfigKey::RetentionEvictBatchSize, Value::Uint8(128));

	let engine: &StandardEngine = test.inner();
	let plane = RetentionPlane::for_engine(engine, RetentionMetrics::new());
	let mut task = RetentionEvictTask::silent(engine.clone(), plane.clone());
	let clock = test.mock_clock();
	let done = AtomicBool::new(false);

	let conflicted = scope(|scope| {
		let _stop = StopOnDrop(&done);
		scope.spawn(|| {
			while !done.load(Ordering::Acquire) {
				let result = engine.command_as(
					TestEngine::identity(),
					"INSERT test::rb [{ v: 1 }]",
					Params::None,
				);
				if let Some(error) = result.error {
					assert!(
						is_conflict(&error),
						"the writer may only lose to a conflict: {error:?}"
					);
				}
			}
		});

		let started = Instant::now();
		loop {
			clock.advance_secs(PAST_TTL_SECONDS);
			task.run_slice();
			if plane.snapshot(RetentionClass::RowTtl).stuck_slices > 0 {
				return true;
			}
			if started.elapsed() >= CONFLICT_SEARCH_LIMIT {
				return false;
			}
		}
	});

	assert!(conflicted, "no conflict seen, the test proved nothing");

	clock.advance_secs(PAST_TTL_SECONDS);
	task.run_slice();
	assert_eq!(
		TestEngine::row_count(&test.query("from test::rb")),
		0,
		"eviction must still drain the ring once the writer stops"
	);
}
