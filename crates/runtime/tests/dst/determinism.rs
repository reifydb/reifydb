// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use reifydb_runtime::actor::context::Context;

use super::helpers::*;

/// Run a fixed scenario and return the processing trace (actor_ids in order).
fn run_scenario(seed: u64) -> Vec<usize> {
	let system = test_system_with_seed(seed);
	let log = new_log();

	let a = system.spawn_system(
		"a",
		LogActor {
			log: log.clone(),
		},
	);
	let b = system.spawn_system(
		"b",
		LogActor {
			log: log.clone(),
		},
	);
	let c = system.spawn_system(
		"c",
		LogActor {
			log: log.clone(),
		},
	);

	a.actor_ref.send("a1".into()).unwrap();
	b.actor_ref.send("b1".into()).unwrap();
	c.actor_ref.send("c1".into()).unwrap();
	a.actor_ref.send("a2".into()).unwrap();
	b.actor_ref.send("b2".into()).unwrap();

	collect_step_trace(&system)
}

#[test]
fn same_seed_same_trace() {
	let trace1 = run_scenario(42);
	let trace2 = run_scenario(42);

	assert_eq!(trace1, trace2);
	assert_eq!(trace1, vec![0, 1, 2, 0, 1]); // a=0, b=1, c=2
}

#[test]
fn same_seed_same_log_contents() {
	let verify = |seed: u64| -> Vec<String> {
		let system = test_system_with_seed(seed);
		let log = new_log();

		let a = system.spawn_system(
			"a",
			LogActor {
				log: log.clone(),
			},
		);
		let b = system.spawn_system(
			"b",
			LogActor {
				log: log.clone(),
			},
		);

		a.actor_ref.send("x".into()).unwrap();
		b.actor_ref.send("y".into()).unwrap();
		a.actor_ref.send("z".into()).unwrap();

		system.run_until_idle();
		log_contents(&log)
	};

	assert_eq!(verify(99), verify(99));
	assert_eq!(verify(99), vec!["x", "y", "z"]);
}

#[test]
fn different_seed_different_clock() {
	let s1 = test_system_with_seed(100);
	let s2 = test_system_with_seed(200);

	assert_eq!(s1.clock().now_millis(), 100);
	assert_eq!(s2.clock().now_millis(), 200);
}

/// Run a scenario with timers and return the log contents.
fn run_timer_scenario(seed: u64) -> Vec<String> {
	let system = test_system_with_seed(seed);
	let log = new_log();
	let handle = system.spawn_system(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());
	ctx.schedule_once(Duration::from_millis(300), || "t300".to_string());
	ctx.schedule_once(Duration::from_millis(100), || "t100".to_string());
	ctx.schedule_once(Duration::from_millis(200), || "t200".to_string());

	// Also send a direct message.
	handle.actor_ref.send("direct".into()).unwrap();

	system.advance_time(Duration::from_millis(300));
	system.run_until_idle();

	log_contents(&log)
}

#[test]
fn deterministic_timer_ordering() {
	let result1 = run_timer_scenario(0);
	let result2 = run_timer_scenario(0);

	assert_eq!(result1, result2);
	// Direct message (ts=0) comes first, then timers in deadline order.
	assert_eq!(result1, vec!["direct", "t100", "t200", "t300"]);
}

#[test]
fn complex_scenario_reproducible() {
	let run = |seed: u64| -> (Vec<usize>, Vec<String>) {
		let system = test_system_with_seed(seed);
		let log = new_log();

		let a = system.spawn_system(
			"a",
			LogActor {
				log: log.clone(),
			},
		);
		let b = system.spawn_system(
			"b",
			ForwardActor {
				target: a.actor_ref.clone(),
			},
		);

		b.actor_ref.send("hello".into()).unwrap();
		a.actor_ref.send("world".into()).unwrap();

		let ctx = Context::new(a.actor_ref.clone(), system.clone(), system.cancellation_token());
		ctx.schedule_once(Duration::from_millis(50), || "timer".to_string());

		system.advance_time(Duration::from_millis(50));

		let trace = collect_step_trace(&system);
		let contents = log_contents(&log);
		(trace, contents)
	};

	let (trace1, log1) = run(7);
	let (trace2, log2) = run(7);

	assert_eq!(trace1, trace2);
	assert_eq!(log1, log2);
}
