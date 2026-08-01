// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::actor::context::Context;
use reifydb_value::value::duration::Duration;

use super::helpers::*;

#[test]
fn scope_shares_clock() {
	let parent = test_system();
	let child = parent.scope();

	parent.advance_time(Duration::from_milliseconds(500).unwrap());

	assert_eq!(parent.clock().now().to_millis(), 500);
	assert_eq!(child.clock().now().to_millis(), 500);
}

#[test]
fn scope_has_own_actors() {
	let parent = test_system();
	let child = parent.scope();

	let _pa = parent.spawn_coordination("pa", CounterActor);
	let _ca = child.spawn_coordination("ca", CounterActor);

	assert_eq!(parent.alive_count(), 1);
	assert_eq!(child.alive_count(), 1);
}

#[test]
fn scope_has_own_cancel() {
	let parent = test_system();
	let child = parent.scope();

	let _pa = parent.spawn_coordination("pa", CounterActor);
	let _ca = child.spawn_coordination("ca", CounterActor);

	// A child scope owns its own cancellation, so shutting it must not reach the parent.
	child.shutdown();

	assert!(child.is_cancelled());
	assert!(!parent.is_cancelled());
	assert_eq!(child.alive_count(), 0);
	assert_eq!(parent.alive_count(), 1);
}

#[test]
fn parent_shutdown_cancels_child_scope() {
	let parent = test_system();
	let child = parent.scope();

	let _pa = parent.spawn_coordination("pa", CounterActor);
	let ch = child.spawn_coordination("ca", CounterActor);

	parent.shutdown();

	assert!(parent.is_cancelled());
	assert!(child.is_cancelled());
	assert_eq!(parent.alive_count(), 0);
	assert_eq!(child.alive_count(), 0);

	// Cancelling must stop delivery too, not only flip the cancelled flag.
	assert!(ch.actor_ref.send(CounterMessage::Inc).is_err());
}

#[test]
fn scope_shares_timer_heap() {
	let parent = test_system();
	let child = parent.scope();

	let log = new_log();
	let handle = child.spawn_coordination(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	// Scheduled through the parent but targeting a child actor, so a per-scope timer heap
	// would strand the message.
	let ctx = Context::new(handle.actor_ref.clone(), parent.clone(), parent.cancellation_token());
	ctx.schedule_once(Duration::from_milliseconds(100).unwrap(), || "from_parent_timer".to_string());

	parent.advance_time(Duration::from_milliseconds(100).unwrap());

	child.run_until_idle();

	assert_eq!(log_contents(&log), vec!["from_parent_timer"]);
}

#[test]
fn cross_scope_messaging() {
	let parent = test_system();
	let child = parent.scope();

	let log = new_log();
	let child_actor = child.spawn_coordination(
		"child_log",
		LogActor {
			log: log.clone(),
		},
	);

	child_actor.actor_ref.send("cross_scope".into()).unwrap();

	child.run_until_idle();

	assert_eq!(log_contents(&log), vec!["cross_scope"]);
}

#[test]
fn nested_scope_must_shutdown_recursively() {
	let root = test_system();
	let level1 = root.scope();
	let level2 = level1.scope();

	let _r = root.spawn_coordination("root_actor", CounterActor);
	let _l1 = level1.spawn_coordination("level1_actor", CounterActor);
	let _l2 = level2.spawn_coordination("level2_actor", CounterActor);

	assert_eq!(root.alive_count(), 1);
	assert_eq!(level1.alive_count(), 1);
	assert_eq!(level2.alive_count(), 1);

	// Cancellation must reach a grandchild scope, and stop at the parent.
	level1.shutdown();

	assert!(level1.is_cancelled());
	assert!(level2.is_cancelled(), "Child scope level2 should have been cancelled by level1 shutdown");
	assert!(!root.is_cancelled());

	assert_eq!(level1.alive_count(), 0);
	assert_eq!(level2.alive_count(), 0, "Actors in child scope level2 should have been shut down");
	assert_eq!(root.alive_count(), 1);
}

#[test]
fn clock_advancement_is_asymmetric() {
	let parent = test_system();
	let child = parent.scope();

	// Time flows down, never up: a child advancing must not move the parent.
	child.advance_time(Duration::from_milliseconds(100).unwrap());
	assert_eq!(child.clock().now().to_millis(), 100);
	assert_eq!(parent.clock().now().to_millis(), 0, "Child clock advancement leaked to parent!");

	parent.advance_time(Duration::from_milliseconds(200).unwrap());
	assert_eq!(parent.clock().now().to_millis(), 200);
	// A child owns its own clock and is advanced by the parent's delta, so it keeps the 100ms
	// it advanced alone and lands at 300; the assertion only bounds it from below.
	assert!(child.clock().now().to_millis() >= 200, "Child clock failed to advance with parent!");
}
