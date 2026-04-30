// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use reifydb_runtime::actor::context::Context;

use super::helpers::*;

#[test]
fn scope_shares_clock() {
	let parent = test_system();
	let child = parent.scope();

	parent.advance_time(Duration::from_millis(500));

	// Both should see the same mock clock value.
	assert_eq!(parent.clock().now_millis(), 500);
	assert_eq!(child.clock().now_millis(), 500);
}

#[test]
fn scope_has_own_actors() {
	let parent = test_system();
	let child = parent.scope();

	let _pa = parent.spawn("pa", CounterActor);
	let _ca = child.spawn("ca", CounterActor);

	assert_eq!(parent.alive_count(), 1);
	assert_eq!(child.alive_count(), 1);
}

#[test]
fn scope_has_own_cancel() {
	let parent = test_system();
	let child = parent.scope();

	let _pa = parent.spawn("pa", CounterActor);
	let _ca = child.spawn("ca", CounterActor);

	// Shut down child scope only.
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

	let _pa = parent.spawn("pa", CounterActor);
	let ch = child.spawn("ca", CounterActor);

	// Shut down parent - should propagate to child.
	parent.shutdown();

	assert!(parent.is_cancelled());
	assert!(child.is_cancelled());
	assert_eq!(parent.alive_count(), 0);
	assert_eq!(child.alive_count(), 0);

	// Child actor should be dead.
	assert!(ch.actor_ref.send(CounterMessage::Inc).is_err());
}

#[test]
fn scope_shares_timer_heap() {
	let parent = test_system();
	let child = parent.scope();

	let log = new_log();
	let handle = child.spawn(
		"log",
		LogActor {
			log: log.clone(),
		},
	);

	// Schedule timer via parent's context but targeting child's actor.
	let ctx = Context::new(handle.actor_ref.clone(), parent.clone(), parent.cancellation_token());
	ctx.schedule_once(Duration::from_millis(100), || "from_parent_timer".to_string());

	// Advance time on parent - timers are shared.
	parent.advance_time(Duration::from_millis(100));

	// Process on child - the message should be there.
	child.run_until_idle();

	assert_eq!(log_contents(&log), vec!["from_parent_timer"]);
}

#[test]
fn cross_scope_messaging() {
	let parent = test_system();
	let child = parent.scope();

	let log = new_log();
	let child_actor = child.spawn(
		"child_log",
		LogActor {
			log: log.clone(),
		},
	);

	// Send from outside the child scope to child's actor.
	child_actor.actor_ref.send("cross_scope".into()).unwrap();

	child.run_until_idle();

	assert_eq!(log_contents(&log), vec!["cross_scope"]);
}

#[test]
fn nested_scope_must_shutdown_recursively() {
	let root = test_system();
	let level1 = root.scope();
	let level2 = level1.scope();

	let _r = root.spawn("root_actor", CounterActor);
	let _l1 = level1.spawn("level1_actor", CounterActor);
	let _l2 = level2.spawn("level2_actor", CounterActor);

	assert_eq!(root.alive_count(), 1);
	assert_eq!(level1.alive_count(), 1);
	assert_eq!(level2.alive_count(), 1);

	// Shut down level1.
	level1.shutdown();

	// level1 and level2 MUST be dead. root should be alive.
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

	// 1. Advance child clock - parent MUST NOT be affected.
	child.advance_time(std::time::Duration::from_millis(100));
	assert_eq!(child.clock().now_millis(), 100);
	assert_eq!(parent.clock().now_millis(), 0, "Child clock advancement leaked to parent!");

	// 2. Advance parent clock - child MUST be affected.
	parent.advance_time(std::time::Duration::from_millis(200));
	assert_eq!(parent.clock().now_millis(), 200);
	// Child clock was at 100, we advanced parent by 200. Does child become 300 or 200?
	// If it's a "shared" mock clock, it might be 200. If child has an offset, it might be 300.
	// But it certainly should be at least 200.
	assert!(child.clock().now_millis() >= 200, "Child clock failed to advance with parent!");
}
