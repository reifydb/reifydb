// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! End-to-end subscription tests under dst, over a running flow subsystem.
//!
//! The unit tests in `settle.rs` all build without a flow subsystem, so they only ever exercise a
//! subscription reading straight from a table. That is the short path: one hop, and it settles in
//! the confirming pass. Everything uptime actually subscribes to is a deferred view, where a write
//! has to reach the cdc producer, be polled by the flow actors, be materialized, be polled again by
//! the subscription consumer and only then be staged. These tests cover that path.

#![cfg(reifydb_dst)]

use reifydb::{
	Database, WithSubsystem, embedded,
	runtime::{RuntimeConfig, fatal::FatalConfig},
};
use reifydb_core::interface::catalog::subscription::HydrationConfig;
use reifydb_value::{params::Params, value::frame::frame::Frame};

/// A database with a flow subsystem, seeded so the run is reproducible.
fn db() -> Database {
	embedded::memory()
		.with_runtime_config(RuntimeConfig::default().seeded(7).fatal(FatalConfig::disarmed()))
		.with_flow(|flow| flow)
		.build()
		.expect("dst database with a flow subsystem builds")
}

/// A table plus a deferred view over it. The view is what a subscription attaches to.
fn with_view() -> Database {
	let db = db();
	db.admin_as_root("CREATE NAMESPACE app", Params::None).expect("create namespace");
	db.admin_as_root("CREATE TABLE app::t { id: int4, val: int4 }", Params::None).expect("create table");
	db.admin_as_root(
		"CREATE DEFERRED VIEW app::v { id: int4, val: int4 } AS { FROM app::t MAP { id, val } }",
		Params::None,
	)
	.expect("create deferred view");
	db
}

fn insert(db: &Database, id: i32, val: i32) {
	db.command_as_root(&format!("INSERT app::t [{{id: {id}, val: {val}}}]"), Params::None).expect("insert");
}

fn row_count(frames: &[Frame]) -> usize {
	frames.iter().map(|f| f.row_count()).sum()
}

#[test]
fn settle_delivers_a_write_through_a_deferred_view() {
	// The write does not reach the subscriber by itself: it has to cross the flow subsystem
	// first. Until the event bus deadlock was fixed this database could not even be built under
	// dst, so nothing covered the path uptime actually uses.
	let db = with_view();
	let sub = db
		.subscribe_as_root("FROM app::v MAP { id, val }", Params::None, HydrationConfig::default())
		.expect("subscribe to the view");
	db.settle_subscriptions().expect("settle after subscribe");
	sub.drain(usize::MAX);

	insert(&db, 1, 10);
	let settled = db.settle_subscriptions().expect("settle after insert");

	assert_eq!(row_count(&sub.drain(usize::MAX)), 1, "the view row must reach the subscriber");
	assert!(settled.is_complete(), "no subscription may have overrun: {:?}", settled.lagged);
}

#[test]
fn a_view_write_reaches_the_subscriber_only_once_the_driver_runs() {
	// Under dst the calling thread is the executor, so nothing crosses the flow subsystem on its
	// own. Pinning the before state is what makes the after state mean something: if delivery ever
	// started happening without the driver, this would catch it, and the settle call in every other
	// test here would silently stop proving anything.
	let db = with_view();
	let sub = db
		.subscribe_as_root("FROM app::v MAP { id, val }", Params::None, HydrationConfig::default())
		.expect("subscribe to the view");
	db.settle_subscriptions().expect("settle after subscribe");
	sub.drain(usize::MAX);

	insert(&db, 1, 10);
	assert_eq!(row_count(&sub.drain(usize::MAX)), 0, "nothing may cross the flow subsystem unsettled");

	db.settle_subscriptions().expect("settle after insert");
	assert_eq!(row_count(&sub.drain(usize::MAX)), 1, "the driver must deliver the write");
}

#[test]
fn hydration_delivers_view_rows_that_predate_the_subscription() {
	// Uptime subscribes to views that already hold data, so a subscriber that only saw forward
	// changes would render an empty page until the next write happened to arrive.
	let db = with_view();
	for id in 1..=3 {
		insert(&db, id, id * 10);
	}
	db.settle_subscriptions().expect("settle the writes into the view");

	let sub = db
		.subscribe_as_root("FROM app::v MAP { id, val }", Params::None, HydrationConfig::default())
		.expect("subscribe after the rows exist");
	db.settle_subscriptions().expect("settle after subscribe");

	assert_eq!(row_count(&sub.drain(usize::MAX)), 3, "hydration must deliver the rows already in the view");
}

#[test]
fn hydration_delivers_nothing_when_disabled() {
	// The counterpart to the test above: with hydration off the same subscription must start
	// empty, which is what proves the rows above came from hydration and not from forward cdc.
	let db = with_view();
	for id in 1..=3 {
		insert(&db, id, id * 10);
	}
	db.settle_subscriptions().expect("settle the writes into the view");

	let sub = db
		.subscribe_as_root(
			"FROM app::v MAP { id, val }",
			Params::None,
			HydrationConfig {
				enabled: false,
				max_rows: None,
			},
		)
		.expect("subscribe without hydration");
	db.settle_subscriptions().expect("settle after subscribe");

	assert_eq!(row_count(&sub.drain(usize::MAX)), 0, "a subscription without hydration starts empty");
}

#[test]
fn hydration_over_max_rows_is_refused_rather_than_truncated() {
	// max_rows is a guard, not a cap: a view holding more rows than the caller budgeted fails the
	// subscribe outright. Truncating instead would hand the caller a silently partial snapshot it
	// could not tell apart from a complete one, so the error is the contract worth pinning.
	let db = with_view();
	for id in 1..=5 {
		insert(&db, id, id * 10);
	}
	db.settle_subscriptions().expect("settle the writes into the view");

	// `Subscription` is not Debug, so the Ok arm is discharged by hand rather than via expect_err.
	let message = match db.subscribe_as_root(
		"FROM app::v MAP { id, val }",
		Params::None,
		HydrationConfig {
			enabled: true,
			max_rows: Some(2),
		},
	) {
		Ok(_) => panic!("a view of 5 rows must not hydrate under a 2 row budget"),
		Err(e) => e.to_string(),
	};
	assert!(message.contains("max_rows=2"), "the refusal must name the budget it broke: {message}");

	// The same subscription under a sufficient budget delivers every row, which is what shows the
	// refusal came from the cap and not from hydration being broken.
	let sub = db
		.subscribe_as_root(
			"FROM app::v MAP { id, val }",
			Params::None,
			HydrationConfig {
				enabled: true,
				max_rows: Some(5),
			},
		)
		.expect("a budget that fits the view hydrates");
	db.settle_subscriptions().expect("settle after subscribe");
	assert_eq!(row_count(&sub.drain(usize::MAX)), 5, "a sufficient budget delivers the whole view");
}

#[test]
fn stopping_delivers_a_write_still_in_flight_through_a_view() {
	// stop() is the only thing between a write and shutdown that steps an actor under dst. With a
	// flow subsystem in the path there is strictly more in flight to lose, so if the shutdown
	// drain does not settle, the write is dropped at exit. No explicit settle here on purpose.
	let mut db = with_view();
	let sub = db
		.subscribe_as_root("FROM app::v MAP { id, val }", Params::None, HydrationConfig::default())
		.expect("subscribe to the view");
	db.settle_subscriptions().expect("settle after subscribe");
	sub.drain(usize::MAX);

	insert(&db, 1, 10);
	db.stop().expect("stop");

	assert_eq!(row_count(&sub.drain(usize::MAX)), 1, "shutdown must drive the write through the view");
}
