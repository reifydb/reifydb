// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{thread, time::Duration};

use reifydb::{Database, Params, embedded as db_embedded};
use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_engine::{
	engine::StandardEngine,
	subscription::{HydrateError, SubscriptionServiceRef},
};
use reifydb_transaction::multi::lease::VersionLeaseGuard;
use reifydb_type::value::{Value, frame::frame::Frame, identity::IdentityId};

fn extract_sub_id(frames: &[Frame]) -> SubscriptionId {
	let frame = frames.first().expect("subscription frame");
	let value = frame
		.columns
		.iter()
		.find(|c| c.name == "subscription_id")
		.and_then(|c| {
			if c.data.is_empty() {
				None
			} else {
				Some(c.data.get_value(0))
			}
		})
		.expect("subscription_id column");
	match value {
		Value::Uint8(n) => SubscriptionId(n),
		other => panic!("unexpected subscription_id value: {:?}", other),
	}
}

fn engine_lease_service(db: &Database) -> (StandardEngine, VersionLeaseGuard, SubscriptionServiceRef) {
	let engine = db.engine().clone();
	let (_, lease) = engine.acquire_current_snapshot_lease().expect("acquire lease");
	let sub_service = engine.services().ioc.resolve::<SubscriptionServiceRef>().expect("resolve service");
	(engine, lease, sub_service)
}

fn create_and_setup(
	db: &Database,
	query: &str,
) -> (StandardEngine, SubscriptionId, VersionLeaseGuard, SubscriptionServiceRef) {
	let stmt = format!("CREATE SUBSCRIPTION AS {{ {} }}", query);
	let frames = db.admin_as_root(&stmt, Params::None).expect("create subscription");
	let sub_id = extract_sub_id(&frames);
	let (engine, lease, sub_service) = engine_lease_service(db);
	thread::sleep(Duration::from_millis(50));
	(engine, sub_id, lease, sub_service)
}

#[test]
fn hydrate_returns_existing_rows_at_pinned_version() {
	let mut db = db_embedded::memory().build().expect("build");
	db.start().expect("start");

	db.admin_as_root("CREATE NAMESPACE app", Params::None).expect("create namespace");
	db.admin_as_root("CREATE TABLE app::orders { id: int4, qty: int4 }", Params::None).expect("create table");

	db.command_as_root("INSERT app::orders [{id: 1, qty: 10}, {id: 2, qty: 20}, {id: 3, qty: 30}]", Params::None)
		.expect("insert seed rows");

	let (engine, sub_id, lease, sub_service) = create_and_setup(&db, "from app::orders");

	let outcome = sub_service.hydrate(sub_id, &engine, IdentityId::root(), lease, 1024).expect("hydrate succeeds");

	let total_rows: usize = outcome.batches.iter().map(|c| c.row_count()).sum();
	assert_eq!(total_rows, 3, "snapshot should contain 3 seeded rows");
}

#[test]
fn hydrate_fails_when_row_cap_exceeded() {
	let mut db = db_embedded::memory().build().expect("build");
	db.start().expect("start");

	db.admin_as_root("CREATE NAMESPACE app", Params::None).expect("create namespace");
	db.admin_as_root("CREATE TABLE app::big { id: int4 }", Params::None).expect("create table");

	let mut insert_stmt = String::from("INSERT app::big [");
	for i in 0..50 {
		if i > 0 {
			insert_stmt.push(',');
		}
		insert_stmt.push_str(&format!("{{id: {}}}", i));
	}
	insert_stmt.push(']');
	db.command_as_root(&insert_stmt, Params::None).expect("insert big set");

	let (engine, sub_id, lease, sub_service) = create_and_setup(&db, "from app::big");

	let err = sub_service
		.hydrate(sub_id, &engine, IdentityId::root(), lease, 10)
		.expect_err("expected RowCapExceeded");

	match err {
		HydrateError::RowCapExceeded {
			cap,
		} => assert_eq!(cap, 10),
		other => panic!("unexpected error: {:?}", other),
	}
}

#[test]
fn hydrate_pushes_take_into_source_query() {
	let mut db = db_embedded::memory().build().expect("build");
	db.start().expect("start");

	db.admin_as_root("CREATE NAMESPACE app", Params::None).expect("create namespace");
	db.admin_as_root("CREATE TABLE app::big { id: int4 }", Params::None).expect("create table");

	let mut insert_stmt = String::from("INSERT app::big [");
	for i in 0..50 {
		if i > 0 {
			insert_stmt.push(',');
		}
		insert_stmt.push_str(&format!("{{id: {}}}", i));
	}
	insert_stmt.push(']');
	db.command_as_root(&insert_stmt, Params::None).expect("insert big set");

	let (engine, sub_id, lease, sub_service) = create_and_setup(&db, "from app::big | take 5");

	sub_service
		.hydrate(sub_id, &engine, IdentityId::root(), lease, 10)
		.expect("hydrate succeeds: take 5 should be pushed into source so cap=10 holds");
}

#[test]
fn hydrate_pushes_filter_into_source_query() {
	let mut db = db_embedded::memory().build().expect("build");
	db.start().expect("start");

	db.admin_as_root("CREATE NAMESPACE app", Params::None).expect("create namespace");
	db.admin_as_root("CREATE TABLE app::events { id: int4, kind: utf8 }", Params::None).expect("create table");

	let mut insert_stmt = String::from("INSERT app::events [");
	let mut first = true;
	for kind in ["a", "b", "c"] {
		for i in 0..100 {
			if !first {
				insert_stmt.push(',');
			}
			first = false;
			insert_stmt.push_str(&format!("{{id: {}, kind: '{}'}}", i, kind));
		}
	}
	insert_stmt.push(']');
	db.command_as_root(&insert_stmt, Params::None).expect("insert seed rows");

	let (engine, sub_id, lease, sub_service) =
		create_and_setup(&db, "from app::events | filter { kind == 'b' } | take 5");

	// The fix: filter must be pushed into the source query, so the 5-row TAKE selects
	// 5 'b' rows (matching the filter) rather than 5 'a' rows (the first by primary key,
	// which the in-flow filter would then discard, leaving the snapshot empty).
	//
	// Cap at 5 - exactly the take limit. Without filter pushdown the source returns 5 'a'
	// rows; with filter pushdown it returns 5 'b' rows. Both fit under the cap, so this
	// test does not rely on cap-exceeded errors. It instead checks that every snapshot
	// row matches the filter, which is impossible if the filter is dropped from the
	// source query and only enforced by the downstream flow operator.
	let outcome = sub_service
		.hydrate(sub_id, &engine, IdentityId::root(), lease, 5)
		.expect("hydrate succeeds at cap=5 (matches TAKE 5)");

	let total_rows: usize = outcome.batches.iter().map(|c| c.row_count()).sum();
	assert!(total_rows > 0, "snapshot must deliver at least one filtered row");

	for cols in &outcome.batches {
		let kind_col = cols.iter().find(|c| c.name() == "kind").expect("kind column present");
		for i in 0..cols.row_count() {
			match kind_col.data().get_value(i) {
				Value::Utf8(s) => assert_eq!(s, "b", "filter must restrict to kind == 'b'"),
				other => panic!("unexpected kind value: {:?}", other),
			}
		}
	}
}

#[test]
fn hydrate_returns_subscription_not_found_for_unknown_id() {
	let mut db = db_embedded::memory().build().expect("build");
	db.start().expect("start");

	let (engine, lease, sub_service) = engine_lease_service(&db);

	let err = sub_service
		.hydrate(SubscriptionId(99_999), &engine, IdentityId::root(), lease, 1024)
		.expect_err("expected SubscriptionNotFound");

	match err {
		HydrateError::SubscriptionNotFound => {}
		other => panic!("unexpected error: {:?}", other),
	}
}

fn first_value(frames: &[Frame], name: &str) -> Option<Value> {
	let frame = frames.first()?;
	let col = frame.columns.iter().find(|c| c.name == name)?;
	if col.data.is_empty() {
		return None;
	}
	Some(col.data.get_value(0))
}

#[test]
fn create_subscription_default_returns_hydration_enabled_true_with_no_max_rows() {
	let mut db = db_embedded::memory().build().expect("build");
	db.start().expect("start");
	db.admin_as_root("CREATE NAMESPACE app", Params::None).expect("create namespace");
	db.admin_as_root("CREATE TABLE app::orders { id: int4, qty: int4 }", Params::None).expect("create table");

	let frames = db
		.admin_as_root("CREATE SUBSCRIPTION AS { FROM app::orders }", Params::None)
		.expect("create subscription");

	match first_value(&frames, "hydration_enabled") {
		Some(Value::Boolean(b)) => assert!(b, "default hydration should be enabled"),
		other => panic!("hydration_enabled column missing or wrong type: {:?}", other),
	}
	match first_value(&frames, "hydration_max_rows") {
		Some(Value::None {
			..
		})
		| None => {}
		other => panic!("hydration_max_rows should be None when not specified, got: {:?}", other),
	}
}

#[test]
fn create_subscription_with_disabled_returns_hydration_enabled_false() {
	let mut db = db_embedded::memory().build().expect("build");
	db.start().expect("start");
	db.admin_as_root("CREATE NAMESPACE app", Params::None).expect("create namespace");
	db.admin_as_root("CREATE TABLE app::orders { id: int4, qty: int4 }", Params::None).expect("create table");

	let frames = db
		.admin_as_root(
			"CREATE SUBSCRIPTION WITH { hydration: { enabled: false } } AS { FROM app::orders }",
			Params::None,
		)
		.expect("create subscription");

	match first_value(&frames, "hydration_enabled") {
		Some(Value::Boolean(b)) => assert!(!b, "explicit enabled=false should produce false"),
		other => panic!("hydration_enabled column missing or wrong type: {:?}", other),
	}
}

#[test]
fn create_subscription_with_max_rows_returns_max_rows_uint8() {
	let mut db = db_embedded::memory().build().expect("build");
	db.start().expect("start");
	db.admin_as_root("CREATE NAMESPACE app", Params::None).expect("create namespace");
	db.admin_as_root("CREATE TABLE app::orders { id: int4, qty: int4 }", Params::None).expect("create table");

	let frames = db
		.admin_as_root(
			"CREATE SUBSCRIPTION WITH { hydration: { max_rows: 250 } } AS { FROM app::orders }",
			Params::None,
		)
		.expect("create subscription");

	match first_value(&frames, "hydration_enabled") {
		Some(Value::Boolean(b)) => assert!(b, "max_rows-only should default enabled to true"),
		other => panic!("hydration_enabled wrong: {:?}", other),
	}
	match first_value(&frames, "hydration_max_rows") {
		Some(Value::Uint8(n)) => assert_eq!(n, 250, "max_rows should round-trip to 250"),
		other => panic!("hydration_max_rows wrong: {:?}", other),
	}
}
