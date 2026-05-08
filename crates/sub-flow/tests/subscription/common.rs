// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// Hydration-parity test harness shared across operators.
//
// Hypothesis under test: subscribing with hydration enabled (the default) replays existing
// source rows in bulk at a pinned MVCC version V. Doing so should produce sink output
// equivalent to incrementally replaying the same rows as a series of single-row commits
// (the path subscribers using WITH { hydration: { enabled: false } } get via CDC catch-up).
//
// Comparison granularity: SINK OUTPUT only. Operator-state byte comparison would require a
// trait-level accessor on SubscriptionService and is left as a follow-up.
//
// Randomization: workspace `rand` (proptest is not vendored). 16 cases per operator. No shrinking;
// failure messages include the seed and rows for repro.
//
// Failure policy: surface, do not fix. Per standing instruction, operator code is never modified
// in response to a parity failure - failures get documented as regression reproducers.

#![allow(dead_code)]

use std::{thread, time::Duration};

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb::{Params, embedded as db_embedded};
use reifydb_core::{interface::catalog::id::SubscriptionId, value::column::columns::Columns};
use reifydb_engine::subscription::SubscriptionServiceRef;
use reifydb_sub_subscription::subsystem::SubscriptionSubsystem;
use reifydb_type::value::{Value, identity::IdentityId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
	pub id: i32,
	pub qty: i32,
	pub ts_ms: i64,
}

pub fn extract_sub_id(frames: &[reifydb_type::value::frame::frame::Frame]) -> SubscriptionId {
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

pub fn make_db() -> reifydb::Database {
	let mut db = db_embedded::memory().build().expect("build");
	db.start().expect("start");
	db.admin_as_root("CREATE NAMESPACE app", Params::None).expect("create namespace");
	db.admin_as_root("CREATE TABLE app::t { id: int4, qty: int4, ts_ms: int8 }", Params::None)
		.expect("create table");
	db
}

pub fn insert_all_at_once(db: &reifydb::Database, rows: &[Row]) {
	if rows.is_empty() {
		return;
	}
	let mut stmt = String::from("INSERT app::t [");
	for (i, r) in rows.iter().enumerate() {
		if i > 0 {
			stmt.push(',');
		}
		stmt.push_str(&format!("{{id: {}, qty: {}, ts_ms: {}}}", r.id, r.qty, r.ts_ms));
	}
	stmt.push(']');
	db.command_as_root(&stmt, Params::None).expect("bulk insert");
}

pub fn insert_one_at_a_time(db: &reifydb::Database, rows: &[Row]) {
	for r in rows {
		let stmt = format!("INSERT app::t [{{id: {}, qty: {}, ts_ms: {}}}]", r.id, r.qty, r.ts_ms);
		db.command_as_root(&stmt, Params::None).expect("incremental insert");
	}
}

pub fn drain_sub(db: &reifydb::Database, sub_id: SubscriptionId) -> Vec<Columns> {
	let subsystem = db.subsystem::<SubscriptionSubsystem>().expect("subscription subsystem present");
	let store = subsystem.store();
	store.drain(&sub_id, usize::MAX)
}

pub fn poll_until_stable(db: &reifydb::Database, sub_id: SubscriptionId) -> Vec<Columns> {
	let subsystem = db.subsystem::<SubscriptionSubsystem>().expect("subscription subsystem present");
	let store = subsystem.store();
	let deadline = std::time::Instant::now() + Duration::from_secs(5);
	let quiescent_threshold = 3;
	let mut last_seen: Vec<Columns> = Vec::new();
	let mut consecutive_empty = 0;
	thread::sleep(Duration::from_millis(50));
	loop {
		let drained = store.drain(&sub_id, usize::MAX);
		if drained.is_empty() {
			consecutive_empty += 1;
			if consecutive_empty >= quiescent_threshold {
				return last_seen;
			}
		} else {
			consecutive_empty = 0;
			last_seen.extend(drained);
		}
		if std::time::Instant::now() >= deadline {
			return last_seen;
		}
		thread::sleep(Duration::from_millis(50));
	}
}

// Use when the subscription preserves the source schema (id, qty, ts_ms).
pub fn normalize(batches: Vec<Columns>) -> Vec<(i32, i32, i64)> {
	let mut out: Vec<(i32, i32, i64)> = Vec::new();
	for cols in batches {
		let id_col = cols.iter().find(|c| c.name().text() == "id");
		let qty_col = cols.iter().find(|c| c.name().text() == "qty");
		let ts_col = cols.iter().find(|c| c.name().text() == "ts_ms");
		let (Some(id_col), Some(qty_col), Some(ts_col)) = (id_col, qty_col, ts_col) else {
			let names: Vec<&str> = cols.iter().map(|c| c.name().text()).collect();
			panic!("expected columns id, qty, ts_ms but found {:?}", names);
		};
		for i in 0..cols.row_count() {
			let id = match id_col.data().get_value(i) {
				Value::Int4(v) => v,
				other => panic!("expected Int4 id, got {:?}", other),
			};
			let qty = match qty_col.data().get_value(i) {
				Value::Int4(v) => v,
				other => panic!("expected Int4 qty, got {:?}", other),
			};
			let ts = match ts_col.data().get_value(i) {
				Value::Int8(v) => v,
				other => panic!("expected Int8 ts_ms, got {:?}", other),
			};
			out.push((id, qty, ts));
		}
	}
	out.sort();
	out
}

// Use when the subscription's output schema isn't (id, qty, ts_ms) - e.g. aggregations,
// projections, windows. Captures every column as (name, debug-formatted value) so the test
// works regardless of the operator's emitted shape.
pub fn normalize_aggregated(batches: Vec<Columns>) -> Vec<Vec<(String, String)>> {
	let mut out: Vec<Vec<(String, String)>> = Vec::new();
	for cols in batches {
		let mut row_records: Vec<Vec<(String, String)>> = vec![Vec::new(); cols.row_count()];
		for col in cols.iter() {
			let name = col.name().text().to_string();
			for i in 0..cols.row_count() {
				let v = format!("{:?}", col.data().get_value(i));
				row_records[i].push((name.clone(), v.clone()));
			}
		}
		for mut rec in row_records {
			rec.sort();
			out.push(rec);
		}
	}
	out.sort();
	out
}

// Path A: bulk-insert all rows in one commit, then create subscription, then call hydrate.
pub fn run_path_snapshot(rql: &str, rows: &[Row]) -> Vec<Columns> {
	let db = make_db();
	insert_all_at_once(&db, rows);

	let create_stmt = format!("CREATE SUBSCRIPTION AS {{ {} }}", rql);
	let frames = db.admin_as_root(&create_stmt, Params::None).expect("create subscription");
	let sub_id = extract_sub_id(&frames);

	let engine = db.engine().clone();
	let (_, lease) = engine.acquire_current_snapshot_lease().expect("acquire lease");
	let services = engine.services();
	let sub_service = services.ioc.resolve::<SubscriptionServiceRef>().expect("resolve service");

	thread::sleep(Duration::from_millis(50));

	let outcome = sub_service.hydrate(sub_id, &engine, IdentityId::root(), lease, 100_000).expect("hydrate");

	let mut all = outcome.batches;
	thread::sleep(Duration::from_millis(150));
	all.extend(drain_sub(&db, sub_id));
	all
}

// Path B: create subscription on empty table, insert rows one at a time, let CDC catch up.
pub fn run_path_incremental(rql: &str, rows: &[Row]) -> Vec<Columns> {
	let db = make_db();

	let create_stmt = format!("CREATE SUBSCRIPTION AS {{ {} }}", rql);
	let frames = db.admin_as_root(&create_stmt, Params::None).expect("create subscription");
	let sub_id = extract_sub_id(&frames);

	thread::sleep(Duration::from_millis(50));

	insert_one_at_a_time(&db, rows);

	poll_until_stable(&db, sub_id)
}

pub fn random_rows(seed: u64, count: usize, max_id: i32) -> Vec<Row> {
	let mut rng = StdRng::seed_from_u64(seed);
	(0..count)
		.map(|_| Row {
			id: rng.random_range(1..=max_id),
			qty: rng.random_range(0..1000),
			ts_ms: rng.random_range(0..1_000_000),
		})
		.collect()
}
