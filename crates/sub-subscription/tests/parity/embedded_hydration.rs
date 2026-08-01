// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{thread, time::Instant};

use reifydb::{HydrationConfig, Params, Subscription};
use reifydb_core::value::column::columns::Columns;
use reifydb_test_harness::db::TestDb;
use reifydb_value::value::{duration::Duration, frame::frame::Frame};

use crate::common::{Row, insert_all_at_once, make_db, normalize};

const RQL: &str = "from app::t | map { id, qty, ts_ms }";

fn rows() -> Vec<Row> {
	vec![
		Row {
			id: 1,
			qty: 10,
			ts_ms: 100,
		},
		Row {
			id: 2,
			qty: 20,
			ts_ms: 200,
		},
		Row {
			id: 3,
			qty: 30,
			ts_ms: 300,
		},
	]
}

fn drain_collect(sub: &Subscription) -> Vec<Columns> {
	// The handle is drained rather than the raw store because the hydration snapshot lives in the handle's
	// prelude, which a store drain would bypass.
	let deadline = Instant::now() + Duration::from_seconds(10).unwrap().to_std();
	let mut acc: Vec<Frame> = Vec::new();
	let mut empty = 0u32;
	while Instant::now() < deadline {
		let batch = sub.drain(usize::MAX);
		if batch.is_empty() {
			empty += 1;
			if empty >= 5 {
				break;
			}
		} else {
			empty = 0;
			acc.extend(batch);
		}
		thread::sleep(Duration::from_milliseconds(20).unwrap().to_std());
	}
	acc.into_iter().map(Columns::from).collect()
}

fn wait_caught_up(db: &TestDb) {
	let target = db.watermarks().tx().current().expect("current version");
	assert!(
		db.watermarks().cdc().wait_for_consumer(target, Duration::from_seconds(10).unwrap()),
		"CDC consumer did not catch up to {:?}",
		target
	);
}

#[test]
fn embedded_subscribe_with_hydration_delivers_existing_rows() {
	// An embedded subscription over a populated source must replay the pre-existing rows; without that,
	// re-subscribing after a restart silently drops every row not subsequently modified.
	let db = make_db();
	let expected = rows();
	insert_all_at_once(&db, &expected);

	let sub =
		db.subscribe_as_root(RQL, Params::None, HydrationConfig::default()).expect("subscribe with hydration");

	let got = normalize(drain_collect(&sub));
	let want: Vec<(i32, i32, i64)> = expected.iter().map(|r| (r.id, r.qty, r.ts_ms)).collect();
	assert_eq!(got, want, "hydration-enabled subscription must replay the existing snapshot");
}

#[test]
fn embedded_subscribe_without_hydration_skips_existing_rows() {
	// With hydration disabled only forward changes may arrive; waiting for the consumer to pass the seed
	// inserts first is what makes "forward only" deterministic.
	let db = make_db();
	insert_all_at_once(&db, &rows());
	wait_caught_up(&db);

	let sub = db
		.subscribe_as_root(
			RQL,
			Params::None,
			HydrationConfig {
				enabled: false,
				max_rows: None,
			},
		)
		.expect("subscribe without hydration");

	let forward = Row {
		id: 4,
		qty: 40,
		ts_ms: 400,
	};
	insert_all_at_once(&db, std::slice::from_ref(&forward));
	wait_caught_up(&db);

	let got = normalize(drain_collect(&sub));
	assert_eq!(
		got,
		vec![(forward.id, forward.qty, forward.ts_ms)],
		"hydration-disabled subscription must deliver only forward changes, not the existing snapshot"
	);
}
