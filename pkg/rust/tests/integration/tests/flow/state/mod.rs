// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::time::Duration;

use reifydb::{
	Value,
	testing::db::{TestDb, await_value},
};
use reifydb_test_harness::assert::column_values;

mod append;
mod join_inner;
mod join_inner_latest;
mod join_inner_snapshot;
mod join_inner_snapshot_latest;
mod join_left;
mod join_left_latest;
mod join_left_snapshot;
mod join_left_snapshot_latest;
mod window_tumbling;
mod window_tumbling_guest;

pub fn state_keys(db: &TestDb, rql: &str) -> u64 {
	// One surface row per keyspace carries the live key count as a measure, so counting rows counts keyspaces.
	db.query(rql)
		.iter()
		.flat_map(|frame| column_values(frame, "keys"))
		.map(|value| match value {
			Value::Uint8(keys) => keys,
			other => panic!("the keys measure must be an unsigned count, found {other:?}"),
		})
		.sum()
}

pub fn await_state_keys(db: &TestDb, rql: &str, want: u64, timeout: Duration) -> u64 {
	await_value(want, timeout, || state_keys(db, rql))
}
