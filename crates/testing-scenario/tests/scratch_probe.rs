// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::db::TestDb;

const ROWS: u64 = 1_000;
const START: u64 = 500;
const WINDOW: u64 = 100;

fn seed(db: &TestDb) {
	db.try_admin("create namespace bench").unwrap();
	db.try_admin("create table bench::users { id: int8, name: utf8, email: utf8 }").unwrap();

	for chunk in (0..ROWS).collect::<Vec<u64>>().chunks(200) {
		let rows: Vec<String> = chunk
			.iter()
			.map(|index| {
				format!(
					"{{ id: {}, name: \"user_{}\", email: \"user_{}@bench.test\" }}",
					index, index, index
				)
			})
			.collect();
		db.try_command(&format!("INSERT bench::users [{}]", rows.join(", "))).unwrap();
	}
}

#[test]
fn range_scan_take_returns_only_rows_past_the_filter_bound() {
	// The scan benchmark reports a flat ~61us at both 10k and 100k rows even though the table has
	// no index. That is only possible if `take` fills before the scan walks deep into the table.
	// It does, because the table scan yields rows newest-first: with ids ascending in insert
	// order, `id > start` matches on the very first rows produced. Pinning the order down here
	// matters because it is what makes the scan scenario a ~WINDOW-row measurement rather than a
	// scale-dependent one - if the scan ever became ascending, the benchmark would silently turn
	// into an O(scale) probe and the flat number would stop meaning what it means today.
	let db = TestDb::memory();
	seed(&db);

	let frames = db.try_query(&format!("from bench::users filter id > {} take {}", START, WINDOW)).unwrap();

	let mut ids = Vec::new();
	for frame in &frames {
		for row in 0..frame.row_count() {
			let rendered = format!("{:?}", frame[0].data.get_value(row));
			ids.push(rendered);
		}
	}

	println!("returned_rows={} first={:?} last={:?}", ids.len(), ids.first(), ids.last());
	assert_eq!(ids.len() as u64, WINDOW, "take should yield exactly WINDOW rows");
	assert_eq!(ids.first().map(String::as_str), Some("Int8(999)"), "scan yields the newest row first");
	assert_eq!(
		ids.last().map(String::as_str),
		Some("Int8(900)"),
		"take fills from the newest WINDOW rows, so the scan never reaches the filter bound"
	);
}
