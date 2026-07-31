// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_value::value::Value;

use crate::common::{Row, normalize_aggregated, random_rows, run_path_incremental, run_path_snapshot};

#[test]
fn extend_parity() {
	let rql = "from app::t | extend { qty_x2: qty * 2 }";
	for case in 0..16 {
		let seed: u64 = 10_000 + case;
		let count = ((seed % 9) + 1) as usize;
		let rows = random_rows(seed, count, 1_000);
		let a = normalize_aggregated(run_path_snapshot(rql, &rows));
		let b = normalize_aggregated(run_path_incremental(rql, &rows));
		assert_eq!(
			a, b,
			"EXTEND parity failed for seed={} rows={:?}\nsnapshot path={:?}\nincremental={:?}",
			seed, rows, a, b
		);
	}
}

fn as_i64(value: Value) -> i64 {
	match value {
		Value::Int1(v) => v as i64,
		Value::Int2(v) => v as i64,
		Value::Int4(v) => v as i64,
		Value::Int8(v) => v,
		other => panic!("expected an integer qty/qty_x2 value, got {:?}", other),
	}
}

// Pull (qty, qty_x2) for every delivered row, independent of batch and row ordering.
fn qty_and_doubled(batches: Vec<Columns>) -> Vec<(i64, i64)> {
	let mut out = Vec::new();
	for cols in batches {
		let qty_col = cols
			.iter()
			.find(|c| c.name().text() == "qty")
			.expect("output must carry the source qty column");
		let x2_col = cols
			.iter()
			.find(|c| c.name().text() == "qty_x2")
			.expect("EXTEND must add the computed qty_x2 column");
		for i in 0..cols.row_count() {
			out.push((as_i64(qty_col.data().get_value(i)), as_i64(x2_col.data().get_value(i))));
		}
	}
	out
}

// EXTEND must actually compute and append qty_x2 = qty * 2 while keeping the source columns, not
// pass rows through unchanged. The parity test alone cannot catch a pass-through stub: it only
// compares the snapshot and incremental paths to each other, so both would be equally wrong and
// still match. This pins both the presence of the extended column and its computed value.
#[test]
fn extend_appends_doubled_qty() {
	let rql = "from app::t | extend { qty_x2: qty * 2 }";
	let rows = vec![
		Row {
			id: 1,
			qty: 10,
			ts_ms: 100,
		},
		Row {
			id: 2,
			qty: 0,
			ts_ms: 200,
		},
		Row {
			id: 3,
			qty: 7,
			ts_ms: 300,
		},
	];

	let pairs = qty_and_doubled(run_path_snapshot(rql, &rows));

	assert_eq!(pairs.len(), rows.len(), "every source row must be delivered with its extended column");
	for (qty, qty_x2) in pairs {
		assert_eq!(qty_x2, 2 * qty, "EXTEND must set qty_x2 = qty * 2");
	}
}
