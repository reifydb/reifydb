// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TestDb;
use reifydb_core::interface::change::StagedBatch;
use reifydb_value::value::{Value, diff_type::DiffType};

use crate::common::{
	Row, drain_after_consumer_caught_up, extract_sub_id, make_db, normalize, random_rows, run_path_incremental,
	run_path_snapshot,
};

#[test]
fn distinct_emits_most_recent_row_per_key() {
	// `distinct {key}` emits one row per key, the latest arrival to carry it, replacing the previous one.
	// The bulk-hydrate and incremental paths must converge on the same final sink state.
	let rql = "from app::t | distinct {id}";
	let rows = vec![
		Row {
			id: 3,
			qty: 320,
			ts_ms: 881420,
		},
		Row {
			id: 4,
			qty: 948,
			ts_ms: 821663,
		},
		Row {
			id: 4,
			qty: 351,
			ts_ms: 293762,
		},
	];
	let expected = vec![(3, 320, 881420), (4, 351, 293762)];

	assert_eq!(
		normalize(run_path_snapshot(rql, &rows)),
		expected,
		"snapshot path must emit the most recent row seen for each distinct key"
	);
	assert_eq!(
		normalize(run_path_incremental(rql, &rows)),
		expected,
		"incremental path must emit the most recent row seen for each distinct key"
	);
}

fn ops_and_qty(batches: Vec<StagedBatch>) -> Vec<(DiffType, i32)> {
	// normalize collapses the op sequence into a final state, so an insert and an update read alike there.
	let mut out = Vec::new();
	for (op, cols) in batches {
		let qty_col = cols.iter().find(|c| c.name().text() == "qty").expect("subscription output carries qty");
		for i in 0..cols.row_count() {
			let qty = match qty_col.data().get_value(i) {
				Value::Int4(v) => v,
				other => panic!("expected Int4 qty, got {:?}", other),
			};
			out.push((op, qty));
		}
	}
	out
}

fn subscribe(db: &TestDb, rql: &str) -> reifydb_core::interface::catalog::id::SubscriptionId {
	extract_sub_id(&db.admin(&format!("CREATE SUBSCRIPTION AS {{ {} }}", rql)))
}

#[test]
fn distinct_state_outlives_the_transaction_that_produced_it() {
	// Each commit is its own change on its own transaction, so operator state must outlive the transaction.
	let db = make_db();
	let sub_id = subscribe(&db, "from app::t | distinct {id}");

	db.command("INSERT app::t [{id: 4, qty: 948, ts_ms: 1}]");
	// Commit 2 arrives as an update to the delivered row only if the sink kept its delivered-row set.
	db.command("INSERT app::t [{id: 4, qty: 351, ts_ms: 2}]");
	// Commit 3 resurrects the shadowed qty=948 row only if distinct kept its entry map.
	db.command("DELETE app::t FILTER { qty == 351 }");

	assert_eq!(
		ops_and_qty(drain_after_consumer_caught_up(&db, sub_id)),
		vec![(DiffType::Insert, 948), (DiffType::Update, 351), (DiffType::Update, 948)],
		"operator state must carry across the three commits"
	);
}

#[test]
fn distinct_parity() {
	let rql = "from app::t | distinct {id}";
	for case in 0..16 {
		let seed: u64 = 2000 + case;
		let count = ((seed % 9) + 1) as usize;
		let rows = random_rows(seed, count, 5);
		let a = normalize(run_path_snapshot(rql, &rows));
		let b = normalize(run_path_incremental(rql, &rows));
		assert_eq!(
			a, b,
			"DISTINCT parity failed for seed={} rows={:?}\nsnapshot path={:?}\nincremental={:?}",
			seed, rows, a, b
		);
	}
}
