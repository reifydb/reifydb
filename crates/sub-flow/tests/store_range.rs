// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

// Regression guard for review #2 (store range "cap"). NativeStore::range wraps
// flow_txn.range(range, 1024) and .collect()s it; the FFI host_store_range path
// wraps the identical call and drains it fully. The 1024 is the storage
// pagination batch_size, NOT a row limit. This test pins the operator-facing
// native StoreApi layer: with more than 1024 rows in range, range() must return
// every one. A native-side cap such as `.take(1024)` on the collected iterator
// would make this fail. The underlying storage pagination (that batch_size does
// not truncate the scan) is covered separately by
// crates/store-multi/tests/regression.rs.

use fixtures::{NODE_ID, deferred_txn, engine, ephemeral_txn, make_row, transactional_txn};
use reifydb_core::{
	interface::catalog::shape::ShapeId,
	key::{
		EncodableKey,
		row::{RowKey, RowKeyRange},
	},
};
use reifydb_sdk::operator::context::{OperatorContext, StoreApi};
use reifydb_sub_flow::{operator::context::native::NativeOperatorContext, transaction::FlowTransaction};
use reifydb_type::value::row_number::RowNumber;

#[path = "state/fixtures.rs"]
mod fixtures;

const TABLE: u64 = 4096;

// 1500 > the 1024 batch_size, so the scan must page past the first batch.
const ROW_COUNT: u64 = 1500;

fn assert_range_returns_all_rows(txn: &mut FlowTransaction) {
	let keys: Vec<_> = (1..=ROW_COUNT)
		.map(|n| {
			RowKey {
				shape: ShapeId::table(TABLE),
				row: RowNumber(n),
			}
			.encode()
		})
		.collect();
	let values: Vec<_> = (1..=ROW_COUNT).map(|n| make_row(&format!("row-{n}"), 0, 0)).collect();
	txn.set_batch(&keys, &values).unwrap();

	let range = RowKeyRange::scan_range(ShapeId::table(TABLE), None);
	let mut ctx = NativeOperatorContext::new(txn, NODE_ID);
	let rows = ctx.store().range(range.start.as_ref(), range.end.as_ref()).unwrap();

	assert_eq!(
		rows.len(),
		ROW_COUNT as usize,
		"NativeStore::range returned fewer than the {ROW_COUNT} rows in range - the 1024 batch_size truncated the scan"
	);
}

#[test]
fn deferred() {
	let e = engine();
	let mut txn = deferred_txn(&e);
	assert_range_returns_all_rows(&mut txn);
}

#[test]
fn transactional() {
	let e = engine();
	let mut txn = transactional_txn(&e);
	assert_range_returns_all_rows(&mut txn);
}

#[test]
fn ephemeral() {
	let e = engine();
	let mut txn = ephemeral_txn(&e);
	assert_range_returns_all_rows(&mut txn);
}
