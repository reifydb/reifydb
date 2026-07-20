// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::flow::diff::DiffType;
use reifydb_codec::{
	encoded::{
		row::{EncodedRow, SHAPE_HEADER_SIZE},
		shape::{RowShape, RowShapeField},
	},
	key::encoded::EncodedKey,
};
use reifydb_core::{
	interface::{catalog::shape::ShapeId, change::Change},
	key::{EncodableKey, row::RowKey},
	row::Row,
};
use reifydb_sdk::testing::builders::TestChangeBuilder;
use reifydb_value::{
	util::cowvec::CowVec,
	value::{Value, row_number::RowNumber, value_type::ValueType},
};

pub const STORE_TABLE: u64 = 4096;

pub const STORE_ROW_COUNT: u64 = 1500;

pub fn ts_row(row_number: u64, timestamp: i64) -> Row {
	let shape = RowShape::new(vec![RowShapeField::unconstrained("timestamp", ValueType::Int8)]);
	let mut encoded = shape.allocate();
	shape.set_values(&mut encoded, &[Value::Int8(timestamp)]);
	Row {
		number: RowNumber(row_number),
		encoded,
		shape,
	}
}

pub fn window_change(row_number: u64, timestamp: i64) -> Change {
	TestChangeBuilder::new().insert(ts_row(row_number, timestamp)).build()
}

pub fn trigger() -> Change {
	TestChangeBuilder::new().insert_row(1u64, vec![Value::Int8(0)]).build()
}

fn store_value(payload: &str) -> EncodedRow {
	let mut buf = vec![0u8; SHAPE_HEADER_SIZE + payload.len()];
	buf[SHAPE_HEADER_SIZE..].copy_from_slice(payload.as_bytes());
	EncodedRow(CowVec::new(buf))
}

pub fn store_seed() -> Vec<(EncodedKey, EncodedRow)> {
	(1..=STORE_ROW_COUNT)
		.map(|n| {
			let key = RowKey {
				shape: ShapeId::table(STORE_TABLE),
				row: RowNumber(n),
			}
			.encode();
			(key, store_value(&format!("row-{n}")))
		})
		.collect()
}

pub fn row_ints(change: &Change) -> Vec<i64> {
	let cols = change.diffs[0].post().expect("emitted diff has post columns");
	assert_eq!(cols.row_count(), 1, "expected exactly one emitted row");
	cols.row(0)
		.into_iter()
		.map(|v| match v {
			Value::Int8(n) => n,
			other => panic!("expected Int8 emitted value, got {other:?}"),
		})
		.collect()
}

pub fn diff_kind(change: &Change) -> DiffType {
	change.diffs[0].kind()
}
