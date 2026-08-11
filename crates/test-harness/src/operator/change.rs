// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::RowShapeField;
use reifydb_core::{
	interface::change::{Change, DiffType},
	row::Row,
};
use reifydb_testing_sdk::builders::{TestChangeBuilder, TestOperatorRowBuilder};
use reifydb_value::value::{Value, row_number::RowNumber, value_type::ValueType};

pub fn ts_row(row_number: u64, timestamp: i64) -> Row {
	TestOperatorRowBuilder::new(RowNumber(row_number))
		.with_fields(vec![RowShapeField::unconstrained("timestamp", ValueType::Int8)])
		.with_values(vec![Value::Int8(timestamp)])
		.build()
}

pub fn window_change(row_number: u64, timestamp: i64) -> Change {
	TestChangeBuilder::new().insert(ts_row(row_number, timestamp)).build()
}

pub fn trigger() -> Change {
	TestChangeBuilder::new().insert_row(1u64, vec![Value::Int8(0)]).build()
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
