// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::value::column::{builder::ColumnBuilder, factory};
use reifydb_value::value::{column_view::ColumnView, value_type::ValueType};

fn extended(column: (FieldRef, ArrayRef), other: (FieldRef, ArrayRef)) -> String {
	let mut builder = ColumnBuilder::from_view(&ColumnView::try_from(&column).unwrap());
	match builder.extend(&ColumnView::try_from(&other).unwrap()) {
		Ok(()) => format!("ok {:?}", builder.get_type()),
		Err(err) => format!("error {}", err.diagnostic().message),
	}
}

#[test]
fn a_boolean_column_of_only_nones_is_not_retyped_by_an_int4_column() {
	// Otherwise real boolean nones silently turn into int4 nones, and only the untyped none column may retype.
	let bool_first = extended(factory::none_typed("c", ValueType::Boolean, 2), factory::int4("c", [1]));
	let int4_first = extended(factory::int4("c", [1, 2]), factory::none_typed("c", ValueType::Boolean, 2));

	assert!(bool_first.contains("column type mismatch"), "boolean first: {bool_first}");
	assert!(int4_first.contains("column type mismatch"), "int4 first: {int4_first}");
}
