// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_row::{RowConverter, SortField};
use arrow_schema::FieldRef;
use reifydb_core::{
	internal_error,
	value::column::view::group_by::{cast_key, key_column},
};
use reifydb_value::value::{column_view::ColumnView, value_type::ValueType};

use crate::Result;

pub(crate) fn key_types(key_columns: &[(&FieldRef, &ArrayRef)]) -> Result<Vec<ValueType>> {
	key_columns
		.iter()
		.map(|(field, array)| Ok(ColumnView::try_from((*array, field.as_ref()))?.get_type()))
		.collect()
}

pub(crate) fn key_arrays(key_columns: &[(&FieldRef, &ArrayRef)], targets: &[ValueType]) -> Vec<ArrayRef> {
	key_columns
		.iter()
		.zip(targets)
		.map(|((_, array), target)| {
			let normalized = key_column(array);
			let (cast, _) = cast_key(&normalized, target);
			cast
		})
		.collect()
}

pub(crate) fn key_rows(
	key_columns: &[(&FieldRef, &ArrayRef)],
	targets: &[ValueType],
) -> Result<(RowConverter, Vec<ArrayRef>)> {
	let arrays = key_arrays(key_columns, targets);
	let fields = arrays.iter().map(|array| SortField::new(array.data_type().clone())).collect();
	let converter = RowConverter::new(fields).map_err(|e| internal_error!("Failed to build row keys: {}", e))?;
	Ok((converter, arrays))
}
