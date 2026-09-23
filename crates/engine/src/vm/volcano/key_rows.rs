// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_row::{RowConverter, SortField};
use reifydb_core::{
	internal_error,
	value::column::{
		buffer::ColumnBuffer,
		view::group_by::{cast_key, key_column},
	},
};
use reifydb_value::value::value_type::ValueType;

use crate::Result;

pub(crate) fn key_types(key_columns: &[&ColumnBuffer]) -> Vec<ValueType> {
	key_columns.iter().map(|column| column.get_type()).collect()
}

pub(crate) fn key_arrays(key_columns: &[&ColumnBuffer], targets: &[ValueType]) -> Vec<ArrayRef> {
	key_columns
		.iter()
		.zip(targets)
		.map(|(column, target)| {
			let normalized = key_column(column);
			let (cast, _) = cast_key(&normalized, target);
			cast.to_array_ref()
		})
		.collect()
}

pub(crate) fn key_rows(key_columns: &[&ColumnBuffer], targets: &[ValueType]) -> Result<(RowConverter, Vec<ArrayRef>)> {
	let arrays = key_arrays(key_columns, targets);
	let fields = arrays.iter().map(|array| SortField::new(array.data_type().clone())).collect();
	let converter = RowConverter::new(fields).map_err(|e| internal_error!("Failed to build row keys: {}", e))?;
	Ok((converter, arrays))
}
