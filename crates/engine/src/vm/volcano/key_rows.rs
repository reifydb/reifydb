// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_row::{RowConverter, SortField};
use reifydb_core::{
	internal_error,
	value::column::{buffer::ColumnBuffer, view::group_by::key_column},
};

use crate::Result;

pub(crate) fn key_arrays(key_columns: &[&ColumnBuffer]) -> Vec<ArrayRef> {
	key_columns.iter().map(|column| key_column(column).to_array_ref()).collect()
}

pub(crate) fn key_rows(key_columns: &[&ColumnBuffer]) -> Result<(RowConverter, Vec<ArrayRef>)> {
	let arrays = key_arrays(key_columns);
	let fields = arrays.iter().map(|array| SortField::new(array.data_type().clone())).collect();
	let converter = RowConverter::new(fields).map_err(|e| internal_error!("Failed to build row keys: {}", e))?;
	Ok((converter, arrays))
}
