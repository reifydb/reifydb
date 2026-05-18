// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::iter;

use reifydb_core::{
	interface::catalog::{column::Column, ringbuffer::RingBuffer, series::Series, table::Table},
	value::column::buffer::ColumnBuffer,
};
use reifydb_type::{fragment::Fragment, params::Params, value::Value};

use super::coerce::coerce_columns;
use crate::{Result, error::EngineError};

pub fn validate_and_coerce_rows(rows: &[Params], table: &Table) -> Result<Vec<Vec<Value>>> {
	if rows.is_empty() {
		return Ok(Vec::new());
	}

	let num_cols = table.columns.len();
	let num_rows = rows.len();

	let column_data = collect_rows_to_columns(rows, &table.columns, &table.name)?;
	let coerced_columns = coerce_columns(&column_data, &table.columns, num_rows)?;

	Ok(columns_to_rows(&coerced_columns, num_rows, num_cols))
}

pub fn validate_and_coerce_rows_rb(rows: &[Params], ringbuffer: &RingBuffer) -> Result<Vec<Vec<Value>>> {
	if rows.is_empty() {
		return Ok(Vec::new());
	}

	let num_cols = ringbuffer.columns.len();
	let num_rows = rows.len();

	let column_data = collect_rows_to_columns(rows, &ringbuffer.columns, &ringbuffer.name)?;
	let coerced_columns = coerce_columns(&column_data, &ringbuffer.columns, num_rows)?;

	Ok(columns_to_rows(&coerced_columns, num_rows, num_cols))
}

pub fn reorder_rows_unvalidated(rows: &[Params], table: &Table) -> Result<Vec<Vec<Value>>> {
	if rows.is_empty() {
		return Ok(Vec::new());
	}

	let num_cols = table.columns.len();
	let num_rows = rows.len();

	let column_data = collect_rows_to_columns(rows, &table.columns, &table.name)?;

	Ok(columns_to_rows(&column_data, num_rows, num_cols))
}

pub fn reorder_rows_unvalidated_rb(rows: &[Params], ringbuffer: &RingBuffer) -> Result<Vec<Vec<Value>>> {
	if rows.is_empty() {
		return Ok(Vec::new());
	}

	let num_cols = ringbuffer.columns.len();
	let num_rows = rows.len();

	let column_data = collect_rows_to_columns(rows, &ringbuffer.columns, &ringbuffer.name)?;

	Ok(columns_to_rows(&column_data, num_rows, num_cols))
}

pub fn validate_and_coerce_rows_series(rows: &[Params], series: &Series) -> Result<Vec<Vec<Value>>> {
	if rows.is_empty() {
		return Ok(Vec::new());
	}

	let num_cols = series.columns.len();
	let num_rows = rows.len();

	let column_data = collect_rows_to_columns(rows, &series.columns, &series.name)?;
	let coerced_columns = coerce_columns(&column_data, &series.columns, num_rows)?;

	Ok(columns_to_rows(&coerced_columns, num_rows, num_cols))
}

pub fn reorder_rows_unvalidated_series(rows: &[Params], series: &Series) -> Result<Vec<Vec<Value>>> {
	if rows.is_empty() {
		return Ok(Vec::new());
	}

	let num_cols = series.columns.len();
	let num_rows = rows.len();

	let column_data = collect_rows_to_columns(rows, &series.columns, &series.name)?;

	Ok(columns_to_rows(&column_data, num_rows, num_cols))
}

fn collect_rows_to_columns(rows: &[Params], columns: &[Column], source_name: &str) -> Result<Vec<ColumnBuffer>> {
	let num_cols = columns.len();
	let mut column_data: Vec<ColumnBuffer> =
		columns.iter().map(|col| ColumnBuffer::none_typed(col.constraint.get_type(), 0)).collect();

	for params in rows {
		match params {
			Params::Named(map) => {
				for (col_idx, col) in columns.iter().enumerate() {
					let value = map.get(&col.name).cloned().unwrap_or(Value::none());
					column_data[col_idx].push_value(value);
				}
			}
			Params::Positional(vals) => {
				if vals.len() > num_cols {
					return Err(EngineError::BulkInsertTooManyValues {
						fragment: Fragment::None,
						expected: num_cols,
						actual: vals.len(),
					}
					.into());
				}
				for (col_data, val) in
					column_data.iter_mut().zip(vals.iter().map(Some).chain(iter::repeat(None)))
				{
					col_data.push_value(val.cloned().unwrap_or(Value::none()));
				}
			}
			Params::None => {
				for col_data in column_data.iter_mut() {
					col_data.push_none();
				}
			}
		}
	}

	for params in rows {
		if let Params::Named(map) = params {
			for name in map.keys() {
				if !columns.iter().any(|c| &c.name == name) {
					return Err(EngineError::BulkInsertColumnNotFound {
						fragment: Fragment::None,
						table_name: source_name.to_string(),
						column: name.to_string(),
					}
					.into());
				}
			}
		}
	}

	Ok(column_data)
}

fn columns_to_rows(columns: &[ColumnBuffer], num_rows: usize, num_cols: usize) -> Vec<Vec<Value>> {
	let mut result = Vec::with_capacity(num_rows);

	for row_idx in 0..num_rows {
		let mut row_values = Vec::with_capacity(num_cols);
		for col in columns.iter().take(num_cols) {
			row_values.push(col.get_value(row_idx));
		}
		result.push(row_values);
	}

	result
}
