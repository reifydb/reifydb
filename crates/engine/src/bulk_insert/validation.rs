// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Row validation and column mapping for bulk inserts with batch coercion.

use reifydb_core::{
	interface::catalog::{column::ColumnDef, ringbuffer::RingBufferDef, table::TableDef},
	value::column::data::ColumnData,
};
use reifydb_type::{fragment::Fragment, params::Params, value::Value};

use super::coerce::coerce_columns;
use crate::error::EngineError;

/// Validate and coerce all rows for a table in columnar batch mode.
///
/// Processes all rows at once by:
/// 1. Collecting params into columnar format
/// 2. Coercing each column's data in one batch using `cast_column_data`
/// 3. Extracting coerced values back to row format
///
/// Returns `Vec<Vec<Value>>` where outer vec is rows, inner is column values in table column order.
pub fn validate_and_coerce_rows(rows: &[Params], table: &TableDef) -> crate::Result<Vec<Vec<Value>>> {
	if rows.is_empty() {
		return Ok(Vec::new());
	}

	let num_cols = table.columns.len();
	let num_rows = rows.len();

	let column_data = collect_rows_to_columns(rows, &table.columns, &table.name)?;
	let coerced_columns = coerce_columns(&column_data, &table.columns, num_rows)?;

	Ok(columns_to_rows(&coerced_columns, num_rows, num_cols))
}

/// Validate and coerce all rows for a ring buffer in columnar batch mode.
pub fn validate_and_coerce_rows_rb(rows: &[Params], ringbuffer: &RingBufferDef) -> crate::Result<Vec<Vec<Value>>> {
	if rows.is_empty() {
		return Ok(Vec::new());
	}

	let num_cols = ringbuffer.columns.len();
	let num_rows = rows.len();

	let column_data = collect_rows_to_columns(rows, &ringbuffer.columns, &ringbuffer.name)?;
	let coerced_columns = coerce_columns(&column_data, &ringbuffer.columns, num_rows)?;

	Ok(columns_to_rows(&coerced_columns, num_rows, num_cols))
}

/// Reorder all rows for a table without coercion (trusted mode).
///
/// Used when validation is skipped for pre-validated internal data.
pub fn reorder_rows_trusted(rows: &[Params], table: &TableDef) -> crate::Result<Vec<Vec<Value>>> {
	if rows.is_empty() {
		return Ok(Vec::new());
	}

	let num_cols = table.columns.len();
	let num_rows = rows.len();

	// Build columnar data from params (no coercion)
	let column_data = collect_rows_to_columns(rows, &table.columns, &table.name)?;

	// Convert directly to row format without coercion
	Ok(columns_to_rows(&column_data, num_rows, num_cols))
}

/// Reorder all rows for a ring buffer without coercion (trusted mode).
pub fn reorder_rows_trusted_rb(rows: &[Params], ringbuffer: &RingBufferDef) -> crate::Result<Vec<Vec<Value>>> {
	if rows.is_empty() {
		return Ok(Vec::new());
	}

	let num_cols = ringbuffer.columns.len();
	let num_rows = rows.len();

	// Build columnar data from params (no coercion)
	let column_data = collect_rows_to_columns(rows, &ringbuffer.columns, &ringbuffer.name)?;

	// Convert directly to row format without coercion
	Ok(columns_to_rows(&column_data, num_rows, num_cols))
}

/// Collect rows (params) into columnar format.
///
/// Returns `Vec<ColumnData>` where each entry contains all values for that column.
fn collect_rows_to_columns(
	rows: &[Params],
	columns: &[ColumnDef],
	source_name: &str,
) -> crate::Result<Vec<ColumnData>> {
	let num_cols = columns.len();
	let mut column_data: Vec<ColumnData> =
		columns.iter().map(|col| ColumnData::none_typed(col.constraint.get_type(), 0)).collect();

	for params in rows {
		match params {
			Params::Named(map) => {
				// For each column, look up value in map or use Undefined
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
				for col_idx in 0..num_cols {
					let value = vals.get(col_idx).cloned().unwrap_or(Value::none());
					column_data[col_idx].push_value(value);
				}
			}
			Params::None => {
				for col_idx in 0..num_cols {
					column_data[col_idx].push_none();
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

/// Convert columnar data back to row format.
fn columns_to_rows(columns: &[ColumnData], num_rows: usize, num_cols: usize) -> Vec<Vec<Value>> {
	let mut result = Vec::with_capacity(num_rows);

	for row_idx in 0..num_rows {
		let mut row_values = Vec::with_capacity(num_cols);
		for col_idx in 0..num_cols {
			row_values.push(columns[col_idx].get_value(row_idx));
		}
		result.push(row_values);
	}

	result
}
