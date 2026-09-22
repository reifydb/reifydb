// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::iter;

use reifydb_core::{
	interface::catalog::column::Column,
	value::column::{buffer::ColumnBuffer, builder::ColumnBuilder},
};
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	value::{Value, identity::IdentityId},
};

use super::coerce::RowCoercer;
use crate::{Result, error::EngineError};

pub fn coerce_rows(
	rows: &[Params],
	columns: &[Column],
	source_name: &str,
	identity: IdentityId,
) -> Result<Vec<Vec<Value>>> {
	if rows.is_empty() {
		return Ok(Vec::new());
	}

	let column_data = collect_rows_to_columns(rows, columns, source_name, &RowCoercer::new(identity))?;

	Ok(columns_to_rows(&column_data, rows.len(), columns.len()))
}

fn collect_rows_to_columns(
	rows: &[Params],
	columns: &[Column],
	source_name: &str,
	coercer: &RowCoercer,
) -> Result<Vec<ColumnBuffer>> {
	let num_cols = columns.len();
	let mut column_data: Vec<ColumnBuilder> = columns
		.iter()
		.map(|col| ColumnBuffer::none_typed(col.constraint.get_type(), 0).into_builder())
		.collect();

	for (row_idx, params) in rows.iter().enumerate() {
		match params {
			Params::Named(map) => {
				for (col_idx, col) in columns.iter().enumerate() {
					let value = map.get(&col.name).cloned().unwrap_or(Value::none());
					column_data[col_idx].push_value(coercer.coerce(
						value,
						col,
						source_name,
						row_idx,
					)?);
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
				for ((col_data, col), val) in column_data
					.iter_mut()
					.zip(columns.iter())
					.zip(vals.iter().map(Some).chain(iter::repeat(None)))
				{
					let value = val.cloned().unwrap_or(Value::none());
					col_data.push_value(coercer.coerce(value, col, source_name, row_idx)?);
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

	Ok(column_data.into_iter().map(ColumnBuilder::finish).collect())
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
