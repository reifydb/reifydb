// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::iter;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_core::{interface::catalog::column::Column, value::column::builder::ColumnBuilder};
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	value::{
		Value,
		column_view::ColumnView,
		identity::IdentityId,
		system_columns::{column_view, is_system_field},
	},
};

use super::coerce::RowCoercer;
use crate::{Result, error::EngineError};

pub fn coerce_rows(
	rows: &[Params],
	batches: &[(usize, RecordBatch)],
	columns: &[Column],
	source_name: &str,
	identity: IdentityId,
) -> Result<Vec<Vec<Value>>> {
	let coercer = RowCoercer::new(identity);
	let mut coerced = Vec::new();
	let mut taken = 0;
	for (at, batch) in batches {
		coerced.extend(coerce_params(&rows[taken..*at], columns, source_name, &coercer, coerced.len())?);
		taken = *at;
		coerced.extend(coerce_batch(batch, columns, source_name, &coercer, coerced.len())?);
	}
	coerced.extend(coerce_params(&rows[taken..], columns, source_name, &coercer, coerced.len())?);
	Ok(coerced)
}

fn coerce_params(
	rows: &[Params],
	columns: &[Column],
	source_name: &str,
	coercer: &RowCoercer,
	first_row: usize,
) -> Result<Vec<Vec<Value>>> {
	if rows.is_empty() {
		return Ok(Vec::new());
	}

	let column_data = collect_rows_to_columns(rows, columns, source_name, coercer, first_row)?;

	columns_to_rows(&column_data, rows.len(), columns.len())
}

fn coerce_batch(
	batch: &RecordBatch,
	columns: &[Column],
	source_name: &str,
	coercer: &RowCoercer,
	first_row: usize,
) -> Result<Vec<Vec<Value>>> {
	if batch.num_rows() == 0 {
		return Ok(Vec::new());
	}

	let views = columns.iter().map(|col| column_view(batch, &col.name)).collect::<Result<Vec<_>>>()?;
	let mut column_data: Vec<ColumnBuilder> = columns
		.iter()
		.map(|col| ColumnBuilder::with_capacity(col.constraint.get_type(), batch.num_rows()))
		.collect();

	for row_idx in 0..batch.num_rows() {
		for ((col_data, col), view) in column_data.iter_mut().zip(columns).zip(&views) {
			let value = view.as_ref().map_or_else(Value::none, |view| view.get_value(row_idx));
			col_data.push_value(coercer.coerce(value, col, source_name, first_row + row_idx)?);
		}
	}

	if let Some(field) = batch
		.schema()
		.fields()
		.iter()
		.find(|field| is_system_field(field) || !columns.iter().any(|c| &c.name == field.name()))
	{
		return Err(EngineError::BulkInsertColumnNotFound {
			fragment: Fragment::None,
			table_name: source_name.to_string(),
			column: field.name().to_string(),
		}
		.into());
	}

	let column_data: Vec<(FieldRef, ArrayRef)> =
		column_data.into_iter().zip(columns).map(|(builder, column)| builder.finish(&column.name)).collect();
	columns_to_rows(&column_data, batch.num_rows(), columns.len())
}

fn collect_rows_to_columns(
	rows: &[Params],
	columns: &[Column],
	source_name: &str,
	coercer: &RowCoercer,
	first_row: usize,
) -> Result<Vec<(FieldRef, ArrayRef)>> {
	let num_cols = columns.len();
	let mut column_data: Vec<ColumnBuilder> =
		columns.iter().map(|col| ColumnBuilder::with_capacity(col.constraint.get_type(), rows.len())).collect();

	for (row_idx, params) in rows.iter().enumerate() {
		match params {
			Params::Named(map) => {
				for (col_idx, col) in columns.iter().enumerate() {
					let value = map.get(&col.name).cloned().unwrap_or(Value::none());
					column_data[col_idx].push_value(coercer.coerce(
						value,
						col,
						source_name,
						first_row + row_idx,
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
					col_data.push_value(coercer.coerce(
						value,
						col,
						source_name,
						first_row + row_idx,
					)?);
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

	Ok(column_data.into_iter().zip(columns).map(|(builder, column)| builder.finish(&column.name)).collect())
}

fn columns_to_rows(columns: &[(FieldRef, ArrayRef)], num_rows: usize, num_cols: usize) -> Result<Vec<Vec<Value>>> {
	let views = columns.iter().take(num_cols).map(ColumnView::try_from).collect::<Result<Vec<_>>>()?;
	let mut result = Vec::with_capacity(num_rows);

	for row_idx in 0..num_rows {
		let mut row_values = Vec::with_capacity(num_cols);
		for col in views.iter() {
			row_values.push(col.get_value(row_idx));
		}
		result.push(row_values);
	}

	Ok(result)
}
