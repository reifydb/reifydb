// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::iter;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_core::{interface::catalog::column::Column, internal_error, value::column::builder::ColumnBuilder};
use reifydb_value::{
	error::Error,
	fragment::Fragment,
	params::Params,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		identity::IdentityId,
		system_columns::{column_view, is_system_field},
		value_type::ValueType,
	},
};

use super::coerce::RowCoercer;
use crate::{Result, error::EngineError};

const TAG: &str = "tag";

struct Segment<'a> {
	columns: &'a [Column],
	tagged: bool,
	source_name: &'a str,
	coercer: &'a RowCoercer,
}

#[allow(clippy::type_complexity)]
pub fn coerce_columns(
	rows: &[Params],
	batches: &[(usize, RecordBatch)],
	columns: &[Column],
	tagged: bool,
	source_name: &str,
	identity: IdentityId,
) -> Result<(Vec<(FieldRef, ArrayRef)>, Option<(FieldRef, ArrayRef)>)> {
	let coercer = RowCoercer::new(identity);
	let segment = Segment {
		columns,
		tagged,
		source_name,
		coercer: &coercer,
	};
	let total = rows.len() + batches.iter().map(|(_, batch)| batch.num_rows()).sum::<usize>();
	let mut builders: Vec<ColumnBuilder> =
		columns.iter().map(|col| ColumnBuilder::with_capacity(col.constraint.get_type(), total)).collect();
	let mut tags = tagged.then(|| ColumnBuilder::with_capacity(ValueType::Any, total));
	let mut taken = 0;
	let mut first_row = 0;
	for (at, batch) in batches {
		first_row +=
			collect_rows_to_columns(&segment, &rows[taken..*at], first_row, &mut builders, tags.as_mut())?;
		taken = *at;
		first_row += coerce_batch(&segment, batch, first_row, &mut builders, tags.as_mut())?;
	}
	collect_rows_to_columns(&segment, &rows[taken..], first_row, &mut builders, tags.as_mut())?;
	let columns = builders.into_iter().zip(columns).map(|(builder, column)| builder.finish(&column.name)).collect();
	Ok((columns, tags.map(|builder| builder.finish(TAG))))
}

fn coerce_batch(
	segment: &Segment<'_>,
	batch: &RecordBatch,
	first_row: usize,
	builders: &mut [ColumnBuilder],
	tags: Option<&mut ColumnBuilder>,
) -> Result<usize> {
	let rows = batch.num_rows();
	if rows == 0 {
		return Ok(0);
	}

	let views = segment.columns.iter().map(|col| column_view(batch, &col.name)).collect::<Result<Vec<_>>>()?;
	let mut failure: Option<(usize, Error)> = None;
	for ((builder, column), view) in builders.iter_mut().zip(segment.columns).zip(&views) {
		let limit = failure.as_ref().map_or(rows, |(row, _)| *row);
		let Some(view) = view else {
			for _ in 0..rows {
				builder.push_none();
			}
			continue;
		};
		let per_cell =
			matches!(view.data, ViewData::Any { .. } | ViewData::Digest { .. } | ViewData::None { .. });
		if per_cell {
			if let Some(found) = coerce_cells(segment, column, view, first_row, limit, builder) {
				failure = Some(found);
			}
			continue;
		}
		if view.base_type() == *column.constraint.get_type().inner_type() {
			builder.append_values(view)?;
			continue;
		}
		match segment.coercer.cast_column(view, column) {
			Ok(cast) => builder.append_values(&ColumnView::try_from(&cast)?)?,
			Err(_) => match coerce_cells(segment, column, view, first_row, limit, builder) {
				Some(found) => failure = Some(found),
				None if failure.is_some() => {}
				None => {
					return Err(internal_error!(
						"bulk column {} failed its column cast where every cell cast passes",
						column.name
					));
				}
			},
		}
	}
	if let Some((_, error)) = failure {
		return Err(error);
	}

	if let Some(field) = batch.schema().fields().iter().find(|field| {
		is_system_field(field)
			|| !(segment.columns.iter().any(|c| &c.name == field.name())
				|| (segment.tagged && field.name() == TAG))
	}) {
		return Err(EngineError::BulkInsertColumnNotFound {
			fragment: Fragment::None,
			table_name: segment.source_name.to_string(),
			column: field.name().to_string(),
		}
		.into());
	}

	if let Some(tags) = tags {
		match column_view(batch, TAG)? {
			Some(view) => {
				for row in 0..rows {
					push_tag(tags, view.get_value(row));
				}
			}
			None => {
				for _ in 0..rows {
					tags.push_none();
				}
			}
		}
	}
	Ok(rows)
}

fn coerce_cells(
	segment: &Segment<'_>,
	column: &Column,
	view: &ColumnView<'_>,
	first_row: usize,
	limit: usize,
	builder: &mut ColumnBuilder,
) -> Option<(usize, Error)> {
	for row in 0..limit {
		match segment.coercer.coerce(view.get_value(row), column, segment.source_name, first_row + row) {
			Ok(value) => builder.push_value(value),
			Err(error) => return Some((row, error)),
		}
	}
	None
}

fn push_tag(tags: &mut ColumnBuilder, value: Value) {
	match value {
		Value::None {
			..
		} => tags.push_none(),
		value => tags.push_value(Value::Any(Box::new(value))),
	}
}

fn collect_rows_to_columns(
	segment: &Segment<'_>,
	rows: &[Params],
	first_row: usize,
	builders: &mut [ColumnBuilder],
	mut tags: Option<&mut ColumnBuilder>,
) -> Result<usize> {
	let columns = segment.columns;
	let source_name = segment.source_name;
	let coercer = segment.coercer;
	let num_cols = columns.len();

	for (row_idx, params) in rows.iter().enumerate() {
		match params {
			Params::Named(map) => {
				for (col_idx, col) in columns.iter().enumerate() {
					let value = map.get(&col.name).cloned().unwrap_or(Value::none());
					builders[col_idx].push_value(coercer.coerce(
						value,
						col,
						source_name,
						first_row + row_idx,
					)?);
				}
				if let Some(tags) = tags.as_deref_mut() {
					push_tag(tags, map.get(TAG).cloned().unwrap_or(Value::none()));
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
				for ((col_data, col), val) in builders
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
				if let Some(tags) = tags.as_deref_mut() {
					tags.push_none();
				}
			}
			Params::None => {
				for col_data in builders.iter_mut() {
					col_data.push_none();
				}
				if let Some(tags) = tags.as_deref_mut() {
					tags.push_none();
				}
			}
		}
	}

	for params in rows {
		if let Params::Named(map) = params {
			for name in map.keys() {
				if !(columns.iter().any(|c| &c.name == name) || (segment.tagged && name == TAG)) {
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

	Ok(rows.len())
}
