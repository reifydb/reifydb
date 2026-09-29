// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{iter::repeat_n, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{Field, FieldRef, Schema, SchemaRef};
use reifydb_core::value::{
	batch::{batch, take_rows},
	column::{
		builder::ColumnBuilder,
		factory::{none, rename},
	},
};
use reifydb_value::{
	Result, reifydb_assertions,
	value::{
		Value,
		datetime::DateTime,
		row_number::RowNumber,
		system_columns::{SystemColumn, is_system_field, system_column, user_columns, with_system_column},
		value_type::field::from_field,
	},
};

use crate::operator::{row_times, time_column};

pub(crate) struct JoinedColumnsBuilder {
	right_column_names: Vec<String>,

	included_right_cols: Vec<usize>,
}

impl JoinedColumnsBuilder {
	pub(crate) fn new(left: &Schema, right: &Schema, alias: &Option<String>, natural: bool) -> Self {
		let left_names: Vec<String> = left
			.fields()
			.iter()
			.filter(|field| !is_system_field(field))
			.map(|field| field.name().clone())
			.collect();

		let alias_str = alias.as_deref().unwrap_or("other");
		let mut right_column_names = Vec::with_capacity(right.fields().len());
		let mut included_right_cols = Vec::with_capacity(right.fields().len());
		let mut all_names = left_names.clone();

		for (idx, field) in right.fields().iter().enumerate() {
			if is_system_field(field) {
				continue;
			}
			let col_name = field.name().as_str();

			if natural && left_names.iter().any(|ln| ln.as_str() == col_name) {
				continue;
			}

			let prefixed_name = format!("{}_{}", alias_str, col_name);

			let mut final_name = prefixed_name.clone();
			if all_names.contains(&final_name) {
				let mut counter = 2;
				loop {
					let candidate = format!("{}_{}", prefixed_name, counter);
					if !all_names.contains(&candidate) {
						final_name = candidate;
						break;
					}
					counter += 1;
				}
			}

			all_names.push(final_name.clone());
			right_column_names.push(final_name);
			included_right_cols.push(idx);
		}

		Self {
			right_column_names,
			included_right_cols,
		}
	}

	pub(crate) fn schema(&self, left: &Schema, right: &Schema) -> SchemaRef {
		let mut fields: Vec<FieldRef> =
			left.fields().iter().filter(|field| !is_system_field(field)).cloned().collect();
		fields.extend(self
			.included_right_cols
			.iter()
			.zip(self.right_column_names.iter())
			.map(|(&idx, name)| Arc::new(right.field(idx).clone().with_name(name))));
		Arc::new(Schema::new(fields))
	}

	pub(crate) fn join_one_to_many(
		&self,
		row_numbers: &[RowNumber],
		left: &RecordBatch,
		left_idx: usize,
		right: &RecordBatch,
	) -> Result<RecordBatch> {
		let right_count = right.num_rows();
		reifydb_assertions! {
			assert_eq!(row_numbers.len(), right_count, "row_numbers must match right row count");
		}

		let left_rows = vec![left_idx; right_count];
		Self::assemble(row_numbers, left, &left_rows, self.right_columns(right), row_times(right)?)
	}

	pub(crate) fn join_many_to_one(
		&self,
		row_numbers: &[RowNumber],
		left: &RecordBatch,
		right: &RecordBatch,
		right_idx: usize,
	) -> Result<RecordBatch> {
		let left_count = left.num_rows();
		reifydb_assertions! {
			assert_eq!(row_numbers.len(), left_count, "row_numbers must match left row count");
		}

		let left_rows: Vec<usize> = (0..left_count).collect();
		let right_rows = vec![right_idx; left_count];
		let right_time = row_times(right)?.get(right_idx).copied().flatten();
		let right_columns = self.right_columns(&take_rows(right, &right_rows)?);
		Self::assemble(row_numbers, left, &left_rows, right_columns, vec![right_time; left_count])
	}

	pub(crate) fn retain_rows(columns: &RecordBatch, keep: &[usize]) -> Result<RecordBatch> {
		if keep.len() == columns.num_rows() {
			return Ok(columns.clone());
		}
		take_rows(columns, keep)
	}

	pub(crate) fn join_cartesian(
		&self,
		row_numbers: &[RowNumber],
		left: &RecordBatch,
		left_indices: &[usize],
		right: &RecordBatch,
		right_indices: &[usize],
	) -> Result<RecordBatch> {
		let right_count = right_indices.len();
		reifydb_assertions! {
			assert_eq!(
				row_numbers.len(),
				left_indices.len() * right_count,
				"row_numbers must match cartesian product size"
			);
		}

		let left_rows: Vec<usize> =
			left_indices.iter().flat_map(|&left_idx| repeat_n(left_idx, right_count)).collect();
		let right_rows: Vec<usize> = left_indices.iter().flat_map(|_| right_indices.iter().copied()).collect();
		let right_times = row_times(right)?;
		let times = right_rows.iter().map(|&right_idx| right_times.get(right_idx).copied().flatten()).collect();
		let right_columns = self.right_columns(&take_rows(right, &right_rows)?);
		Self::assemble(row_numbers, left, &left_rows, right_columns, times)
	}

	pub(crate) fn unmatched_left(
		&self,
		row_number: RowNumber,
		left: &RecordBatch,
		left_idx: usize,
		right_shape: &Schema,
	) -> Result<RecordBatch> {
		Self::assemble(&[row_number], left, &[left_idx], self.unmatched_right(right_shape, 1)?, Vec::new())
	}

	pub(crate) fn unmatched_left_batch(
		&self,
		row_numbers: &[RowNumber],
		left: &RecordBatch,
		left_indices: &[usize],
		right_shape: &Schema,
	) -> Result<RecordBatch> {
		let count = left_indices.len();
		reifydb_assertions! {
			assert_eq!(row_numbers.len(), count, "row_numbers must match indices count");
		}

		let right_columns = self.unmatched_right(right_shape, count)?;
		Self::assemble(row_numbers, left, left_indices, right_columns, Vec::new())
	}

	fn right_columns(&self, right: &RecordBatch) -> Vec<(FieldRef, ArrayRef)> {
		self.included_right_cols
			.iter()
			.zip(self.right_column_names.iter())
			.map(|(&idx, name)| {
				rename((right.schema_ref().fields()[idx].clone(), right.column(idx).clone()), name)
			})
			.collect()
	}

	fn unmatched_right(&self, right_shape: &Schema, count: usize) -> Result<Vec<(FieldRef, ArrayRef)>> {
		self.included_right_cols
			.iter()
			.zip(self.right_column_names.iter())
			.map(|(&idx, name)| none_column(right_shape.field(idx), name, count))
			.collect()
	}

	fn assemble(
		row_numbers: &[RowNumber],
		left: &RecordBatch,
		left_rows: &[usize],
		right_columns: Vec<(FieldRef, ArrayRef)>,
		right_times: Vec<Option<DateTime>>,
	) -> Result<RecordBatch> {
		let gathered = take_rows(left, left_rows)?;
		let mut columns: Vec<(FieldRef, ArrayRef)> =
			user_columns(&gathered).map(|(field, array)| (field.clone(), array.clone())).collect();
		columns.extend(right_columns);
		let mut joined = batch(columns)?;
		for column in SystemColumn::ALL {
			let array = match column {
				SystemColumn::RowNumbers => Some(Arc::new(UInt64Array::from_iter_values(
					row_numbers.iter().map(|number| number.value()),
				)) as ArrayRef),
				SystemColumn::Time => Self::joined_time(&gathered, &right_times)?,
				other => system_column(&gathered, other).cloned(),
			};
			if let Some(array) = array {
				joined = with_system_column(joined, column, array)?;
			}
		}
		Ok(joined)
	}

	fn joined_time(left: &RecordBatch, right_times: &[Option<DateTime>]) -> Result<Option<ArrayRef>> {
		let left_times = row_times(left)?;
		if left_times.is_empty() {
			return Ok(None);
		}
		Ok(Some(time_column(left_times.into_iter().enumerate().map(|(row, own)| {
			match (own, right_times.get(row).copied().flatten()) {
				(Some(own), Some(right)) => Some(own.max(right)),
				(own, right) => own.or(right),
			}
		}))))
	}
}

fn none_column(field: &Field, name: &str, count: usize) -> Result<(FieldRef, ArrayRef)> {
	let Some(value_type) = from_field(field)?.value_type else {
		return Ok(none(name, count));
	};
	let mut builder = ColumnBuilder::with_capacity(value_type, count);
	for _ in 0..count {
		builder.push_value(Value::none());
	}
	Ok(builder.finish(name))
}
