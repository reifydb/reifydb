// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::{
	fragment::Fragment,
	util::cowvec::CowVec,
	value::{Value, datetime::DateTime, row_number::RowNumber},
};

pub(crate) struct JoinedColumnsBuilder {
	left_column_count: usize,

	right_column_names: Vec<String>,
}

impl JoinedColumnsBuilder {
	pub(crate) fn new(left: &Columns, right: &Columns, alias: &Option<String>) -> Self {
		let left_column_count = left.columns.len();

		let left_names: Vec<String> = left.names.iter().map(|n| n.as_ref().to_string()).collect();

		let alias_str = alias.as_deref().unwrap_or("other");
		let mut right_column_names = Vec::with_capacity(right.columns.len());
		let mut all_names = left_names.clone();

		for name in right.names.iter() {
			let col_name = name.as_ref();
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
		}

		Self {
			left_column_count,
			right_column_names,
		}
	}

	pub(crate) fn join_single(&self, row_number: RowNumber, left: &Columns, right: &Columns) -> Columns {
		debug_assert_eq!(left.row_count(), 1, "left must have exactly 1 row");
		debug_assert_eq!(right.row_count(), 1, "right must have exactly 1 row");

		self.join_one_to_many(&[row_number], left, 0, right)
	}

	pub(crate) fn join_at_indices(
		&self,
		row_number: RowNumber,
		left: &Columns,
		left_idx: usize,
		right: &Columns,
		right_idx: usize,
	) -> Columns {
		let total_columns = self.left_column_count + self.right_column_names.len();
		let mut result_columns = Vec::with_capacity(total_columns);

		for (i, left_col) in left.columns.iter().enumerate() {
			let mut col_data = ColumnBuffer::with_capacity(left_col.get_type(), 1);
			col_data.push_value(left_col.get_value(left_idx));
			result_columns.push(ColumnWithName::new(left.names[i].clone(), col_data));
		}

		for (right_col, aliased_name) in right.columns.iter().zip(self.right_column_names.iter()) {
			let mut col_data = ColumnBuffer::with_capacity(right_col.get_type(), 1);
			col_data.push_value(right_col.get_value(right_idx));
			result_columns.push(ColumnWithName::new(Fragment::internal(aliased_name), col_data));
		}

		Columns::with_system_columns(
			result_columns,
			vec![row_number],
			Self::extract_single_timestamp(&left.created_at, left_idx),
			Self::extract_single_timestamp(&left.updated_at, left_idx),
		)
	}

	pub(crate) fn join_one_to_many(
		&self,
		row_numbers: &[RowNumber],
		left: &Columns,
		left_idx: usize,
		right: &Columns,
	) -> Columns {
		let right_count = right.row_count();
		debug_assert_eq!(row_numbers.len(), right_count, "row_numbers must match right row count");

		let total_columns = self.left_column_count + self.right_column_names.len();
		let mut result_columns = Vec::with_capacity(total_columns);

		for (i, left_col) in left.columns.iter().enumerate() {
			let left_value = left_col.get_value(left_idx);
			let mut col_data = ColumnBuffer::with_capacity(left_col.get_type(), right_count);
			for _ in 0..right_count {
				col_data.push_value(left_value.clone());
			}
			result_columns.push(ColumnWithName::new(left.names[i].clone(), col_data));
		}

		for (right_col, aliased_name) in right.columns.iter().zip(self.right_column_names.iter()) {
			let mut col_data = ColumnBuffer::with_capacity(right_col.get_type(), right_count);
			for row_idx in 0..right_count {
				col_data.push_value(right_col.get_value(row_idx));
			}
			result_columns.push(ColumnWithName::new(Fragment::internal(aliased_name), col_data));
		}

		Columns::with_system_columns(
			result_columns,
			row_numbers.to_vec(),
			Self::duplicate_timestamp(&left.created_at, left_idx, right_count),
			Self::duplicate_timestamp(&left.updated_at, left_idx, right_count),
		)
	}

	pub(crate) fn join_many_to_one(
		&self,
		row_numbers: &[RowNumber],
		left: &Columns,
		right: &Columns,
		right_idx: usize,
	) -> Columns {
		let left_count = left.row_count();
		debug_assert_eq!(row_numbers.len(), left_count, "row_numbers must match left row count");

		let total_columns = self.left_column_count + self.right_column_names.len();
		let mut result_columns = Vec::with_capacity(total_columns);

		for (i, left_col) in left.columns.iter().enumerate() {
			let mut col_data = ColumnBuffer::with_capacity(left_col.get_type(), left_count);
			for row_idx in 0..left_count {
				col_data.push_value(left_col.get_value(row_idx));
			}
			result_columns.push(ColumnWithName::new(left.names[i].clone(), col_data));
		}

		for (right_col, aliased_name) in right.columns.iter().zip(self.right_column_names.iter()) {
			let right_value = right_col.get_value(right_idx);
			let mut col_data = ColumnBuffer::with_capacity(right_col.get_type(), left_count);
			for _ in 0..left_count {
				col_data.push_value(right_value.clone());
			}
			result_columns.push(ColumnWithName::new(Fragment::internal(aliased_name), col_data));
		}

		Columns::with_system_columns(
			result_columns,
			row_numbers.to_vec(),
			left.created_at.as_ref().to_vec(),
			left.updated_at.as_ref().to_vec(),
		)
	}

	pub(crate) fn join_cartesian(
		&self,
		row_numbers: &[RowNumber],
		left: &Columns,
		left_indices: &[usize],
		right: &Columns,
		right_indices: &[usize],
	) -> Columns {
		let left_count = left_indices.len();
		let right_count = right_indices.len();
		let result_count = left_count * right_count;
		debug_assert_eq!(row_numbers.len(), result_count, "row_numbers must match cartesian product size");

		let total_columns = self.left_column_count + self.right_column_names.len();
		let mut result_columns = Vec::with_capacity(total_columns);

		for (i, left_col) in left.columns.iter().enumerate() {
			let mut col_data = ColumnBuffer::with_capacity(left_col.get_type(), result_count);
			for &left_idx in left_indices {
				let left_value = left_col.get_value(left_idx);
				for _ in 0..right_count {
					col_data.push_value(left_value.clone());
				}
			}
			result_columns.push(ColumnWithName::new(left.names[i].clone(), col_data));
		}

		for (right_col, aliased_name) in right.columns.iter().zip(self.right_column_names.iter()) {
			let mut col_data = ColumnBuffer::with_capacity(right_col.get_type(), result_count);
			for _ in 0..left_count {
				for &right_idx in right_indices {
					col_data.push_value(right_col.get_value(right_idx));
				}
			}
			result_columns.push(ColumnWithName::new(Fragment::internal(aliased_name), col_data));
		}

		Columns::with_system_columns(
			result_columns,
			row_numbers.to_vec(),
			Self::expand_timestamps_cartesian(&left.created_at, left_indices, right_count),
			Self::expand_timestamps_cartesian(&left.updated_at, left_indices, right_count),
		)
	}

	pub(crate) fn unmatched_left(
		&self,
		row_number: RowNumber,
		left: &Columns,
		left_idx: usize,
		right_shape: &Columns,
	) -> Columns {
		let total_columns = self.left_column_count + self.right_column_names.len();
		let mut result_columns = Vec::with_capacity(total_columns);

		for (i, left_col) in left.columns.iter().enumerate() {
			let mut col_data = ColumnBuffer::with_capacity(left_col.get_type(), 1);
			col_data.push_value(left_col.get_value(left_idx));
			result_columns.push(ColumnWithName::new(left.names[i].clone(), col_data));
		}

		for (right_col, aliased_name) in right_shape.columns.iter().zip(self.right_column_names.iter()) {
			let mut col_data = ColumnBuffer::with_capacity(right_col.get_type(), 1);
			col_data.push_value(Value::none());
			result_columns.push(ColumnWithName::new(Fragment::internal(aliased_name), col_data));
		}

		Columns::with_system_columns(
			result_columns,
			vec![row_number],
			Self::extract_single_timestamp(&left.created_at, left_idx),
			Self::extract_single_timestamp(&left.updated_at, left_idx),
		)
	}

	pub(crate) fn unmatched_left_batch(
		&self,
		row_numbers: &[RowNumber],
		left: &Columns,
		left_indices: &[usize],
		right_shape: &Columns,
	) -> Columns {
		let count = left_indices.len();
		debug_assert_eq!(row_numbers.len(), count, "row_numbers must match indices count");

		let total_columns = self.left_column_count + self.right_column_names.len();
		let mut result_columns = Vec::with_capacity(total_columns);

		for (i, left_col) in left.columns.iter().enumerate() {
			let mut col_data = ColumnBuffer::with_capacity(left_col.get_type(), count);
			for &idx in left_indices {
				col_data.push_value(left_col.get_value(idx));
			}
			result_columns.push(ColumnWithName::new(left.names[i].clone(), col_data));
		}

		for (right_col, aliased_name) in right_shape.columns.iter().zip(self.right_column_names.iter()) {
			let mut col_data = ColumnBuffer::with_capacity(right_col.get_type(), count);
			for _ in 0..count {
				col_data.push_value(Value::none());
			}
			result_columns.push(ColumnWithName::new(Fragment::internal(aliased_name), col_data));
		}

		Columns::with_system_columns(
			result_columns,
			row_numbers.to_vec(),
			Self::extract_timestamps_at_indices(&left.created_at, left_indices),
			Self::extract_timestamps_at_indices(&left.updated_at, left_indices),
		)
	}

	pub(crate) fn right_column_names(&self) -> &[String] {
		&self.right_column_names
	}

	fn extract_single_timestamp(ts: &CowVec<DateTime>, idx: usize) -> Vec<DateTime> {
		if ts.is_empty() {
			Vec::new()
		} else {
			vec![ts[idx]]
		}
	}

	fn duplicate_timestamp(ts: &CowVec<DateTime>, idx: usize, count: usize) -> Vec<DateTime> {
		if ts.is_empty() {
			Vec::new()
		} else {
			vec![ts[idx]; count]
		}
	}

	fn expand_timestamps_cartesian(
		ts: &CowVec<DateTime>,
		left_indices: &[usize],
		right_count: usize,
	) -> Vec<DateTime> {
		if ts.is_empty() {
			return Vec::new();
		}
		let mut result = Vec::with_capacity(left_indices.len() * right_count);
		for &left_idx in left_indices {
			for _ in 0..right_count {
				result.push(ts[left_idx]);
			}
		}
		result
	}

	fn extract_timestamps_at_indices(ts: &CowVec<DateTime>, indices: &[usize]) -> Vec<DateTime> {
		if ts.is_empty() {
			Vec::new()
		} else {
			indices.iter().map(|&i| ts[i]).collect()
		}
	}
}
