// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::{
	fragment::Fragment,
	util::cowvec::CowVec,
	value::{Value, row_number::RowNumber},
};

/// Builder for creating combined columns when joining left and right sides.
/// Encapsulates the logic for merging column names (with conflict resolution)
/// and types from both sides of a join.
pub(crate) struct JoinedColumnsBuilder {
	left_column_count: usize,
	/// Pre-computed aliased names for right columns
	right_column_names: Vec<String>,
}

impl JoinedColumnsBuilder {
	/// Create a new builder from left and right Columns templates.
	/// Handles name conflicts by applying the alias prefix.
	pub(crate) fn new(left: &Columns, right: &Columns, alias: &Option<String>) -> Self {
		let left_column_count = left.columns.len();

		// Collect left column names for conflict detection
		let left_names: Vec<String> = left.columns.iter().map(|c| c.name.as_ref().to_string()).collect();

		// Compute right column names with alias prefix
		let alias_str = alias.as_deref().unwrap_or("other");
		let mut right_column_names = Vec::with_capacity(right.columns.len());
		let mut all_names = left_names.clone();

		for col in right.columns.iter() {
			let col_name = col.name.as_ref();
			let prefixed_name = format!("{}_{}", alias_str, col_name);

			// Check for conflict with existing names
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

	/// Join a single left row with a single right row.
	/// Both Columns must contain exactly one row.
	pub(crate) fn join_single(&self, row_number: RowNumber, left: &Columns, right: &Columns) -> Columns {
		debug_assert_eq!(left.row_count(), 1, "left must have exactly 1 row");
		debug_assert_eq!(right.row_count(), 1, "right must have exactly 1 row");

		self.join_one_to_many(&[row_number], left, 0, right)
	}

	/// Join a single left row at left_idx with a single right row at right_idx.
	/// Avoids extraction by accessing values directly at the specified indices.
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

		// Add left columns - single value from left_idx
		for left_col in left.columns.iter() {
			let mut col_data = ColumnData::with_capacity(left_col.data().get_type(), 1);
			col_data.push_value(left_col.data().get_value(left_idx));
			result_columns.push(Column {
				name: left_col.name.clone(),
				data: col_data,
			});
		}

		// Add right columns - single value from right_idx
		for (right_col, aliased_name) in right.columns.iter().zip(self.right_column_names.iter()) {
			let mut col_data = ColumnData::with_capacity(right_col.data().get_type(), 1);
			col_data.push_value(right_col.data().get_value(right_idx));
			result_columns.push(Column {
				name: Fragment::internal(aliased_name),
				data: col_data,
			});
		}

		Columns {
			row_numbers: CowVec::new(vec![row_number]),
			columns: CowVec::new(result_columns),
		}
	}

	/// Join one left row (at left_idx) with all right rows.
	/// Produces right.row_count() output rows.
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

		// Add left columns - duplicate the left row value for each right row
		for left_col in left.columns.iter() {
			let left_value = left_col.data().get_value(left_idx);
			let mut col_data = ColumnData::with_capacity(left_col.data().get_type(), right_count);
			for _ in 0..right_count {
				col_data.push_value(left_value.clone());
			}
			result_columns.push(Column {
				name: left_col.name.clone(),
				data: col_data,
			});
		}

		// Add right columns - copy all values from right
		for (right_col, aliased_name) in right.columns.iter().zip(self.right_column_names.iter()) {
			let mut col_data = ColumnData::with_capacity(right_col.data().get_type(), right_count);
			for row_idx in 0..right_count {
				col_data.push_value(right_col.data().get_value(row_idx));
			}
			result_columns.push(Column {
				name: Fragment::internal(aliased_name),
				data: col_data,
			});
		}

		Columns {
			row_numbers: CowVec::new(row_numbers.to_vec()),
			columns: CowVec::new(result_columns),
		}
	}

	/// Join all left rows with one right row (at right_idx).
	/// Produces left.row_count() output rows.
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

		// Add left columns - copy all values from left
		for left_col in left.columns.iter() {
			let mut col_data = ColumnData::with_capacity(left_col.data().get_type(), left_count);
			for row_idx in 0..left_count {
				col_data.push_value(left_col.data().get_value(row_idx));
			}
			result_columns.push(Column {
				name: left_col.name.clone(),
				data: col_data,
			});
		}

		// Add right columns - duplicate the right row value for each left row
		for (right_col, aliased_name) in right.columns.iter().zip(self.right_column_names.iter()) {
			let right_value = right_col.data().get_value(right_idx);
			let mut col_data = ColumnData::with_capacity(right_col.data().get_type(), left_count);
			for _ in 0..left_count {
				col_data.push_value(right_value.clone());
			}
			result_columns.push(Column {
				name: Fragment::internal(aliased_name),
				data: col_data,
			});
		}

		Columns {
			row_numbers: CowVec::new(row_numbers.to_vec()),
			columns: CowVec::new(result_columns),
		}
	}

	/// Join left rows (at specified indices) with all right rows (cartesian product).
	/// Produces left_indices.len() * right.row_count() output rows.
	pub(crate) fn join_cartesian(
		&self,
		row_numbers: &[RowNumber],
		left: &Columns,
		left_indices: &[usize],
		right: &Columns,
	) -> Columns {
		let left_count = left_indices.len();
		let right_count = right.row_count();
		let result_count = left_count * right_count;
		debug_assert_eq!(row_numbers.len(), result_count, "row_numbers must match cartesian product size");

		let total_columns = self.left_column_count + self.right_column_names.len();
		let mut result_columns = Vec::with_capacity(total_columns);

		// Add left columns - for each left index, duplicate value for all right rows
		for left_col in left.columns.iter() {
			let mut col_data = ColumnData::with_capacity(left_col.data().get_type(), result_count);
			for &left_idx in left_indices {
				let left_value = left_col.data().get_value(left_idx);
				for _ in 0..right_count {
					col_data.push_value(left_value.clone());
				}
			}
			result_columns.push(Column {
				name: left_col.name.clone(),
				data: col_data,
			});
		}

		// Add right columns - repeat all right rows for each left row
		for (right_col, aliased_name) in right.columns.iter().zip(self.right_column_names.iter()) {
			let mut col_data = ColumnData::with_capacity(right_col.data().get_type(), result_count);
			for _ in 0..left_count {
				for row_idx in 0..right_count {
					col_data.push_value(right_col.data().get_value(row_idx));
				}
			}
			result_columns.push(Column {
				name: Fragment::internal(aliased_name),
				data: col_data,
			});
		}

		Columns {
			row_numbers: CowVec::new(row_numbers.to_vec()),
			columns: CowVec::new(result_columns),
		}
	}

	/// Create unmatched left columns (left join with no right match).
	/// Right side columns are filled with Undefined values.
	pub(crate) fn unmatched_left(
		&self,
		row_number: RowNumber,
		left: &Columns,
		left_idx: usize,
		right_schema: &Columns,
	) -> Columns {
		let total_columns = self.left_column_count + self.right_column_names.len();
		let mut result_columns = Vec::with_capacity(total_columns);

		// Add left columns - single value from left_idx
		for left_col in left.columns.iter() {
			let mut col_data = ColumnData::with_capacity(left_col.data().get_type(), 1);
			col_data.push_value(left_col.data().get_value(left_idx));
			result_columns.push(Column {
				name: left_col.name.clone(),
				data: col_data,
			});
		}

		// Add right columns with Undefined values
		for (right_col, aliased_name) in right_schema.columns.iter().zip(self.right_column_names.iter()) {
			let mut col_data = ColumnData::with_capacity(right_col.data().get_type(), 1);
			col_data.push_value(Value::Undefined);
			result_columns.push(Column {
				name: Fragment::internal(aliased_name),
				data: col_data,
			});
		}

		Columns {
			row_numbers: CowVec::new(vec![row_number]),
			columns: CowVec::new(result_columns),
		}
	}

	/// Create unmatched left columns for multiple left rows.
	/// Right side columns are filled with Undefined values.
	pub(crate) fn unmatched_left_batch(
		&self,
		row_numbers: &[RowNumber],
		left: &Columns,
		left_indices: &[usize],
		right_schema: &Columns,
	) -> Columns {
		let count = left_indices.len();
		debug_assert_eq!(row_numbers.len(), count, "row_numbers must match indices count");

		let total_columns = self.left_column_count + self.right_column_names.len();
		let mut result_columns = Vec::with_capacity(total_columns);

		// Add left columns - values from specified indices
		for left_col in left.columns.iter() {
			let mut col_data = ColumnData::with_capacity(left_col.data().get_type(), count);
			for &idx in left_indices {
				col_data.push_value(left_col.data().get_value(idx));
			}
			result_columns.push(Column {
				name: left_col.name.clone(),
				data: col_data,
			});
		}

		// Add right columns with Undefined values
		for (right_col, aliased_name) in right_schema.columns.iter().zip(self.right_column_names.iter()) {
			let mut col_data = ColumnData::with_capacity(right_col.data().get_type(), count);
			for _ in 0..count {
				col_data.push_value(Value::Undefined);
			}
			result_columns.push(Column {
				name: Fragment::internal(aliased_name),
				data: col_data,
			});
		}

		Columns {
			row_numbers: CowVec::new(row_numbers.to_vec()),
			columns: CowVec::new(result_columns),
		}
	}

	/// Get the pre-computed aliased names for right columns.
	pub(crate) fn right_column_names(&self) -> &[String] {
		&self.right_column_names
	}
}
