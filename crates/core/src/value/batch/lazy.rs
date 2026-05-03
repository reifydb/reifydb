// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	fragment::Fragment,
	util::bitvec::BitVec,
	value::{Value, datetime::DateTime, row_number::RowNumber, r#type::Type},
};

use crate::{
	encoded::{row::EncodedRow, shape::RowShape},
	interface::catalog::dictionary::Dictionary,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};

#[derive(Debug, Clone)]
pub struct LazyColumnMeta {
	pub name: Fragment,

	pub storage_type: Type,

	pub output_type: Type,

	pub dictionary: Option<Dictionary>,
}

#[derive(Debug, Clone)]
pub struct LazyBatch {
	rows: Vec<EncodedRow>,

	row_numbers: Vec<RowNumber>,

	shape: RowShape,

	column_metas: Vec<LazyColumnMeta>,

	validity: BitVec,
}

impl LazyBatch {
	pub fn new(
		rows: Vec<EncodedRow>,
		row_numbers: Vec<RowNumber>,
		shape: &RowShape,
		column_metas: Vec<LazyColumnMeta>,
	) -> Self {
		debug_assert_eq!(rows.len(), row_numbers.len());
		debug_assert_eq!(shape.field_count(), column_metas.len());

		let validity = BitVec::repeat(rows.len(), true);
		Self {
			rows,
			row_numbers,
			shape: shape.clone(),
			column_metas,
			validity,
		}
	}

	pub fn total_row_count(&self) -> usize {
		self.rows.len()
	}

	pub fn valid_row_count(&self) -> usize {
		self.validity.count_ones()
	}

	#[inline]
	pub fn is_row_valid(&self, row_idx: usize) -> bool {
		self.validity.get(row_idx)
	}

	pub fn column_count(&self) -> usize {
		self.column_metas.len()
	}

	pub fn column_meta(&self, col_idx: usize) -> Option<&LazyColumnMeta> {
		self.column_metas.get(col_idx)
	}

	pub fn column_index(&self, name: &str) -> Option<usize> {
		self.column_metas.iter().position(|m| m.name.text() == name)
	}

	pub fn get_value(&self, row_idx: usize, col_idx: usize) -> Value {
		self.shape.get_value(&self.rows[row_idx], col_idx)
	}

	#[inline]
	pub fn is_defined(&self, row_idx: usize, col_idx: usize) -> bool {
		self.rows[row_idx].is_defined(col_idx)
	}

	pub fn apply_filter(&mut self, filter: &BitVec) {
		debug_assert_eq!(filter.len(), self.rows.len());

		for i in 0..self.validity.len() {
			if self.validity.get(i) && !filter.get(i) {
				self.validity.set(i, false);
			}
		}
	}

	pub fn into_columns(self) -> Columns {
		let valid_count = self.valid_row_count();

		let mut result_row_numbers = Vec::with_capacity(valid_count);
		let mut result_created_at = Vec::with_capacity(valid_count);
		let mut result_updated_at = Vec::with_capacity(valid_count);
		for (row_idx, &row_num) in self.row_numbers.iter().enumerate() {
			if self.is_row_valid(row_idx) {
				result_row_numbers.push(row_num);
				result_created_at.push(DateTime::from_nanos(self.rows[row_idx].created_at_nanos()));
				result_updated_at.push(DateTime::from_nanos(self.rows[row_idx].updated_at_nanos()));
			}
		}

		let mut result_columns = Vec::with_capacity(self.column_metas.len());
		for (col_idx, meta) in self.column_metas.iter().enumerate() {
			let mut data = ColumnBuffer::with_capacity(meta.storage_type.clone(), valid_count);

			for (row_idx, row) in self.rows.iter().enumerate() {
				if self.is_row_valid(row_idx) {
					let value = self.shape.get_value(row, col_idx);
					data.push_value(value);
				}
			}

			result_columns.push(ColumnWithName {
				name: meta.name.clone(),
				data,
			});
		}

		Columns::with_system_columns(result_columns, result_row_numbers, result_created_at, result_updated_at)
	}

	pub fn column_names(&self) -> Vec<Fragment> {
		self.column_metas.iter().map(|m| m.name.clone()).collect()
	}

	pub fn shape(&self) -> &RowShape {
		&self.shape
	}

	#[deprecated(since = "0.1.0", note = "Use shape() instead")]
	pub fn layout(&self) -> &RowShape {
		&self.shape
	}

	pub fn column_metas(&self) -> &[LazyColumnMeta] {
		&self.column_metas
	}

	pub fn valid_row_indices(&self) -> impl Iterator<Item = usize> + '_ {
		(0..self.rows.len()).filter(|&i| self.is_row_valid(i))
	}

	pub fn encoded_row(&self, row_idx: usize) -> &EncodedRow {
		&self.rows[row_idx]
	}

	pub fn row_number(&self, row_idx: usize) -> RowNumber {
		self.row_numbers[row_idx]
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::{row_number::RowNumber, r#type::Type};

	use super::*;

	fn create_test_shape() -> RowShape {
		RowShape::testing(&[Type::Int4, Type::Utf8, Type::Boolean])
	}

	fn create_test_metas() -> Vec<LazyColumnMeta> {
		vec![
			LazyColumnMeta {
				name: Fragment::internal("id"),
				storage_type: Type::Int4,
				output_type: Type::Int4,
				dictionary: None,
			},
			LazyColumnMeta {
				name: Fragment::internal("name"),
				storage_type: Type::Utf8,
				output_type: Type::Utf8,
				dictionary: None,
			},
			LazyColumnMeta {
				name: Fragment::internal("active"),
				storage_type: Type::Boolean,
				output_type: Type::Boolean,
				dictionary: None,
			},
		]
	}

	#[test]
	fn test_lazy_batch_creation() {
		let shape = create_test_shape();
		let metas = create_test_metas();

		// Create some encoded rows
		let mut row1 = shape.allocate();
		shape.set_i32(&mut row1, 0, 1);
		shape.set_utf8(&mut row1, 1, "Alice");
		shape.set_bool(&mut row1, 2, true);

		let mut row2 = shape.allocate();
		shape.set_i32(&mut row2, 0, 2);
		shape.set_utf8(&mut row2, 1, "Bob");
		shape.set_bool(&mut row2, 2, false);

		let batch = LazyBatch::new(vec![row1, row2], vec![RowNumber(100), RowNumber(101)], &shape, metas);

		assert_eq!(batch.total_row_count(), 2);
		assert_eq!(batch.valid_row_count(), 2);
		assert_eq!(batch.column_count(), 3);
	}

	#[test]
	fn test_get_value() {
		let shape = create_test_shape();
		let metas = create_test_metas();

		let mut row1 = shape.allocate();
		shape.set_i32(&mut row1, 0, 42);
		shape.set_utf8(&mut row1, 1, "Test");
		shape.set_bool(&mut row1, 2, true);

		let batch = LazyBatch::new(vec![row1], vec![RowNumber(1)], &shape, metas);

		assert_eq!(batch.get_value(0, 0), Value::Int4(42));
		assert_eq!(batch.get_value(0, 1), Value::Utf8("Test".to_string()));
		assert_eq!(batch.get_value(0, 2), Value::Boolean(true));
	}

	#[test]
	fn test_apply_filter() {
		let shape = create_test_shape();
		let metas = create_test_metas();

		let mut row1 = shape.allocate();
		shape.set_i32(&mut row1, 0, 1);
		shape.set_utf8(&mut row1, 1, "A");
		shape.set_bool(&mut row1, 2, true);

		let mut row2 = shape.allocate();
		shape.set_i32(&mut row2, 0, 2);
		shape.set_utf8(&mut row2, 1, "B");
		shape.set_bool(&mut row2, 2, false);

		let mut row3 = shape.allocate();
		shape.set_i32(&mut row3, 0, 3);
		shape.set_utf8(&mut row3, 1, "C");
		shape.set_bool(&mut row3, 2, true);

		let mut batch = LazyBatch::new(
			vec![row1, row2, row3],
			vec![RowNumber(1), RowNumber(2), RowNumber(3)],
			&shape,
			metas,
		);

		// Filter: keep rows 0 and 2
		let mut filter = BitVec::repeat(3, false);
		filter.set(0, true);
		filter.set(2, true);
		batch.apply_filter(&filter);

		assert_eq!(batch.total_row_count(), 3);
		assert_eq!(batch.valid_row_count(), 2);
		assert!(batch.is_row_valid(0));
		assert!(!batch.is_row_valid(1));
		assert!(batch.is_row_valid(2));
	}

	#[test]
	fn test_into_columns() {
		let shape = create_test_shape();
		let metas = create_test_metas();

		let mut row1 = shape.allocate();
		shape.set_i32(&mut row1, 0, 1);
		shape.set_utf8(&mut row1, 1, "Alice");
		shape.set_bool(&mut row1, 2, true);

		let mut row2 = shape.allocate();
		shape.set_i32(&mut row2, 0, 2);
		shape.set_utf8(&mut row2, 1, "Bob");
		shape.set_bool(&mut row2, 2, false);

		let mut row3 = shape.allocate();
		shape.set_i32(&mut row3, 0, 3);
		shape.set_utf8(&mut row3, 1, "Charlie");
		shape.set_bool(&mut row3, 2, true);

		let mut batch = LazyBatch::new(
			vec![row1, row2, row3],
			vec![RowNumber(100), RowNumber(101), RowNumber(102)],
			&shape,
			metas,
		);

		// Filter: keep only row 1 (Bob)
		let mut filter = BitVec::repeat(3, false);
		filter.set(1, true);
		batch.apply_filter(&filter);

		let columns = batch.into_columns();

		assert_eq!(columns.row_count(), 1);
		assert_eq!(columns.row_numbers.len(), 1);
		assert_eq!(columns.row_numbers[0], RowNumber(101));

		// Check values
		assert_eq!(columns[0].get_value(0), Value::Int4(2));
		assert_eq!(columns[1].get_value(0), Value::Utf8("Bob".to_string()));
		assert_eq!(columns[2].get_value(0), Value::Boolean(false));
	}

	#[test]
	fn test_column_index() {
		let shape = create_test_shape();
		let metas = create_test_metas();

		let row = shape.allocate();
		let batch = LazyBatch::new(vec![row], vec![RowNumber(1)], &shape, metas);

		assert_eq!(batch.column_index("id"), Some(0));
		assert_eq!(batch.column_index("name"), Some(1));
		assert_eq!(batch.column_index("active"), Some(2));
		assert_eq!(batch.column_index("nonexistent"), None);
	}

	#[test]
	fn test_multiple_filters() {
		let shape = create_test_shape();
		let metas = create_test_metas();

		let mut rows = Vec::new();
		for i in 0..5 {
			let mut row = shape.allocate();
			shape.set_i32(&mut row, 0, i);
			shape.set_utf8(&mut row, 1, &format!("row{}", i));
			shape.set_bool(&mut row, 2, i % 2 == 0);
			rows.push(row);
		}

		let mut batch = LazyBatch::new(
			rows,
			vec![RowNumber(10), RowNumber(11), RowNumber(12), RowNumber(13), RowNumber(14)],
			&shape,
			metas,
		);

		// First filter: keep 0, 2, 4 (even indices)
		let mut filter1 = BitVec::repeat(5, false);
		filter1.set(0, true);
		filter1.set(2, true);
		filter1.set(4, true);
		batch.apply_filter(&filter1);

		assert_eq!(batch.valid_row_count(), 3);

		// Second filter: from remaining, keep only 2
		let mut filter2 = BitVec::repeat(5, false);
		filter2.set(2, true);
		batch.apply_filter(&filter2);

		assert_eq!(batch.valid_row_count(), 1);
		assert!(batch.is_row_valid(2));
		assert!(!batch.is_row_valid(0));
		assert!(!batch.is_row_valid(4));
	}
}
