// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	fragment::Fragment,
	util::bitvec::BitVec,
	value::{Value, row_number::RowNumber, r#type::Type},
};

use crate::{
	encoded::{encoded::EncodedValues, schema::Schema},
	interface::catalog::dictionary::DictionaryDef,
	value::column::{Column, columns::Columns, data::ColumnData},
};

/// Metadata for a column in a lazy batch
#[derive(Debug, Clone)]
pub struct LazyColumnMeta {
	/// Column name
	pub name: Fragment,
	/// Type as stored in encoded form (e.g., Uint2 for dictionary ID)
	pub storage_type: Type,
	/// Type after decoding (e.g., Utf8 for dictionary value)
	pub output_type: Type,
	/// Dictionary definition if this column uses dictionary encoding
	pub dictionary: Option<DictionaryDef>,
}

/// A batch of rows that defers column materialization until needed.
///
/// This allows filters to evaluate predicates on encoded data and only
/// materialize the rows that pass the filter.
#[derive(Debug, Clone)]
pub struct LazyBatch {
	/// Encoded row data
	rows: Vec<EncodedValues>,
	/// Row numbers from storage
	row_numbers: Vec<RowNumber>,
	/// Schema for interpreting encoded rows
	schema: Schema,
	/// Column metadata (names, types, dictionary defs)
	column_metas: Vec<LazyColumnMeta>,
	/// Validity bitmap - rows that passed filters (true = valid)
	validity: BitVec,
}

impl LazyBatch {
	pub fn new(
		rows: Vec<EncodedValues>,
		row_numbers: Vec<RowNumber>,
		schema: &Schema,
		column_metas: Vec<LazyColumnMeta>,
	) -> Self {
		debug_assert_eq!(rows.len(), row_numbers.len());
		debug_assert_eq!(schema.field_count(), column_metas.len());

		let validity = BitVec::repeat(rows.len(), true);
		Self {
			rows,
			row_numbers,
			schema: schema.clone(),
			column_metas,
			validity,
		}
	}

	/// Total number of rows (before filtering)
	pub fn total_row_count(&self) -> usize {
		self.rows.len()
	}

	/// Number of valid (non-filtered) rows
	pub fn valid_row_count(&self) -> usize {
		self.validity.count_ones()
	}

	/// Check if a specific row is valid (not filtered out)
	#[inline]
	pub fn is_row_valid(&self, row_idx: usize) -> bool {
		self.validity.get(row_idx)
	}

	/// Number of columns
	pub fn column_count(&self) -> usize {
		self.column_metas.len()
	}

	/// Get column metadata by index
	pub fn column_meta(&self, col_idx: usize) -> Option<&LazyColumnMeta> {
		self.column_metas.get(col_idx)
	}

	/// Find column index by name
	pub fn column_index(&self, name: &str) -> Option<usize> {
		self.column_metas.iter().position(|m| m.name.text() == name)
	}

	/// Get a value from encoded row without full materialization
	pub fn get_value(&self, row_idx: usize, col_idx: usize) -> Value {
		self.schema.get_value(&self.rows[row_idx], col_idx)
	}

	/// Check if a value is defined (not null) without materializing
	#[inline]
	pub fn is_defined(&self, row_idx: usize, col_idx: usize) -> bool {
		self.rows[row_idx].is_defined(col_idx)
	}

	/// Apply a filter mask to this batch.
	/// Only keeps rows where the mask bit is true.
	/// ANDs with the existing validity mask.
	pub fn apply_filter(&mut self, filter: &BitVec) {
		debug_assert_eq!(filter.len(), self.rows.len());

		for i in 0..self.validity.len() {
			if self.validity.get(i) && !filter.get(i) {
				self.validity.set(i, false);
			}
		}
	}

	/// Materialize to Columns, only including valid rows.
	/// Does NOT decode dictionary columns - call decode_dictionaries separately if needed.
	pub fn into_columns(self) -> Columns {
		let valid_count = self.valid_row_count();

		// Collect valid row numbers
		let mut result_row_numbers = Vec::with_capacity(valid_count);
		for (row_idx, &row_num) in self.row_numbers.iter().enumerate() {
			if self.is_row_valid(row_idx) {
				result_row_numbers.push(row_num);
			}
		}

		// Materialize each column
		let mut result_columns = Vec::with_capacity(self.column_metas.len());
		for (col_idx, meta) in self.column_metas.iter().enumerate() {
			let mut data = ColumnData::with_capacity(meta.storage_type.clone(), valid_count);

			for (row_idx, row) in self.rows.iter().enumerate() {
				if self.is_row_valid(row_idx) {
					let value = self.schema.get_value(row, col_idx);
					data.push_value(value);
				}
			}

			result_columns.push(Column {
				name: meta.name.clone(),
				data,
			});
		}

		Columns::with_row_numbers(result_columns, result_row_numbers)
	}

	/// Get column names for headers
	pub fn column_names(&self) -> Vec<Fragment> {
		self.column_metas.iter().map(|m| m.name.clone()).collect()
	}

	/// Get the schema
	pub fn schema(&self) -> &Schema {
		&self.schema
	}

	#[deprecated(since = "0.1.0", note = "Use schema() instead")]
	pub fn layout(&self) -> &Schema {
		&self.schema
	}

	/// Get the column metas
	pub fn column_metas(&self) -> &[LazyColumnMeta] {
		&self.column_metas
	}

	/// Iterator over valid row indices
	pub fn valid_row_indices(&self) -> impl Iterator<Item = usize> + '_ {
		(0..self.rows.len()).filter(|&i| self.is_row_valid(i))
	}

	/// Get encoded row by index
	pub fn encoded_row(&self, row_idx: usize) -> &EncodedValues {
		&self.rows[row_idx]
	}

	/// Get row number by index
	pub fn row_number(&self, row_idx: usize) -> RowNumber {
		self.row_numbers[row_idx]
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_type::value::{row_number::RowNumber, r#type::Type};

	use super::*;

	fn create_test_schema() -> Schema {
		Schema::testing(&[Type::Int4, Type::Utf8, Type::Boolean])
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
		let schema = create_test_schema();
		let metas = create_test_metas();

		// Create some encoded rows
		let mut row1 = schema.allocate();
		schema.set_i32(&mut row1, 0, 1);
		schema.set_utf8(&mut row1, 1, "Alice");
		schema.set_bool(&mut row1, 2, true);

		let mut row2 = schema.allocate();
		schema.set_i32(&mut row2, 0, 2);
		schema.set_utf8(&mut row2, 1, "Bob");
		schema.set_bool(&mut row2, 2, false);

		let batch = LazyBatch::new(vec![row1, row2], vec![RowNumber(100), RowNumber(101)], &schema, metas);

		assert_eq!(batch.total_row_count(), 2);
		assert_eq!(batch.valid_row_count(), 2);
		assert_eq!(batch.column_count(), 3);
	}

	#[test]
	fn test_get_value() {
		let schema = create_test_schema();
		let metas = create_test_metas();

		let mut row1 = schema.allocate();
		schema.set_i32(&mut row1, 0, 42);
		schema.set_utf8(&mut row1, 1, "Test");
		schema.set_bool(&mut row1, 2, true);

		let batch = LazyBatch::new(vec![row1], vec![RowNumber(1)], &schema, metas);

		assert_eq!(batch.get_value(0, 0), Value::Int4(42));
		assert_eq!(batch.get_value(0, 1), Value::Utf8("Test".to_string()));
		assert_eq!(batch.get_value(0, 2), Value::Boolean(true));
	}

	#[test]
	fn test_apply_filter() {
		let schema = create_test_schema();
		let metas = create_test_metas();

		let mut row1 = schema.allocate();
		schema.set_i32(&mut row1, 0, 1);
		schema.set_utf8(&mut row1, 1, "A");
		schema.set_bool(&mut row1, 2, true);

		let mut row2 = schema.allocate();
		schema.set_i32(&mut row2, 0, 2);
		schema.set_utf8(&mut row2, 1, "B");
		schema.set_bool(&mut row2, 2, false);

		let mut row3 = schema.allocate();
		schema.set_i32(&mut row3, 0, 3);
		schema.set_utf8(&mut row3, 1, "C");
		schema.set_bool(&mut row3, 2, true);

		let mut batch = LazyBatch::new(
			vec![row1, row2, row3],
			vec![RowNumber(1), RowNumber(2), RowNumber(3)],
			&schema,
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
		let schema = create_test_schema();
		let metas = create_test_metas();

		let mut row1 = schema.allocate();
		schema.set_i32(&mut row1, 0, 1);
		schema.set_utf8(&mut row1, 1, "Alice");
		schema.set_bool(&mut row1, 2, true);

		let mut row2 = schema.allocate();
		schema.set_i32(&mut row2, 0, 2);
		schema.set_utf8(&mut row2, 1, "Bob");
		schema.set_bool(&mut row2, 2, false);

		let mut row3 = schema.allocate();
		schema.set_i32(&mut row3, 0, 3);
		schema.set_utf8(&mut row3, 1, "Charlie");
		schema.set_bool(&mut row3, 2, true);

		let mut batch = LazyBatch::new(
			vec![row1, row2, row3],
			vec![RowNumber(100), RowNumber(101), RowNumber(102)],
			&schema,
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
		assert_eq!(columns[0].data().get_value(0), Value::Int4(2));
		assert_eq!(columns[1].data().get_value(0), Value::Utf8("Bob".to_string()));
		assert_eq!(columns[2].data().get_value(0), Value::Boolean(false));
	}

	#[test]
	fn test_column_index() {
		let schema = create_test_schema();
		let metas = create_test_metas();

		let row = schema.allocate();
		let batch = LazyBatch::new(vec![row], vec![RowNumber(1)], &schema, metas);

		assert_eq!(batch.column_index("id"), Some(0));
		assert_eq!(batch.column_index("name"), Some(1));
		assert_eq!(batch.column_index("active"), Some(2));
		assert_eq!(batch.column_index("nonexistent"), None);
	}

	#[test]
	fn test_multiple_filters() {
		let schema = create_test_schema();
		let metas = create_test_metas();

		let mut rows = Vec::new();
		for i in 0..5 {
			let mut row = schema.allocate();
			schema.set_i32(&mut row, 0, i);
			schema.set_utf8(&mut row, 1, &format!("row{}", i));
			schema.set_bool(&mut row, 2, i % 2 == 0);
			rows.push(row);
		}

		let mut batch = LazyBatch::new(
			rows,
			vec![RowNumber(10), RowNumber(11), RowNumber(12), RowNumber(13), RowNumber(14)],
			&schema,
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
