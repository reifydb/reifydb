// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, rc::Rc};

use super::{extract::FrameError, frame::Frame};
use crate::value::{
	Value,
	row_number::RowNumber,
	try_from::{TryFromValue, TryFromValueCoerce},
};

/// Index for O(1) column lookup by name.
///
/// Built once when iterating over rows, then shared by all FrameRow references.
#[derive(Debug)]
struct ColumnIndex {
	by_name: HashMap<String, usize>,
}

impl ColumnIndex {
	fn new(frame: &Frame) -> Self {
		let mut by_name = HashMap::with_capacity(frame.columns.len());
		for (idx, col) in frame.columns.iter().enumerate() {
			by_name.insert(col.name.clone(), idx);
		}
		Self {
			by_name,
		}
	}

	fn get(&self, name: &str) -> Option<usize> {
		self.by_name.get(name).copied()
	}
}

/// A reference to a single row in a Frame.
///
/// Provides ergonomic access to column values by name.
#[derive(Debug)]
pub struct FrameRow<'a> {
	frame: &'a Frame,
	index: Rc<ColumnIndex>,
	row_idx: usize,
}

impl<'a> FrameRow<'a> {
	/// Get a typed value from this row by column name (strict type matching).
	///
	/// Returns `Ok(None)` for Undefined values.
	pub fn get<T: TryFromValue>(&self, column: &str) -> Result<Option<T>, FrameError> {
		let col_idx = self.index.get(column).ok_or_else(|| FrameError::ColumnNotFound {
			name: column.to_string(),
		})?;

		let col = &self.frame.columns[col_idx];

		if !col.data.is_defined(self.row_idx) {
			return Ok(None);
		}

		let value = col.data.get_value(self.row_idx);
		T::try_from_value(&value).map(Some).map_err(|e| FrameError::ValueError {
			column: column.to_string(),
			row: self.row_idx,
			error: e,
		})
	}

	/// Get a typed value with widening coercion.
	///
	/// Returns `Ok(None)` for Undefined values.
	pub fn get_coerce<T: TryFromValueCoerce>(&self, column: &str) -> Result<Option<T>, FrameError> {
		let col_idx = self.index.get(column).ok_or_else(|| FrameError::ColumnNotFound {
			name: column.to_string(),
		})?;

		let col = &self.frame.columns[col_idx];

		if !col.data.is_defined(self.row_idx) {
			return Ok(None);
		}

		let value = col.data.get_value(self.row_idx);
		T::try_from_value_coerce(&value).map(Some).map_err(|e| FrameError::ValueError {
			column: column.to_string(),
			row: self.row_idx,
			error: e,
		})
	}

	/// Get the raw Value by column name.
	///
	/// Returns the owned Value, or None if the column doesn't exist.
	pub fn get_value(&self, column: &str) -> Option<Value> {
		self.index.get(column).map(|col_idx| self.frame.columns[col_idx].data.get_value(self.row_idx))
	}

	/// Get the row index (0-based position in the frame).
	pub fn index(&self) -> usize {
		self.row_idx
	}

	/// Get the row number from frame metadata, if available.
	pub fn row_number(&self) -> Option<RowNumber> {
		self.frame.row_numbers.get(self.row_idx).copied()
	}

	/// Check if a column value is defined (not Undefined).
	pub fn is_defined(&self, column: &str) -> Option<bool> {
		self.index.get(column).map(|col_idx| self.frame.columns[col_idx].data.is_defined(self.row_idx))
	}
}

/// Iterator over rows in a Frame.
///
/// Created by calling `frame.rows()`. Yields `FrameRow` references that
/// provide ergonomic access to column values.
pub struct FrameRows<'a> {
	frame: &'a Frame,
	index: Rc<ColumnIndex>,
	current: usize,
	len: usize,
}

impl<'a> FrameRows<'a> {
	pub(super) fn new(frame: &'a Frame) -> Self {
		let len = frame.columns.first().map(|c| c.data.len()).unwrap_or(0);
		Self {
			frame,
			index: Rc::new(ColumnIndex::new(frame)),
			current: 0,
			len,
		}
	}
}

impl<'a> Iterator for FrameRows<'a> {
	type Item = FrameRow<'a>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.current >= self.len {
			return None;
		}

		let row = FrameRow {
			frame: self.frame,
			index: Rc::clone(&self.index),
			row_idx: self.current,
		};

		self.current += 1;
		Some(row)
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		let remaining = self.len.saturating_sub(self.current);
		(remaining, Some(remaining))
	}
}

impl ExactSizeIterator for FrameRows<'_> {}

impl<'a> DoubleEndedIterator for FrameRows<'a> {
	fn next_back(&mut self) -> Option<Self::Item> {
		if self.current >= self.len {
			return None;
		}

		self.len -= 1;

		Some(FrameRow {
			frame: self.frame,
			index: Rc::clone(&self.index),
			row_idx: self.len,
		})
	}
}

impl Frame {
	/// Iterate over rows in the frame.
	///
	/// # Example
	///
	/// ```ignore
	/// for row in frame.rows() {
	///     let id: Option<i64> = row.get("id")?;
	///     let name: Option<String> = row.get("name")?;
	///     println!("{:?}: {:?}", id, name);
	/// }
	/// ```
	pub fn rows(&self) -> FrameRows<'_> {
		FrameRows::new(self)
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;
	use crate::{
		util::bitvec::BitVec,
		value::{
			container::{number::NumberContainer, utf8::Utf8Container},
			frame::{column::FrameColumn, data::FrameColumnData},
		},
	};

	fn make_test_frame() -> Frame {
		Frame::with_row_numbers(
			vec![
				FrameColumn {
					name: "id".to_string(),
					data: FrameColumnData::Int8(NumberContainer::from_vec(vec![1i64, 2, 3])),
				},
				FrameColumn {
					name: "name".to_string(),
					data: FrameColumnData::Utf8(Utf8Container::new(
						vec!["Alice".to_string(), "Bob".to_string(), String::new()],
						BitVec::from_slice(&[true, true, false]),
					)),
				},
			],
			vec![100.into(), 200.into(), 300.into()],
		)
	}

	#[test]
	fn test_rows_iterator() {
		let frame = make_test_frame();
		let rows: Vec<_> = frame.rows().collect();

		assert_eq!(rows.len(), 3);
		assert_eq!(rows[0].index(), 0);
		assert_eq!(rows[1].index(), 1);
		assert_eq!(rows[2].index(), 2);
	}

	#[test]
	fn test_row_get() {
		let frame = make_test_frame();
		let mut rows = frame.rows();

		let row0 = rows.next().unwrap();
		assert_eq!(row0.get::<i64>("id").unwrap(), Some(1i64));
		assert_eq!(row0.get::<String>("name").unwrap(), Some("Alice".to_string()));

		let row2 = rows.nth(1).unwrap(); // Skip to index 2
		assert_eq!(row2.get::<i64>("id").unwrap(), Some(3i64));
		assert_eq!(row2.get::<String>("name").unwrap(), None); // Undefined
	}

	#[test]
	fn test_row_get_coerce() {
		let frame = make_test_frame();
		let row = frame.rows().next().unwrap();

		// i64 coerced to f64
		let id: Option<f64> = row.get_coerce("id").unwrap();
		assert_eq!(id, Some(1.0f64));
	}

	#[test]
	fn test_row_get_value() {
		let frame = make_test_frame();
		let row = frame.rows().next().unwrap();

		let value = row.get_value("id");
		assert!(matches!(value, Some(Value::Int8(1))));

		let missing = row.get_value("nonexistent");
		assert!(missing.is_none());
	}

	#[test]
	fn test_row_number() {
		let frame = make_test_frame();
		let rows: Vec<_> = frame.rows().collect();

		assert_eq!(rows[0].row_number(), Some(100.into()));
		assert_eq!(rows[1].row_number(), Some(200.into()));
		assert_eq!(rows[2].row_number(), Some(300.into()));
	}

	#[test]
	fn test_is_defined() {
		let frame = make_test_frame();
		let rows: Vec<_> = frame.rows().collect();

		assert_eq!(rows[0].is_defined("name"), Some(true));
		assert_eq!(rows[2].is_defined("name"), Some(false)); // Undefined
		assert_eq!(rows[0].is_defined("nonexistent"), None);
	}

	#[test]
	fn test_exact_size_iterator() {
		let frame = make_test_frame();
		let rows = frame.rows();

		assert_eq!(rows.len(), 3);
	}

	#[test]
	fn test_double_ended_iterator() {
		let frame = make_test_frame();
		let mut rows = frame.rows();

		let last = rows.next_back().unwrap();
		assert_eq!(last.index(), 2);

		let first = rows.next().unwrap();
		assert_eq!(first.index(), 0);
	}
}
