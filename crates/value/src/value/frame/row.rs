// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, rc::Rc};

use super::{extract::FrameError, frame::Frame};
use crate::value::try_from::TryFromValue;

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

#[derive(Debug)]
pub struct FrameRow<'a> {
	frame: &'a Frame,
	index: Rc<ColumnIndex>,
	row_idx: usize,
}

impl<'a> FrameRow<'a> {
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
}

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
	pub fn rows(&self) -> FrameRows<'_> {
		FrameRows::new(self)
	}
}

#[cfg(test)]
pub mod tests {
	use arrow_array::{Int64Array, LargeStringArray};

	use super::*;
	use crate::value::frame::{column::FrameColumn, data::FrameColumnData};

	fn make_test_frame() -> Frame {
		Frame::with_row_numbers(
			vec![
				FrameColumn {
					name: "id".to_string(),
					data: FrameColumnData::Int8(Int64Array::from(vec![1i64, 2, 3])),
				},
				FrameColumn {
					name: "name".to_string(),
					data: FrameColumnData::Utf8(LargeStringArray::from(vec![
						"Alice".to_string(),
						"Bob".to_string(),
						String::new(),
					])),
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
		assert_eq!(rows[0].get::<i64>("id").unwrap(), Some(1i64));
		assert_eq!(rows[1].get::<i64>("id").unwrap(), Some(2i64));
		assert_eq!(rows[2].get::<i64>("id").unwrap(), Some(3i64));
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
		assert_eq!(row2.get::<String>("name").unwrap(), Some(String::new())); // All values are defined
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
		assert_eq!(last.get::<i64>("id").unwrap(), Some(3i64));

		let first = rows.next().unwrap();
		assert_eq!(first.get::<i64>("id").unwrap(), Some(1i64));
	}
}
