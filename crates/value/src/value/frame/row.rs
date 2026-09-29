// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, rc::Rc};

use super::{extract::FrameError, frame::Frame};
use crate::{
	error::Error,
	value::{column_view::ColumnView, try_from::TryFromValue},
};

#[derive(Debug)]
struct ColumnIndex<'a> {
	by_name: HashMap<String, usize>,
	views: Vec<Result<ColumnView<'a>, Error>>,
}

impl<'a> ColumnIndex<'a> {
	fn new(frame: &'a Frame) -> Self {
		let schema = frame.batch.schema_ref();
		let mut by_name = HashMap::with_capacity(schema.fields().len());
		let mut views = Vec::with_capacity(schema.fields().len());
		for (idx, (field, array)) in schema.fields().iter().zip(frame.batch.columns()).enumerate() {
			by_name.insert(field.name().clone(), idx);
			views.push(ColumnView::try_from((array, field.as_ref())));
		}
		Self {
			by_name,
			views,
		}
	}

	fn get(&self, name: &str) -> Option<&Result<ColumnView<'a>, Error>> {
		self.by_name.get(name).map(|&idx| &self.views[idx])
	}
}

#[derive(Debug)]
pub struct FrameRow<'a> {
	index: Rc<ColumnIndex<'a>>,
	row_idx: usize,
}

impl<'a> FrameRow<'a> {
	pub fn get<T: TryFromValue>(&self, column: &str) -> Result<Option<T>, FrameError> {
		let col = match self.index.get(column) {
			Some(Ok(view)) => view,
			Some(Err(error)) => {
				return Err(FrameError::InvalidColumn {
					name: column.to_string(),
					message: error.to_string(),
				});
			}
			None => {
				return Err(FrameError::ColumnNotFound {
					name: column.to_string(),
				});
			}
		};

		if !col.is_defined(self.row_idx) {
			return Ok(None);
		}

		let value = col.get_value(self.row_idx);
		T::try_from_value(&value).map(Some).map_err(|e| FrameError::ValueError {
			column: column.to_string(),
			row: self.row_idx,
			error: e,
		})
	}
}

pub struct FrameRows<'a> {
	index: Rc<ColumnIndex<'a>>,
	current: usize,
	len: usize,
}

impl<'a> FrameRows<'a> {
	pub(super) fn new(frame: &'a Frame) -> Self {
		Self {
			index: Rc::new(ColumnIndex::new(frame)),
			current: 0,
			len: frame.batch.num_rows(),
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
	use std::sync::Arc;

	use arrow_array::{ArrayRef, Int64Array, LargeStringArray, RecordBatch, UInt64Array};
	use arrow_schema::{FieldRef, Schema};

	use super::*;
	use crate::value::{
		system_columns::{SystemColumn, with_system_column},
		value_type::{
			ValueType,
			field::{FieldType, to_field},
		},
	};

	fn column(name: &str, value_type: ValueType, array: ArrayRef) -> (FieldRef, ArrayRef) {
		let field_type = FieldType {
			value_type: Some(value_type),
			..FieldType::default()
		};
		(Arc::new(to_field(name, &field_type)), array)
	}

	fn make_test_frame() -> Frame {
		let (fields, arrays): (Vec<FieldRef>, Vec<ArrayRef>) = vec![
			column("id", ValueType::Int8, Arc::new(Int64Array::from(vec![1i64, 2, 3]))),
			column(
				"name",
				ValueType::Utf8,
				Arc::new(LargeStringArray::from(vec![
					"Alice".to_string(),
					"Bob".to_string(),
					String::new(),
				])),
			),
		]
		.into_iter()
		.unzip();
		let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
		let batch = with_system_column(
			batch,
			SystemColumn::RowNumbers,
			Arc::new(UInt64Array::from(vec![100u64, 200, 300])),
		)
		.unwrap();
		Frame::from(batch)
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
