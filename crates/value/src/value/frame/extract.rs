// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	error,
	fmt::{self, Display, Formatter},
};

use super::frame::Frame;
use crate::value::{
	column_view::ColumnView,
	try_from::{FromValueError, TryFromValue},
};

#[derive(Debug, Clone, PartialEq)]
pub enum FrameError {
	ColumnNotFound {
		name: String,
	},

	RowOutOfBounds {
		row: usize,
		len: usize,
	},

	ValueError {
		column: String,
		row: usize,
		error: FromValueError,
	},

	InvalidColumn {
		name: String,
		message: String,
	},
}

impl Display for FrameError {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			FrameError::ColumnNotFound {
				name,
			} => {
				write!(f, "column not found: {}", name)
			}
			FrameError::RowOutOfBounds {
				row,
				len,
			} => {
				write!(f, "row {} out of bounds (frame has {} rows)", row, len)
			}
			FrameError::ValueError {
				column,
				row,
				error,
			} => {
				write!(f, "error extracting column '{}' row {}: {}", column, row, error)
			}
			FrameError::InvalidColumn {
				name,
				message,
			} => {
				write!(f, "column '{}' does not match its field: {}", name, message)
			}
		}
	}
}

impl error::Error for FrameError {}

impl Frame {
	pub fn column(&self, name: &str) -> Result<Option<ColumnView<'_>>, FrameError> {
		let schema = self.batch.schema_ref();
		let Some(index) = schema.fields().iter().position(|field| field.name() == name) else {
			return Ok(None);
		};
		ColumnView::try_from((self.batch.column(index), schema.field(index))).map(Some).map_err(|error| {
			FrameError::InvalidColumn {
				name: name.to_string(),
				message: error.to_string(),
			}
		})
	}

	pub fn try_column(&self, name: &str) -> Result<ColumnView<'_>, FrameError> {
		self.column(name)?.ok_or_else(|| FrameError::ColumnNotFound {
			name: name.to_string(),
		})
	}

	pub fn row_count(&self) -> usize {
		self.batch.num_rows()
	}

	pub fn get<T: TryFromValue>(&self, column: &str, row: usize) -> Result<Option<T>, FrameError> {
		let col = self.try_column(column)?;
		let len = col.len();

		if row >= len {
			return Err(FrameError::RowOutOfBounds {
				row,
				len,
			});
		}

		if !col.is_defined(row) {
			return Ok(None);
		}

		let value = col.get_value(row);
		T::try_from_value(&value).map(Some).map_err(|e| FrameError::ValueError {
			column: column.to_string(),
			row,
			error: e,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use std::sync::Arc;

	use arrow_array::{ArrayRef, Int32Array, Int64Array, LargeStringArray, RecordBatch, UInt64Array};
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
			column("score", ValueType::Int4, Arc::new(Int32Array::from(vec![100i32, 85, 92]))),
		]
		.into_iter()
		.unzip();
		let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
		let batch = with_system_column(
			batch,
			SystemColumn::RowNumbers,
			Arc::new(UInt64Array::from(vec![1u64, 2, 3])),
		)
		.unwrap();
		Frame::from(batch)
	}

	#[test]
	fn test_column_by_name() {
		let frame = make_test_frame();
		assert!(frame.column("id").unwrap().is_some());
		assert!(frame.column("name").unwrap().is_some());
		assert!(frame.column("nonexistent").unwrap().is_none());
	}

	#[test]
	fn test_row_count() {
		let frame = make_test_frame();
		assert_eq!(frame.row_count(), 3);

		let empty = Frame::from(RecordBatch::new_empty(Arc::new(Schema::empty())));
		assert_eq!(empty.row_count(), 0);
	}

	#[test]
	fn test_get_value() {
		let frame = make_test_frame();

		let id: Option<i64> = frame.get("id", 0).unwrap();
		assert_eq!(id, Some(1i64));

		let name: Option<String> = frame.get("name", 0).unwrap();
		assert_eq!(name, Some("Alice".to_string()));

		// The column has no bitvec, so an empty string must read as present, not as a none.
		let name_at_2: Option<String> = frame.get("name", 2).unwrap();
		assert_eq!(name_at_2, Some(String::new()));
	}

	#[test]
	fn test_errors() {
		let frame = make_test_frame();

		// Each failure keeps its own variant, so a caller can tell a typo from a bad index from
		// a type mismatch.
		let err = frame.get::<i64>("nonexistent", 0).unwrap_err();
		assert!(matches!(err, FrameError::ColumnNotFound { .. }));

		let err = frame.get::<i64>("id", 100).unwrap_err();
		assert!(matches!(err, FrameError::RowOutOfBounds { .. }));

		let err = frame.get::<i32>("id", 0).unwrap_err();
		assert!(matches!(err, FrameError::ValueError { .. }));
	}
}
