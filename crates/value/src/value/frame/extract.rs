// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	error,
	fmt::{self, Display, Formatter},
};

use super::{column::FrameColumn, frame::Frame};
use crate::value::try_from::{FromValueError, TryFromValue};

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
		}
	}
}

impl error::Error for FrameError {}

impl Frame {
	pub fn column(&self, name: &str) -> Option<&FrameColumn> {
		self.columns.iter().find(|c| c.name == name)
	}

	pub fn try_column(&self, name: &str) -> Result<&FrameColumn, FrameError> {
		self.column(name).ok_or_else(|| FrameError::ColumnNotFound {
			name: name.to_string(),
		})
	}

	pub fn row_count(&self) -> usize {
		self.columns.first().map(|c| c.data.len()).unwrap_or(0)
	}

	pub fn get<T: TryFromValue>(&self, column: &str, row: usize) -> Result<Option<T>, FrameError> {
		let col = self.try_column(column)?;
		let len = col.data.len();

		if row >= len {
			return Err(FrameError::RowOutOfBounds {
				row,
				len,
			});
		}

		if !col.data.is_defined(row) {
			return Ok(None);
		}

		let value = col.data.get_value(row);
		T::try_from_value(&value).map(Some).map_err(|e| FrameError::ValueError {
			column: column.to_string(),
			row,
			error: e,
		})
	}
}

#[cfg(test)]
pub mod tests {
	use arrow_array::{Int32Array, Int64Array, LargeStringArray};

	use super::*;
	use crate::value::frame::data::FrameColumnData;

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
				FrameColumn {
					name: "score".to_string(),
					data: FrameColumnData::Int4(Int32Array::from(vec![100i32, 85, 92])),
				},
			],
			vec![1.into(), 2.into(), 3.into()],
		)
	}

	#[test]
	fn test_column_by_name() {
		let frame = make_test_frame();
		assert!(frame.column("id").is_some());
		assert!(frame.column("name").is_some());
		assert!(frame.column("nonexistent").is_none());
	}

	#[test]
	fn test_row_count() {
		let frame = make_test_frame();
		assert_eq!(frame.row_count(), 3);

		let empty = Frame::new(vec![]);
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
