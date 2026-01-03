// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use std::fmt::{Display, Formatter};

use super::{Frame, FrameColumn};
use crate::{FromValueError, TryFromValue, TryFromValueCoerce};

/// Error type for Frame extraction operations
#[derive(Debug, Clone, PartialEq)]
pub enum FrameError {
	/// Column not found by name
	ColumnNotFound {
		name: String,
	},
	/// Row index out of bounds
	RowOutOfBounds {
		row: usize,
		len: usize,
	},
	/// Value extraction error
	ValueError {
		column: String,
		row: usize,
		error: FromValueError,
	},
}

impl Display for FrameError {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

impl std::error::Error for FrameError {}

impl Frame {
	/// Get a column by name
	pub fn column(&self, name: &str) -> Option<&FrameColumn> {
		self.columns.iter().find(|c| c.name == name)
	}

	/// Get a column by name, returning an error if not found
	pub fn try_column(&self, name: &str) -> Result<&FrameColumn, FrameError> {
		self.column(name).ok_or_else(|| FrameError::ColumnNotFound {
			name: name.to_string(),
		})
	}

	/// Get the number of rows in the frame
	pub fn row_count(&self) -> usize {
		self.columns.first().map(|c| c.data.len()).unwrap_or(0)
	}

	/// Extract a single value by column name and row index (strict type matching).
	///
	/// Returns `Ok(None)` for Undefined values.
	/// Returns `Err` for missing columns, out of bounds rows, or type mismatches.
	pub fn get<T: TryFromValue>(&self, column: &str, row: usize) -> Result<Option<T>, FrameError> {
		let col = self.try_column(column)?;
		let len = col.data.len();

		if row >= len {
			return Err(FrameError::RowOutOfBounds {
				row,
				len,
			});
		}

		// Check if value is undefined
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

	/// Extract a single value with widening coercion.
	///
	/// Returns `Ok(None)` for Undefined values.
	pub fn get_coerce<T: TryFromValueCoerce>(&self, column: &str, row: usize) -> Result<Option<T>, FrameError> {
		let col = self.try_column(column)?;
		let len = col.data.len();

		if row >= len {
			return Err(FrameError::RowOutOfBounds {
				row,
				len,
			});
		}

		// Check if value is undefined
		if !col.data.is_defined(row) {
			return Ok(None);
		}

		let value = col.data.get_value(row);
		T::try_from_value_coerce(&value).map(Some).map_err(|e| FrameError::ValueError {
			column: column.to_string(),
			row,
			error: e,
		})
	}

	/// Extract an entire column as `Vec<Option<T>>` (strict type matching).
	///
	/// Undefined values become `None`, type mismatches return an error.
	pub fn column_values<T: TryFromValue>(&self, name: &str) -> Result<Vec<Option<T>>, FrameError> {
		let col = self.try_column(name)?;
		(0..col.data.len())
			.map(|row| {
				if !col.data.is_defined(row) {
					Ok(None)
				} else {
					let value = col.data.get_value(row);
					T::try_from_value(&value).map(Some).map_err(|e| FrameError::ValueError {
						column: name.to_string(),
						row,
						error: e,
					})
				}
			})
			.collect()
	}

	/// Extract an entire column with widening coercion.
	///
	/// Undefined values become `None`, incompatible types return an error.
	pub fn column_values_coerce<T: TryFromValueCoerce>(&self, name: &str) -> Result<Vec<Option<T>>, FrameError> {
		let col = self.try_column(name)?;
		(0..col.data.len())
			.map(|row| {
				if !col.data.is_defined(row) {
					Ok(None)
				} else {
					let value = col.data.get_value(row);
					T::try_from_value_coerce(&value).map(Some).map_err(|e| FrameError::ValueError {
						column: name.to_string(),
						row,
						error: e,
					})
				}
			})
			.collect()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{
		BitVec,
		value::{
			container::{NumberContainer, Utf8Container},
			frame::FrameColumnData,
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
				FrameColumn {
					name: "score".to_string(),
					data: FrameColumnData::Int4(NumberContainer::from_vec(vec![100i32, 85, 92])),
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

		// Get strict-typed value
		let id: Option<i64> = frame.get("id", 0).unwrap();
		assert_eq!(id, Some(1i64));

		// Get string value
		let name: Option<String> = frame.get("name", 0).unwrap();
		assert_eq!(name, Some("Alice".to_string()));

		// Get undefined value
		let name_undefined: Option<String> = frame.get("name", 2).unwrap();
		assert_eq!(name_undefined, None);
	}

	#[test]
	fn test_get_coerce() {
		let frame = make_test_frame();

		// Int4 coerced to i64
		let score: Option<i64> = frame.get_coerce("score", 0).unwrap();
		assert_eq!(score, Some(100i64));

		// Int4 coerced to f64
		let score_f64: Option<f64> = frame.get_coerce("score", 1).unwrap();
		assert_eq!(score_f64, Some(85.0f64));
	}

	#[test]
	fn test_column_values() {
		let frame = make_test_frame();

		let ids: Vec<Option<i64>> = frame.column_values("id").unwrap();
		assert_eq!(ids, vec![Some(1), Some(2), Some(3)]);

		let names: Vec<Option<String>> = frame.column_values("name").unwrap();
		assert_eq!(names, vec![Some("Alice".to_string()), Some("Bob".to_string()), None]);
	}

	#[test]
	fn test_column_values_coerce() {
		let frame = make_test_frame();

		// Int4 coerced to Vec<Option<i64>>
		let scores: Vec<Option<i64>> = frame.column_values_coerce("score").unwrap();
		assert_eq!(scores, vec![Some(100), Some(85), Some(92)]);
	}

	#[test]
	fn test_errors() {
		let frame = make_test_frame();

		// Column not found
		let err = frame.get::<i64>("nonexistent", 0).unwrap_err();
		assert!(matches!(err, FrameError::ColumnNotFound { .. }));

		// Row out of bounds
		let err = frame.get::<i64>("id", 100).unwrap_err();
		assert!(matches!(err, FrameError::RowOutOfBounds { .. }));

		// Type mismatch (strict)
		let err = frame.get::<i32>("id", 0).unwrap_err();
		assert!(matches!(err, FrameError::ValueError { .. }));
	}
}
