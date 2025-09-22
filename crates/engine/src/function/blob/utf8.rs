// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::column::ColumnData;
use reifydb_type::{OwnedFragment, value::Blob};

use crate::function::{ScalarFunction, ScalarFunctionContext};

pub struct BlobUtf8;

impl BlobUtf8 {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for BlobUtf8 {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::Result<ColumnData> {
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.is_empty() {
			return Ok(ColumnData::blob([]));
		}

		let column = columns.get(0).unwrap();

		match &column.data() {
			ColumnData::Utf8 {
				container,
				..
			} => {
				let mut result_data = Vec::with_capacity(container.data().len());

				for i in 0..row_count {
					if container.is_defined(i) {
						let utf8_str = &container[i];
						let blob = Blob::from_utf8(OwnedFragment::internal(utf8_str));
						result_data.push(blob);
					} else {
						result_data.push(Blob::empty())
					}
				}

				Ok(ColumnData::blob_with_bitvec(result_data, container.bitvec().clone()))
			}
			_ => unimplemented!("BlobUtf8 only supports text input"),
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::{
		column::{Column, ColumnComputed, Columns},
		container::Utf8Container,
	};
	use reifydb_type::{Fragment, value::constraint::bytes::MaxBytes};

	use super::*;

	#[test]
	fn test_blob_utf8_simple_ascii() {
		let function = BlobUtf8::new();

		let utf8_data = vec!["Hello!".to_string()];
		let bitvec = vec![true];
		let input_column = ColumnComputed {
			name: Fragment::borrowed_internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(utf8_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};
		let columns = Columns::new(vec![Column::Computed(input_column)]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 1,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Blob {
			container,
			..
		} = result
		else {
			panic!("Expected BLOB column data");
		};
		assert_eq!(container.len(), 1);
		assert!(container.is_defined(0));
		assert_eq!(container[0].as_bytes(), "Hello!".as_bytes());
	}

	#[test]
	fn test_blob_utf8_empty_string() {
		let function = BlobUtf8::new();

		let utf8_data = vec!["".to_string()];
		let bitvec = vec![true];
		let input_column = ColumnComputed {
			name: Fragment::borrowed_internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(utf8_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};
		let columns = Columns::new(vec![Column::Computed(input_column)]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 1,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Blob {
			container,
			..
		} = result
		else {
			panic!("Expected BLOB column data");
		};
		assert_eq!(container.len(), 1);
		assert!(container.is_defined(0));
		assert_eq!(container[0].as_bytes(), &[] as &[u8]);
	}

	#[test]
	fn test_blob_utf8_unicode_characters() {
		let function = BlobUtf8::new();

		// Test Unicode characters: emoji, accented chars, etc.
		let utf8_data = vec!["Hello 🌍! Café naïve".to_string()];
		let bitvec = vec![true];
		let input_column = ColumnComputed {
			name: Fragment::borrowed_internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(utf8_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};
		let columns = Columns::new(vec![Column::Computed(input_column)]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 1,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Blob {
			container,
			..
		} = result
		else {
			panic!("Expected BLOB column data");
		};
		assert_eq!(container.len(), 1);
		assert!(container.is_defined(0));
		assert_eq!(container[0].as_bytes(), "Hello 🌍! Café naïve".as_bytes());
	}

	#[test]
	fn test_blob_utf8_multibyte_characters() {
		let function = BlobUtf8::new();

		// Test various multibyte UTF-8 characters
		let utf8_data = vec!["日本語 中文 한국어 العربية".to_string()];
		let bitvec = vec![true];
		let input_column = ColumnComputed {
			name: Fragment::borrowed_internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(utf8_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};
		let columns = Columns::new(vec![Column::Computed(input_column)]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 1,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Blob {
			container,
			..
		} = result
		else {
			panic!("Expected BLOB column data");
		};
		assert_eq!(container.len(), 1);
		assert!(container.is_defined(0));
		assert_eq!(container[0].as_bytes(), "日本語 中文 한국어 العربية".as_bytes());
	}

	#[test]
	fn test_blob_utf8_special_characters() {
		let function = BlobUtf8::new();

		// Test special characters including newlines, tabs, etc.
		let utf8_data = vec!["Line1\nLine2\tTabbed\r\nWindows".to_string()];
		let bitvec = vec![true];
		let input_column = ColumnComputed {
			name: Fragment::borrowed_internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(utf8_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};
		let columns = Columns::new(vec![Column::Computed(input_column)]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 1,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Blob {
			container,
			..
		} = result
		else {
			panic!("Expected BLOB column data");
		};
		assert_eq!(container.len(), 1);
		assert!(container.is_defined(0));
		assert_eq!(container[0].as_bytes(), "Line1\nLine2\tTabbed\r\nWindows".as_bytes());
	}

	#[test]
	fn test_blob_utf8_multiple_rows() {
		let function = BlobUtf8::new();

		let utf8_data = vec!["First".to_string(), "Second 🚀".to_string(), "Third café".to_string()];
		let bitvec = vec![true, true, true];
		let input_column = ColumnComputed {
			name: Fragment::borrowed_internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(utf8_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};
		let columns = Columns::new(vec![Column::Computed(input_column)]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 3,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Blob {
			container,
			..
		} = result
		else {
			panic!("Expected BLOB column data");
		};
		assert_eq!(container.len(), 3);
		assert!(container.is_defined(0));
		assert!(container.is_defined(1));
		assert!(container.is_defined(2));

		assert_eq!(container[0].as_bytes(), "First".as_bytes());
		assert_eq!(container[1].as_bytes(), "Second 🚀".as_bytes());
		assert_eq!(container[2].as_bytes(), "Third café".as_bytes());
	}

	#[test]
	fn test_blob_utf8_with_null_data() {
		let function = BlobUtf8::new();

		let utf8_data = vec!["First".to_string(), "".to_string(), "Third".to_string()];
		let bitvec = vec![true, false, true];
		let input_column = ColumnComputed {
			name: Fragment::borrowed_internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(utf8_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};
		let columns = Columns::new(vec![Column::Computed(input_column)]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 3,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Blob {
			container,
			..
		} = result
		else {
			panic!("Expected BLOB column data");
		};
		assert_eq!(container.len(), 3);
		assert!(container.is_defined(0));
		assert!(!container.is_defined(1));
		assert!(container.is_defined(2));

		assert_eq!(container[0].as_bytes(), "First".as_bytes());
		assert_eq!(container[1].as_bytes(), [].as_slice() as &[u8]);
		assert_eq!(container[2].as_bytes(), "Third".as_bytes());
	}

	#[test]
	fn test_blob_utf8_json_data() {
		let function = BlobUtf8::new();

		// Test JSON-like data which is common to store as UTF-8
		let utf8_data = vec![r#"{"name": "John", "age": 30, "city": "New York"}"#.to_string()];
		let bitvec = vec![true];
		let input_column = ColumnComputed {
			name: Fragment::borrowed_internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(utf8_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};
		let columns = Columns::new(vec![Column::Computed(input_column)]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 1,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Blob {
			container,
			..
		} = result
		else {
			panic!("Expected BLOB column data");
		};
		assert_eq!(container.len(), 1);
		assert!(container.is_defined(0));
		assert_eq!(container[0].as_bytes(), r#"{"name": "John", "age": 30, "city": "New York"}"#.as_bytes());
	}

	#[test]
	fn test_blob_utf8_long_string() {
		let function = BlobUtf8::new();

		// Test a longer string to verify no issues with size
		let long_string = "A".repeat(1000);
		let utf8_data = vec![long_string.clone()];
		let bitvec = vec![true];
		let input_column = ColumnComputed {
			name: Fragment::borrowed_internal("input"),
			data: ColumnData::Utf8 {
				container: Utf8Container::new(utf8_data, bitvec.into()),
				max_bytes: MaxBytes::MAX,
			},
		};
		let columns = Columns::new(vec![Column::Computed(input_column)]);
		let ctx = ScalarFunctionContext {
			columns: &columns,
			row_count: 1,
		};

		let result = function.scalar(ctx).unwrap();

		let ColumnData::Blob {
			container,
			..
		} = result
		else {
			panic!("Expected BLOB column data");
		};
		assert_eq!(container.len(), 1);
		assert!(container.is_defined(0));
		assert_eq!(container[0].as_bytes(), long_string.as_bytes());
		assert_eq!(container[0].as_bytes().len(), 1000);
	}
}
